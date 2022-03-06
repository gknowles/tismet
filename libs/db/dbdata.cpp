// Copyright Glen Knowles 2017 - 2022.
// Distributed under the Boost Software License, Version 1.0.
//
// dbdata.cpp - tismet db
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace Dim;


/****************************************************************************
*
*   Tuning parameters
*
***/

constexpr auto kZeroPageNum = (pgno_t) 0;
constexpr auto kFreeStoreRootPageNum = (pgno_t) 1;
constexpr auto kDeprecatedStoreRootPageNum = (pgno_t) 2;
constexpr auto kMetricStoreRootPageNum = (pgno_t) 3;


/****************************************************************************
*
*   Private
*
***/

const unsigned kDataFileSig[] = {
    0x39515728,
    0x4873456d,
    0xf6bfd8a1,
    0xa33f3ba2,
};

struct DbData::ZeroPage {
    static const auto kPageType = DbPageType::kZero;
    DbPageHeader hdr;
    char signature[sizeof(kDataFileSig)];
    unsigned pageSize;
    pgno_t freeStoreRoot;
    pgno_t deprecatedStoreRoot;
    pgno_t metricStoreRoot;
    pgno_t metricTagStoreRoot;
};
static_assert(is_standard_layout_v<DbData::ZeroPage>);
static_assert(2 * sizeof(DbData::ZeroPage) <= kMinPageSize);

struct DbData::FreePage {
    static const auto kPageType = DbPageType::kFree;
    DbPageHeader hdr;
};


/****************************************************************************
*
*   Variables
*
***/

static auto & s_perfPages = uperf("db.data pages (total)");
static auto & s_perfFreePages = uperf("db.data pages (free)");


/****************************************************************************
*
*   DbData
*
***/

//===========================================================================
[[maybe_unused]]
static size_t queryPageSize(FileHandle f) {
    if (!f)
        return 0;
    DbData::ZeroPage zp;
    uint64_t bytes;
    if (fileReadWait(&bytes, &zp, sizeof(zp), f, 0); bytes != sizeof(zp))
        return 0;
    if (zp.hdr.type != zp.kPageType)
        return 0;
    if (memcmp(zp.signature, kDataFileSig, sizeof(zp.signature)) != 0)
        return 0;
    return zp.pageSize;
}

//===========================================================================
DbData::~DbData () {
    metricClearCounters();
    s_perfPages -= (unsigned) m_numPages;
    s_perfFreePages -= (unsigned) m_numFreed;
}

//===========================================================================
void DbData::openForApply(size_t pageSize, EnumFlags<DbOpenFlags> flags) {
    m_verbose = flags.any(fDbOpenVerbose);
    m_pageSize = pageSize;
}

//===========================================================================
bool DbData::openForUpdate(
    DbTxn & txn,
    IDbDataNotify * notify,
    string_view name,
    EnumFlags<DbOpenFlags> flags
) {
    assert(m_pageSize);
    m_verbose = flags.any(fDbOpenVerbose);

    auto zp = (const ZeroPage *) txn.viewPage<DbPageHeader>(kZeroPageNum);
    if (zp->hdr.type == DbPageType::kInvalid) {
        txn.logZeroInit(kZeroPageNum);
        zp = (const ZeroPage *) txn.viewPage<DbPageHeader>(kZeroPageNum);
    }

    if (memcmp(zp->signature, kDataFileSig, sizeof(zp->signature)) != 0) {
        logMsgError() << "Bad signature, " << name;
        return false;
    }
    if (zp->pageSize != m_pageSize) {
        logMsgError() << "Mismatched page size, " << name;
        return false;
    }
    m_numPages = txn.numPages();
    s_perfPages += (unsigned) m_numPages;
    m_freeStoreRoot = zp->freeStoreRoot;
    m_deprecatedStoreRoot = zp->deprecatedStoreRoot;
    m_metricStoreRoot = zp->metricStoreRoot;
    m_metricTagStoreRoot = zp->metricTagStoreRoot;

    if (m_numPages == 1) {
        m_freeStoreRoot = allocPgno(txn);
        assert(m_freeStoreRoot == kFreeStoreRootPageNum);
        txn.logRadixInit(m_freeStoreRoot, 0, 0, nullptr, nullptr);
        m_deprecatedStoreRoot = allocPgno(txn);
        assert(m_deprecatedStoreRoot == kDeprecatedStoreRootPageNum);
        txn.logRadixInit(m_deprecatedStoreRoot, 0, 0, nullptr, nullptr);
        m_metricStoreRoot = allocPgno(txn);
        assert(m_metricStoreRoot == kMetricStoreRootPageNum);
        txn.logRadixInit(m_metricStoreRoot, 0, 0, nullptr, nullptr);
    }

    if (m_verbose)
        logMsgInfo() << "Load free page list";
    if (!loadFreePages(txn))
        return false;
    if (!loadDeprecatedPages(txn))
        return false;
    if (m_verbose)
        logMsgInfo() << "Build metric index";
    if (!loadMetrics(txn, notify))
        return false;

    return true;
}

//===========================================================================
DbStats DbData::queryStats() {
    DbStats s;
    s.pageSize = (unsigned) m_pageSize;
    s.bitsPerPage = (unsigned) bitsPerPage();
    s.metricNameSize = (unsigned) metricNameSize(m_pageSize);
    s.samplesPerPage[kSampleTypeInvalid] = 0;
    for (int8_t i = 1; i < kSampleTypes; ++i)
        s.samplesPerPage[i] = (unsigned) samplesPerPage(DbSampleType{i});

    {
        shared_lock lk{m_mposMut};
        s.metrics = m_numMetrics;
    }

    scoped_lock lk{m_pageMut};
    s.numPages = (unsigned) m_numPages;
    s.freePages = (unsigned) m_freePages.count(0, m_numPages);
    return s;
}


/****************************************************************************
*
*   Free store
*
***/

//===========================================================================
bool DbData::loadFreePages(DbTxn & txn) {
    assert(!m_freePages);
    if (!bitLoad(txn, &m_freePages, m_freeStoreRoot)) 
        return false;
    if (appStopping())
        return false;
    auto num = (unsigned) m_freePages.count();
    m_numFreed += num;
    s_perfFreePages += num;

    // validate that pages in free list are in fact free
    pgno_t blank = {};
    for (auto && p : m_freePages) {
        auto pgno = (pgno_t) p;
        if (pgno >= m_numPages)
            break;
        auto fp = txn.viewPage<DbPageHeader>(pgno);
        if (!fp
            || fp->type != DbPageType::kInvalid
                && fp->type != DbPageType::kFree
        ) {
            logMsgError() << "Bad free page #" << pgno << ", type " 
                << (unsigned) fp->type;
            return false;
        }
        if (fp->type != DbPageType::kInvalid) {
            if (blank) {
                logMsgError() << "Blank data page #" << pgno << ", type "
                    << (unsigned) fp->type;
                return false;
            }
        } else if (!blank) {
            blank = pgno;
        }
        if (appStopping())
            return false;
    }
    if (blank && blank < m_numPages) {
        auto trimmed = (unsigned) (m_numPages - blank);
        logMsgInfo() << "Trimmed " << trimmed << " blank pages";
        m_numPages = blank;
        s_perfPages -= trimmed;
        m_numFreed -= trimmed;
        s_perfFreePages -= trimmed;
    }

    return true;
}

//===========================================================================
bool DbData::loadDeprecatedPages(DbTxn & txn) {
    assert(!m_deprecatedPages);
    if (!bitLoad(txn, &m_deprecatedPages, m_deprecatedStoreRoot))
        return false;
    if (appStopping())
        return false;
    while (m_deprecatedPages) {
        auto pgno = (pgno_t) m_deprecatedPages.pop_front();
        freePage(txn, pgno);
    }
    return true;
}

//===========================================================================
pgno_t DbData::allocPgno (DbTxn & txn) {
    scoped_lock lk{m_pageMut};

    auto pgno = pgno_t{};
    bool freed = false;
    if (m_freePages) {
        freed = true;
        pgno = (pgno_t) m_freePages.pop_front();
        // Free pages are reserved in blocks that may extend beyond the end
        // of file. Therefore, even through a pgno is from the free list it 
        // may still be equal to m_numPages.
    } else {
        assert(!m_numFreed);
        pgno = (pgno_t) m_numPages;
    }
    if (pgno >= m_numPages) {
        assert(pgno == m_numPages);
        m_numPages += 1;
        s_perfPages += 1;
        txn.growToFit(pgno);
    }
    if (freed) {
        m_numFreed -= 1;
        s_perfFreePages -= 1;
        [[maybe_unused]] bool updated = 
            bitUpsert(txn, m_freeStoreRoot, 0, pgno, pgno + 1, false);
        assert(updated);
    }

#ifndef NDEBUG
    auto fp = txn.viewPage<DbPageHeader>(pgno);
    assert(fp->type == DbPageType::kInvalid || fp->type == DbPageType::kFree);
#endif
    return pgno;
}

//===========================================================================
void DbData::freePage(DbTxn & txn, pgno_t pgno) {
    scoped_lock lk{m_pageMut};

    assert(pgno < m_numPages);
    auto p = txn.viewPage<DbPageHeader>(pgno);
    auto type = p->type;
    switch (type) {
    case DbPageType::kMetric:
        metricDestructPage(txn, pgno);
        break;
    case DbPageType::kRadix:
        radixDestructPage(txn, pgno);
        break;
    case DbPageType::kBitmap:
    case DbPageType::kSample:
        break;
    case DbPageType::kFree:
        logMsgFatal() << "freePage(" << (unsigned) pgno 
            << "): page already free";
    default:
        logMsgFatal() << "freePage(" << (unsigned) pgno
            << "): invalid page type (" << (unsigned) type << ")";
    }

    txn.logPageFree(pgno);
    assert(m_freeStoreRoot);
    [[maybe_unused]] bool updated = 
        bitUpsert(txn, m_freeStoreRoot, 0, pgno, pgno + 1, true);
    assert(updated);
    auto bpp = bitsPerPage();
    bool noPages = !m_freePages;
    m_freePages.insert(pgno);
    m_numFreed += 1;
    s_perfFreePages += 1;
    if (noPages && pgno / bpp == m_numPages / bpp) {
        auto num = bpp - m_numPages % bpp;
        if (num) {
            bitUpsert(
                txn, 
                m_freeStoreRoot, 
                0, 
                m_numPages, 
                m_numPages + num, 
                true
            );
            m_freePages.insert((uint32_t) m_numPages, num);
            m_numFreed += num;
            s_perfFreePages += (unsigned) num;
        }
    }
}


/****************************************************************************
*
*   DbLogRecInfo
*
***/

#pragma pack(push)
#pragma pack(1)

namespace {

struct FullPageInitRec {
    DbLog::Record hdr;
    DbPageType type;
    uint32_t id;
    uint16_t dataLen;

    // EXTENDS BEYOND END OF STRUCT
    uint8_t data[1];
};

} // namespace

#pragma pack(pop)


static DbLogRecInfo::Table s_dataRecInfo = {
    { kRecTypeZeroInit,
        DbLogRecInfo::sizeFn<DbLog::Record>,
        [](auto args) {
            args.notify->onLogApplyZeroInit(args.page);
        },
    },
    { kRecTypePageFree,
        DbLogRecInfo::sizeFn<DbLog::Record>,
        [](auto args) {
            args.notify->onLogApplyPageFree(args.page);
        },
    },
    { kRecTypeFullPage,
        [](auto & log) -> uint16_t {    // size
            auto & rec = reinterpret_cast<const FullPageInitRec &>(log);
            return offsetof(FullPageInitRec, data) + rec.dataLen;
        },
        [](auto args) {
            auto rec = reinterpret_cast<const FullPageInitRec *>(args.log);
            args.notify->onLogApplyFullPageInit(
                args.page,
                rec->type,
                rec->id,
                {rec->data, rec->dataLen}
            );
        },
    },
};


/****************************************************************************
*
*   DbTxn
*
***/

//===========================================================================
void DbTxn::logZeroInit(pgno_t pgno) {
    auto [rec, bytes] = alloc<DbLog::Record>(kRecTypeZeroInit, pgno);
    log(rec, bytes);
}

//===========================================================================
void DbTxn::logPageFree(pgno_t pgno) {
    auto [rec, bytes] = alloc<DbLog::Record>(kRecTypePageFree, pgno);
    log(rec, bytes);
}

//===========================================================================
void DbTxn::logFullPageInit(
    pgno_t pgno, 
    DbPageType type, 
    uint32_t id, 
    std::span<uint8_t> data
) {
    auto extra = data.size();
    auto offset = offsetof(FullPageInitRec, data);
    assert(offset + extra <= pageSize());
    auto [rec, bytes] = alloc<FullPageInitRec>(
        kRecTypeFullPage,
        pgno,
        offset + extra
    );
    rec->type = type;
    rec->id = id;
    rec->dataLen = (uint16_t) extra;
    memcpy(rec->data, data.data(), extra);
    log(&rec->hdr, bytes);
}


/****************************************************************************
*
*   Log apply
*
***/

//===========================================================================
void DbData::onLogApplyCommitCheckpoint(uint64_t lsn, uint64_t startLsn)
{}

//===========================================================================
void DbData::onLogApplyBeginTxn(uint64_t lsn, uint16_t localTxn)
{}

//===========================================================================
void DbData::onLogApplyCommitTxn(uint64_t lsn, uint16_t localTxn)
{}

//===========================================================================
void DbData::onLogApplyZeroInit(void * ptr) {
    auto zp = static_cast<ZeroPage *>(ptr);
    assert(zp->hdr.type == DbPageType::kInvalid);
    // We only init the zero page when making a new database, so we can forgo
    // the normal logic to memset when init'd from free pages.
    zp->hdr.type = zp->kPageType;
    zp->hdr.id = 0;
    assert(zp->hdr.pgno == kZeroPageNum);
    memcpy(zp->signature, kDataFileSig, sizeof(zp->signature));
    zp->pageSize = (unsigned) m_pageSize;
    zp->freeStoreRoot = kFreeStoreRootPageNum;
    zp->deprecatedStoreRoot = kDeprecatedStoreRootPageNum;
    zp->metricStoreRoot = kMetricStoreRootPageNum;
    zp->metricTagStoreRoot = {};
}

//===========================================================================
void DbData::onLogApplyPageFree(void * ptr) {
    auto fp = static_cast<FreePage *>(ptr);
    assert(fp->hdr.type != DbPageType::kInvalid
        && fp->hdr.type != DbPageType::kFree);
    fp->hdr.type = DbPageType::kFree;
}

//===========================================================================
void DbData::onLogApplyFullPageInit(
    void * ptr,
    DbPageType type,
    uint32_t id,
    std::span<const uint8_t> data
) {
    auto hdr = static_cast<DbPageHeader *>(ptr);
    auto offset = sizeof *hdr + data.size();
    assert(offset <= m_pageSize);
    if (hdr->type == DbPageType::kFree) {
        memset((char *) hdr + offset, 0, m_pageSize - offset);
    } else {
        assert(hdr->type == DbPageType::kInvalid);
    }
    hdr->type = type;
    hdr->id = id;
    memcpy(hdr + 1, data.data(), data.size());
}
