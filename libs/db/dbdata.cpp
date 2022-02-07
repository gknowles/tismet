// Copyright Glen Knowles 2017 - 2021.
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

auto const kZeroPageNum = (pgno_t) 0;
auto const kMetricIndexPageNum = (pgno_t) 1;


/****************************************************************************
*
*   Private
*
***/

unsigned const kDataFileSig[] = {
    0x39515728,
    0x4873456d,
    0xf6bfd8a1,
    0xa33f3ba2,
};

struct DbData::SegmentPage {
    static const auto kPageType = DbPageType::kSegment;
    DbPageHeader hdr;
};

struct DbData::ZeroPage {
    static const auto kPageType = DbPageType::kZero;
    union {
        DbPageHeader hdr;
        SegmentPage segment;
    };
    char signature[sizeof(kDataFileSig)];
    unsigned pageSize;
    unsigned segmentSize;
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
*   Helpers
*
***/

//===========================================================================
constexpr uint32_t pagesPerSegment(size_t pageSize) {
    static_assert(CHAR_BIT == 8);
    return uint32_t(CHAR_BIT * pageSize / 2);
}

//===========================================================================
constexpr size_t segmentSize(size_t pageSize) {
    static_assert(CHAR_BIT == 8);
    return pageSize * pagesPerSegment(pageSize);
}

//===========================================================================
constexpr pair<pgno_t, size_t> segmentPage(pgno_t pgno, size_t pageSize) {
    auto pps = pagesPerSegment(pageSize);
    auto segPage = pgno / pps * pps;
    auto segPos = pgno % pps;
    return {(pgno_t) segPage, segPos};
}


/****************************************************************************
*
*   DbData
*
***/

//===========================================================================
inline static size_t queryPageSize(FileHandle f) {
    if (!f)
        return 0;
    DbData::ZeroPage zp;
    if (auto bytes = fileReadWait(&zp, sizeof(zp), f, 0); bytes != sizeof(zp))
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
    if (zp->segmentSize != segmentSize(m_pageSize)) {
        logMsgError() << "Mismatched segment size, " << name;
        return false;
    }
    m_numPages = txn.numPages();
    s_perfPages += (unsigned) m_numPages;
    m_segmentSize = zp->segmentSize;

    if (m_verbose)
        logMsgInfo() << "Load free page list";
    if (!loadFreePages(txn))
        return false;
    if (m_numPages == 1) {
        auto pgno = allocPgno(txn);
        assert(pgno == kMetricIndexPageNum);
        txn.logRadixInit(pgno, 0, 0, nullptr, nullptr);
    }
    if (m_verbose)
        logMsgInfo() << "Build metric index";
    if (!loadMetrics(txn, notify, kMetricIndexPageNum))
        return false;

    return true;
}

//===========================================================================
DbStats DbData::queryStats() {
    DbStats s;
    s.pageSize = (unsigned) m_pageSize;
    s.segmentSize = (unsigned) m_segmentSize;
    s.metricNameSize = (unsigned) metricNameSize();
    s.samplesPerPage[kSampleTypeInvalid] = 0;
    for (int8_t i = 1; i < kSampleTypes; ++i)
        s.samplesPerPage[i] = (unsigned) samplesPerPage(DbSampleType{i});

    {
        shared_lock lk{m_mposMut};
        s.metrics = m_numMetrics;
    }

    scoped_lock lk{m_pageMut};
    s.numPages = (unsigned) m_numPages;
    s.freePages = (unsigned) m_numFreed;
    return s;
}

//===========================================================================
void DbData::onLogApplyCommitCheckpoint(uint64_t lsn, uint64_t startLsn)
{}

//===========================================================================
void DbData::onLogApplyBeginTxn(uint64_t lsn, uint16_t localTxn)
{}

//===========================================================================
void DbData::onLogApplyCommitTxn(uint64_t lsn, uint16_t localTxn)
{}


/****************************************************************************
*
*   Segments
*
***/

//===========================================================================
static BitView segmentBitView(void * hdr, size_t pageSize) {
    auto base = (uint64_t *) ((char *) hdr + pageSize / 2);
    auto words = pageSize / 2 / sizeof(*base);
    return {base, words};
}

//===========================================================================
bool DbData::loadFreePages (DbTxn & txn) {
    auto pps = pagesPerSegment(m_pageSize);
    assert(!m_freePages);
    for (pgno_t pgno = {}; pgno < m_numPages; pgno = pgno_t(pgno + pps)) {
        auto pp = segmentPage(pgno, m_pageSize);
        auto segPage = pp.first;
        assert(!pp.second);
        auto sp = txn.viewPage<DbPageHeader>(segPage);
        assert(sp->type == DbPageType::kSegment
            || sp->type == DbPageType::kZero);
        auto bits = segmentBitView(const_cast<DbPageHeader *>(sp), m_pageSize);
        for (auto first = bits.find(0); first != bits.npos; ) {
            auto last = bits.findZero(first);
            if (last == bits.npos)
                last = pps;
            m_freePages.insert(
                pgno + (unsigned) first,
                pgno + (unsigned) last - 1
            );
            if (first < m_numPages) {
                auto num = (unsigned) (min(last, m_numPages - pgno) - first);
                m_numFreed += num;
                s_perfFreePages += num;
            }
            first = bits.find(last);
        }
        if (appStopping())
            return false;
    }

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
            logMsgError() << "Bad free page #" << pgno;
            return false;
        }
        if (fp->type != DbPageType::kInvalid) {
            if (blank) {
                logMsgError() << "Blank data page #" << pgno;
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
pgno_t DbData::allocPgno (DbTxn & txn) {
    scoped_lock lk{m_pageMut};

    if (!m_freePages) {
        assert(!m_numFreed);
        auto [segPage, segPos] = segmentPage((pgno_t) m_numPages, m_pageSize);
        assert(segPage == m_numPages && !segPos);
        (void) segPos;
        m_numPages += 1;
        s_perfPages += 1;
        txn.growToFit(segPage);
        auto pps = pagesPerSegment(m_pageSize);
        m_freePages.insert(segPage + 1, segPage + pps - 1);
    }
    auto pgno = (pgno_t) m_freePages.pop_front();
    if (pgno < m_numPages) {
        m_numFreed -= 1;
        s_perfFreePages -= 1;
    }

    auto segPage = segmentPage(pgno, m_pageSize).first;
    txn.logSegmentUpdate(segPage, pgno, false);
    if (pgno >= m_numPages) {
        assert(pgno == m_numPages);
        m_numPages += 1;
        s_perfPages += 1;
        txn.growToFit(pgno);
    }

    auto fp [[maybe_unused]] = txn.viewPage<DbPageHeader>(pgno);
    assert(fp->type == DbPageType::kInvalid || fp->type == DbPageType::kFree);
    return pgno;
}

//===========================================================================
void DbData::freePage(DbTxn & txn, pgno_t pgno) {
    scoped_lock lk{m_pageMut};

    assert(pgno < m_numPages);
    auto p = txn.viewPage<DbPageHeader>(pgno);
    assert(p->type != DbPageType::kFree);
    FreePage fp;
    fp.hdr = *p;
    switch (fp.hdr.type) {
    case DbPageType::kMetric:
        metricDestructPage(txn, pgno);
        break;
    case DbPageType::kRadix:
        radixDestructPage(txn, pgno);
        break;
    case DbPageType::kSample:
        break;
    case DbPageType::kFree:
        logMsgFatal() << "freePage: page already free";
    default:
        logMsgFatal() << "freePage(" << (unsigned) fp.hdr.type
            << "): invalid state";
    }

    txn.logPageFree(pgno);
    m_freePages.insert(pgno);
    m_numFreed += 1;
    s_perfFreePages += 1;

    auto segPage = segmentPage(pgno, m_pageSize).first;
    txn.logSegmentUpdate(segPage, pgno, true);
}

//===========================================================================
void DbData::onLogApplyPageFree(void * ptr) {
    auto fp = static_cast<FreePage *>(ptr);
    assert(fp->hdr.type != DbPageType::kInvalid
        && fp->hdr.type != DbPageType::kFree);
    fp->hdr.type = DbPageType::kFree;
}

//===========================================================================
void DbData::onLogApplyZeroInit(void * ptr) {
    auto zp = static_cast<ZeroPage *>(ptr);
    assert(zp->hdr.type == DbPageType::kInvalid);
    zp->hdr.type = zp->kPageType;
    zp->hdr.id = 0;
    assert(zp->hdr.pgno == kZeroPageNum);
    auto segSize = segmentSize(m_pageSize);
    assert(segSize <= numeric_limits<decltype(zp->segmentSize)>::max());
    zp->segmentSize = (unsigned) segSize;
    memcpy(zp->signature, kDataFileSig, sizeof(zp->signature));
    zp->pageSize = (unsigned) m_pageSize;
    auto bits = segmentBitView(zp, m_pageSize);
    bits.set();
    bits.reset(0);
}

//===========================================================================
void DbData::onLogApplySegmentUpdate(
    void * ptr,
    pgno_t refPage,
    bool free
) {
    auto sp = static_cast<SegmentPage *>(ptr);
    auto bits = segmentBitView(sp, m_pageSize);
    if (sp->hdr.type == DbPageType::kInvalid) {
        sp->hdr.type = sp->kPageType;
        sp->hdr.id = 0;
        if constexpr (DIMAPP_LIB_BUILD_DEBUG) {
            auto [segPage, segPos] = segmentPage(sp->hdr.pgno, m_pageSize);
            assert(segPage == sp->hdr.pgno && !segPos);
        }
        bits.set();
        bits.reset(0);
    }
    assert(sp->hdr.type == DbPageType::kZero
        || sp->hdr.type == DbPageType::kSegment);
    auto [segPage, segPos] = segmentPage(refPage, m_pageSize);
    assert(sp->hdr.pgno == segPage);
    ignore = segPage;
    assert(bits[segPos] != free);
    bits.set(segPos, free);
}
