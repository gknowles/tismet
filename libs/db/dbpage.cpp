// Copyright Glen Knowles 2017 - 2018.
// Distributed under the Boost Software License, Version 1.0.
//
// dbpage.cpp - tismet db
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace Dim;


/****************************************************************************
*
*   Tuning parameters
*
***/

// Must be a multiple of fileViewAlignment()
const size_t kViewSize = 0x100'0000; // 16MiB
const size_t kDefaultFirstViewSize = 2 * kViewSize;

const uint32_t kFreePageMark = numeric_limits<uint32_t>::max();


/****************************************************************************
*
*   Declarations
*
***/

namespace {

const unsigned kWorkFileSig[] = {
    0xa6e6fd51,
    0x4a443864,
    0x8b4302ae,
    0x84b2074b,
};

const uint32_t kPageTypeZero = 'wZ';

struct ZeroPage {
    DbPageHeader hdr;
    char signature[sizeof(kWorkFileSig)];
    unsigned pageSize;
};

} // namespace


/****************************************************************************
*
*   Variables
*
***/

static auto & s_perfPages = uperf("db.work pages (total)");
static auto & s_perfFreePages = uperf("db.work pages (free)");
static auto & s_perfDirtyPages = uperf("db.work pages (dirty)");
static auto & s_perfWrites = uperf("db.work writes (total)");
static auto & s_perfStableBytes = uperf("db.work stable bytes");


/****************************************************************************
*
*   DbPage
*
***/

//===========================================================================
DbPage::DbPage()
    : m_maxDirtyAge{kDefaultMaxCheckpointInterval}
    , m_maxDirtyData{kDefaultMaxCheckpointData}
    , m_saveTimer{[&](TimePoint now) { return onSaveTimer(now); }}
{}

//===========================================================================
DbPage::~DbPage() {
    close();
}

//===========================================================================
bool DbPage::open(
    string_view datafile,
    string_view workfile,
    size_t pageSize,
    DbOpenFlags flags
) {
    assert(pageSize);
    assert(pageSize == pow2Ceil(pageSize));
    assert(pageSize >= kMinPageSize);

    m_pageSize = pageSize;
    m_flags = flags;
    if (m_flags & fDbOpenVerbose)
        logMsgInfo() << "Open data files";
    if (!openData(datafile))
        return false;
    if (!openWork(workfile)) {
        close();
        return false;
    }
    m_currentWal.push_back({0, Clock::now()});

    return true;
}

//===========================================================================
bool DbPage::openData(string_view datafile) {
    auto oflags = File::fReadWrite | File::fDenyWrite | File::fRandom;
    if (m_flags & fDbOpenCreat)
        oflags |= File::fCreat;
    if (m_flags & fDbOpenTrunc)
        oflags |= File::fTrunc;
    if (m_flags & fDbOpenExcl)
        oflags |= File::fExcl;
    m_fdata = fileOpen(datafile, oflags);
    if (!m_fdata)
        return false;
    auto len = fileSize(m_fdata);
    if (!len) {
        DbPageHeader hdr = {};
        fileWriteWait(m_fdata, 0, &hdr, sizeof(hdr));
        m_newFiles = true;
    }
    if (!m_vdata.open(m_fdata, kViewSize, m_pageSize)) {
        logMsgError() << "Open view failed, " << datafile;
        return false;
    }

    // Remove trailing blank pages from page count
    auto lastPage = (uint32_t) (len / m_pageSize);
    while (lastPage) {
        lastPage -= 1;
        auto p = static_cast<const DbPageHeader *>(m_vdata.rptr(lastPage));
        if (p->type)
            break;
    }
    m_pages.resize(lastPage + 1);

    return true;
}

//===========================================================================
bool DbPage::openWork(string_view workfile) {
    auto oflags = File::fTemp | File::fReadWrite | File::fDenyWrite
        | File::fBlocking | File::fRandom;
    // Opening the data file has already succeeded, so always create the
    // work file (if not exist).
    oflags |= File::fCreat;
    if (m_flags & fDbOpenExcl)
        oflags |= File::fExcl;
    m_fwork = fileOpen(workfile, oflags);
    if (!m_fwork)
        return false;
    auto len = fileSize(m_fwork);
    ZeroPage zp{};
    if (!len) {
        zp.hdr.type = (DbPageType) kPageTypeZero;
        memcpy(zp.signature, kWorkFileSig, sizeof(zp.signature));
        zp.pageSize = (unsigned) m_pageSize;
        fileWriteWait(m_fwork, 0, &zp, sizeof(zp));
        len = m_pageSize;
    } else {
        fileReadWait(&zp, sizeof(zp), m_fwork, 0);
    }
    if (zp.pageSize != m_pageSize) {
        logMsgError() << "Mismatched page size, " << workfile;
        return false;
    }
    if (memcmp(zp.signature, kWorkFileSig, sizeof(zp.signature)) != 0) {
        logMsgError() << "Bad signature, " << workfile;
        return false;
    }
    if (m_pageSize < kMinPageSize || kViewSize % m_pageSize != 0) {
        logMsgError() << "Invalid page size, " << workfile;
        return false;
    }
    m_workPages = len / m_pageSize;
    s_perfPages += (unsigned) m_workPages;
    m_freeWorkPages.insert(1, (unsigned) m_workPages - 1);
    s_perfFreePages += (unsigned) m_workPages - 1;
    if (!m_vwork.open(m_fwork, kViewSize, m_pageSize)) {
        logMsgError() << "Open view failed, " << workfile;
        return false;
    }

    return true;
}

//===========================================================================
DbConfig DbPage::configure(const DbConfig & conf) {
    // checkpoint configuration is assumed to have already been validated
    // by DbLog.
    assert(conf.checkpointMaxInterval.count());
    assert(conf.checkpointMaxData);

    unique_lock lk{m_workMut};
    m_maxDirtyAge = conf.checkpointMaxInterval;
    m_maxDirtyData = conf.checkpointMaxData;
    queueSaveWork_LK();

    return conf;
}

//===========================================================================
void DbPage::close() {
    m_pages.clear();
    m_dirtyPages.clear();
    m_oldPages.clear();
    m_currentWal.clear();

    m_vdata.close();
    fileClose(m_fdata);
    m_vwork.close();

    // TODO: resize to number of dirty pages at start of last checkpoint
    fileResize(m_fwork, m_pageSize);

    fileClose(m_fwork);
    s_perfPages -= (unsigned) m_workPages;
    s_perfFreePages -= (unsigned) m_freeWorkPages.size();
    m_freeWorkPages.clear();
    m_pageSize = 0;
    m_workPages = 0;
}

//===========================================================================
void DbPage::growToFit(uint32_t pgno) {
    unique_lock lk{m_workMut};
    if (pgno < m_pages.size())
        return;
    assert(pgno == m_pages.size());
    m_vdata.growToFit(pgno);
    m_pages.resize(pgno + 1);
}

//===========================================================================
void DbPage::onLogStable(uint64_t lsn, size_t bytes) {
    unique_lock lk{m_workMut};
    m_stableLsn = lsn;
    s_perfStableBytes += (unsigned) bytes;
    m_stableBytes += bytes;
    m_currentWal.push_back({lsn, Clock::now(), bytes});
    while (m_stableBytes - m_overflowBytes > m_maxDirtyData) {
        auto wi = m_currentWal.front();
        m_overflowWal.push_back(wi);
        m_currentWal.pop_front();
        m_overflowBytes += wi.bytes;
    }

    queueSaveWork_LK();
}

//===========================================================================
uint64_t DbPage::onLogCheckpointPages(uint64_t lsn) {
    {
        scoped_lock lk{m_workMut};
        if (!m_overflowWal.empty()) {
            if (auto first = m_overflowWal.front().lsn; first > lsn)
                lsn = first;
        } else if (!m_currentWal.empty()) {
            if (auto first = m_currentWal.front().lsn; first > lsn)
                lsn = first;
        }
    }

    if (!fileFlush(m_fdata))
        logMsgFatal() << "Checkpointing failed.";
    return lsn;
}

//===========================================================================
Duration DbPage::untilNextSave_LK() {
    if (!m_oldPages.empty() && m_stableLsn >= m_oldPages.front().lsn)
        return 0ms;
    if (!m_dirtyPages.empty()) {
        auto & front = m_dirtyPages.front();
        if (m_overflowBytes && m_stableLsn >= front.lsn)
            return 0ms;
        auto minTime = Clock::now() - m_maxDirtyAge;
        auto maxWait = front.time - minTime;
        auto wait = (m_dirtyPages.back().time - minTime) / m_dirtyPages.size();
        if (wait > maxWait)
            wait = maxWait;
        if (wait <= 0ms)
            return 0ms;
        return wait;
    }
    return kTimerInfinite;
}

//===========================================================================
void DbPage::queueSaveWork_LK() {
    auto wait = untilNextSave_LK();
    timerUpdate(&m_saveTimer, wait, true);
}

//===========================================================================
// Remove wal info entries that have had all their pages committed
void DbPage::removeWalPages_LK(uint64_t lsn) {
    assert(lsn);
    size_t removed = 0;

    while (m_overflowWal.size() > 1 && lsn >= m_overflowWal[1].lsn
        || m_overflowWal.size() == 1 && lsn >= m_currentWal.front().lsn
    ) {
        auto bytes = m_overflowWal.front().bytes;
        removed += bytes;
        m_overflowWal.pop_front();
    }
    m_overflowBytes -= removed;

    if (m_overflowWal.empty()) {
        while (m_currentWal.size() > 1 && lsn >= m_currentWal[1].lsn) {
            auto bytes = m_currentWal.front().bytes;
            removed += bytes;
            m_currentWal.pop_front();
        }
    }

    s_perfStableBytes -= (unsigned) removed;
    m_stableBytes -= removed;
}

//===========================================================================
void DbPage::saveOldPages_LK() {
    if (m_oldPages.empty())
        return;

    deque<DirtyPageInfo> pages;
    uint64_t savedLsn = 0;
    for (;;) {
        auto dpi = m_oldPages.front();
        if (dpi.hdr->lsn > m_stableLsn)
            break;
        savedLsn = dpi.lsn;
        pages.push_back(dpi);
        m_oldPages.pop_front();
        if (m_oldPages.empty())
            break;
    }

    if (!pages.empty()) {
        m_workMut.unlock();
        for (auto && dpi : pages)
            writePageWait(dpi.hdr);
        m_workMut.lock();
        for (auto && dpi : pages) {
            assert(m_pages[dpi.hdr->pgno] != dpi.hdr);
            freePage_LK(dpi.hdr);
        }

        removeWalPages_LK(savedLsn);
    }
}

//===========================================================================
Duration DbPage::onSaveTimer(TimePoint now) {
    unique_lock lk{m_workMut};
    saveOldPages_LK();

    if (m_dirtyPages.empty())
        return kTimerInfinite;

    auto buf = make_unique<char[]>(m_pageSize);
    auto tmpHdr = reinterpret_cast<DbPageHeader *>(buf.get());
    uint64_t savedLsn = 0;
    unsigned saved = 0;
    auto minTime = now - m_maxDirtyAge;
    auto minDataLsn = m_overflowBytes
        ? m_currentWal.front().lsn
        : 0;
    while (!m_dirtyPages.empty()) {
        auto dpi = m_dirtyPages.front();
        // Make sure that we've saved:
        //  - at least one page
        //  - all pages older than max age
        //  - enough pages to clear out the overflow bytes
        if (saved
            && dpi.time > minTime
            && dpi.lsn >= minDataLsn
        ) {
            break;
        }
        m_dirtyPages.pop_front();
        saved += 1;
        dpi.hdr->flags &= ~fDbPageDirty;
        s_perfDirtyPages -= 1;

        if (dpi.hdr->lsn > m_stableLsn) {
            m_oldPages.push_back(dpi);
            m_oldPages.back().hdr = dupPage_LK(dpi.hdr);
        } else {
            savedLsn = dpi.lsn;
            auto was = dpi.hdr->lsn;
            memcpy(tmpHdr, dpi.hdr, m_pageSize);
            lk.unlock();
            writePageWait(tmpHdr);
            lk.lock();
            assert(m_pages[dpi.hdr->pgno] == dpi.hdr);
            if (was == dpi.hdr->lsn) {
                // the page didn't change, free it
                m_pages[dpi.hdr->pgno] = nullptr;
                freePage_LK(dpi.hdr);
            }
        }
    }

    if (m_oldPages.empty() && savedLsn)
        removeWalPages_LK(savedLsn);

    return untilNextSave_LK();
}

//===========================================================================
void DbPage::writePageWait(DbPageHeader * hdr) {
    assert(hdr->pgno != (uint32_t) -1);
    s_perfWrites += 1;
    hdr->flags = 0;
    hdr->checksum = hash_crc32c(hdr, m_pageSize);
    fileWriteWait(m_fdata, hdr->pgno * m_pageSize, hdr, m_pageSize);
}

//===========================================================================
void DbPage::freePage_LK(DbPageHeader * hdr) {
    hdr->pgno = kFreePageMark;
    auto wpno = m_vwork.pgno(hdr);
    m_freeWorkPages.insert(wpno);
    s_perfFreePages += 1;
}

//===========================================================================
DbPageHeader * DbPage::dupPage_LK(const DbPageHeader * hdr) {
    auto wpno = (uint32_t) 0;
    if (m_freeWorkPages.empty()) {
        wpno = (uint32_t) m_workPages++;
        m_vwork.growToFit(wpno);
        s_perfPages += 1;
    } else {
        wpno = m_freeWorkPages.pop_front();
        s_perfFreePages -= 1;
    }
    auto ptr = (DbPageHeader *) m_vwork.wptr(wpno);
    memcpy(ptr, hdr, m_pageSize);
    ptr->flags = 0;
    return ptr;
}

//===========================================================================
void * DbPage::dirtyPage_LK(
    DbPageHeader * hdr,
    uint32_t pgno,
    uint64_t lsn
) {
    if (~hdr->flags & fDbPageDirty) {
        hdr->flags |= fDbPageDirty;
        m_dirtyPages.push_back({hdr, Clock::now(), lsn});
        s_perfDirtyPages += 1;
    }
    hdr->pgno = pgno;
    hdr->lsn = lsn;
    return hdr;
}

//===========================================================================
void * DbPage::onLogGetRedoPtr(
    uint32_t pgno,
    uint64_t lsn,
    uint16_t localTxn
) {
    if (pgno >= m_pages.size()) {
        m_vdata.growToFit(pgno);
        m_pages.resize(pgno + 1);
    }
    auto hdr = m_pages[pgno];
    if (!hdr) {
        // create new dirty page from clean page
        auto src = reinterpret_cast<const DbPageHeader *>(m_vdata.rptr(pgno));
        if (lsn <= src->lsn)
            return nullptr;
        hdr = dupPage_LK(src);
        m_pages[pgno] = hdr;
    } else {
        if (lsn <= hdr->lsn)
            return nullptr;
    }
    return dirtyPage_LK(hdr, pgno, lsn);
}

//===========================================================================
void * DbPage::onLogGetUpdatePtr(
    uint32_t pgno,
    uint64_t lsn,
    uint16_t localTxn
) {
    assert(lsn);
    unique_lock lk{m_workMut};
    assert(pgno < m_pages.size());
    auto hdr = m_pages[pgno];
    if (!hdr) {
        // create new dirty page from clean page
        auto src = reinterpret_cast<const DbPageHeader *>(m_vdata.rptr(pgno));
        hdr = dupPage_LK(src);
        m_pages[pgno] = hdr;
    }
    return dirtyPage_LK(hdr, pgno, lsn);
}

//===========================================================================
const void * DbPage::rptr(uint64_t lsn, uint32_t pgno) const {
    unique_lock lk{m_workMut};
    assert(pgno < m_pages.size());
    if (auto hdr = m_pages[pgno])
        return hdr;

    return m_vdata.rptr(pgno);
}
