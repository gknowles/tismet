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
static auto & s_perfAmortized = uperf("db.work pages (amortized)");
static auto & s_perfWrites = uperf("db.work writes (total)");
static auto & s_perfStableBytes = uperf("db.work stable bytes");
static auto & s_perfReqWalPages = uperf("db.wal pages (required)");


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
    auto lastPage = (pgno_t) (len / m_pageSize);
    while (lastPage) {
        lastPage = pgno_t(lastPage - 1);
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
    m_cleanPages.clear();
    m_pageDebt = 0;
    m_freeInfos.clear();
    m_currentWal.clear();
    m_overflowWal.clear();
    m_stableBytes = 0;
    m_overflowBytes = 0;

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


/****************************************************************************
*
*   DbPage - save and checkpoint
*
***/

//===========================================================================
void DbPage::onLogStable(uint64_t lsn, size_t bytes) {
    unique_lock lk{m_workMut};
    m_stableLsn = lsn;
    if (bytes) {
        s_perfStableBytes += (unsigned) bytes;
        s_perfReqWalPages += 1;
        m_stableBytes += bytes;
    }
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
    if (m_oldPages && m_stableLsn >= m_oldPages.front()->hdr->lsn)
        return 0ms;
    if (!m_dirtyPages)
        return kTimerInfinite;
    auto front = m_dirtyPages.front();
    if (m_overflowBytes && m_stableLsn >= front->hdr->lsn)
        return 0ms;

    auto minTime = Clock::now() - m_maxDirtyAge;
    auto maxWait = front->firstTime - minTime;
    auto wait = m_maxDirtyAge / m_pageDebt;
    if (wait > maxWait) wait = maxWait;
    if (wait < 0ms) wait = 0ms;
    return wait;
}

//===========================================================================
void DbPage::queueSaveWork_LK() {
    auto wait = untilNextSave_LK();
    timerUpdate(&m_saveTimer, wait, true);
}

//===========================================================================
Duration DbPage::onSaveTimer(TimePoint now) {
    taskPushCompute([&]() { saveWork(); });
    return kTimerInfinite;
}

//===========================================================================
void DbPage::saveWork() {
    auto now = Clock::now();
    auto lastTime = m_lastSaveTime;
    m_lastSaveTime = now;

    unique_lock lk{m_workMut};
    saveOldPages_LK();

    if (!m_dirtyPages)
        return;

    auto minTime = now - m_maxDirtyAge;
    auto minDataLsn = m_overflowBytes
        ? m_currentWal.front().lsn
        : 0;
    size_t minSaves = 1;
    if (lastTime) {
        if (auto elapsed = now - lastTime; elapsed > 0ms) {
            if (auto multiple = m_maxDirtyAge / elapsed; multiple > 0) {
                minSaves = m_pageDebt
                    + now.time_since_epoch().count() % multiple;
                minSaves /= multiple;
                if (minSaves <= 0)
                    minSaves = 1;
            }
        }
    }

    auto buf = make_unique<char[]>(m_pageSize);
    auto tmpHdr = reinterpret_cast<DbPageHeader *>(buf.get());
    uint64_t savedLsn = 0;
    unsigned saved = 0;
    while (m_dirtyPages) {
        auto pi = m_dirtyPages.front();
        // Make sure that we've saved:
        //  - at least one page
        //  - a percentage of pages equal to the percentage of time remaining
        //      that it's been since the last save event.
        //  - all pages older than max age
        //  - enough pages to clear out the overflow bytes
        if (saved >= minSaves
            && pi->firstTime > minTime
            && pi->firstLsn >= minDataLsn
        ) {
            break;
        }
        saved += 1;
        m_cleanPages.link(pi);
        pi->flags &= ~fDbPageDirty;
        s_perfDirtyPages -= 1;

        if (pi->hdr->lsn > m_stableLsn) {
            auto npi = allocWorkInfo_LK();
            m_oldPages.link(npi);
            npi->hdr = dupPage_LK(pi->hdr);
            npi->firstTime = pi->firstTime;
            npi->firstLsn = pi->firstLsn;
            npi->pgno = {};
            npi->flags = pi->flags;
        } else {
            savedLsn = pi->firstLsn;
            auto was = pi->hdr->lsn;
            memcpy(tmpHdr, pi->hdr, m_pageSize);
            lk.unlock();
            writePageWait(tmpHdr);
            lk.lock();
            assert(m_pages[pi->hdr->pgno] == pi);
            if (was == pi->hdr->lsn) {
                // the page didn't change, free it
                pi->pgno = pi->hdr->pgno;
                freePage_LK(pi->hdr);
                pi->hdr = nullptr;
            }
        }
    }

    if (!m_oldPages && savedLsn)
        removeWalPages_LK(savedLsn);

    queueSaveWork_LK();
}

//===========================================================================
void DbPage::saveOldPages_LK() {
    if (!m_oldPages)
        return;

    List<WorkPageInfo> pages;
    uint64_t savedLsn = 0;
    while (auto pi = m_oldPages.front()) {
        if (pi->hdr->lsn > m_stableLsn)
            break;
        savedLsn = pi->firstLsn;
        pages.link(pi);
    }

    if (pages) {
        m_workMut.unlock();
        for (auto && pi : pages)
            writePageWait(pi.hdr);
        m_workMut.lock();
        while (auto pi = pages.front()) {
            assert(m_pages[pi->hdr->pgno] != pi);
            freePage_LK(pi->hdr);
            freeWorkInfo_LK(pi);
        }

        removeWalPages_LK(savedLsn);
    }
}

//===========================================================================
void DbPage::freePage_LK(DbPageHeader * hdr) {
    hdr->pgno = kFreePageMark;
    auto wpno = m_vwork.pgno(hdr);
    m_freeWorkPages.insert(wpno);
    s_perfFreePages += 1;
}

//===========================================================================
// Remove WAL info entries that have had all their pages committed
void DbPage::removeWalPages_LK(uint64_t lsn) {
    assert(lsn);
    size_t bytes = 0;
    size_t pages = 0;
    size_t debt = 0;

    while (m_overflowWal.size() > 1 && lsn >= m_overflowWal[1].lsn
        || m_overflowWal.size() == 1 && lsn >= m_currentWal.front().lsn
    ) {
        auto & pi = m_overflowWal.front();
        if (auto val = pi.bytes) {
            bytes += val;
            pages += 1;
        }
        m_overflowWal.pop_front();
    }
    m_overflowBytes -= bytes;

    if (m_overflowWal.empty()) {
        while (m_currentWal.size() > 1 && lsn >= m_currentWal[1].lsn) {
            auto & pi = m_currentWal.front();
            if (auto val = pi.bytes) {
                bytes += val;
                pages += 1;
            }
            m_currentWal.pop_front();
        }
    }

    if (m_cleanPages) {
        auto minTime = Clock::now() - m_maxDirtyAge;
        while (auto pi = m_cleanPages.front()) {
            if (pi->firstTime > minTime)
                break;
            debt += 1;
            auto pgno = pi->hdr ? pi->hdr->pgno : pi->pgno;
            assert(m_pages[pgno] == pi);
            m_pages[pgno] = nullptr;
            freeWorkInfo_LK(pi);
        }
    }

    s_perfStableBytes -= (unsigned) bytes;
    m_stableBytes -= bytes;
    s_perfReqWalPages -= (unsigned) pages;
    m_pageDebt -= debt;
    s_perfAmortized -= (unsigned) debt;
}

//===========================================================================
void DbPage::writePageWait(DbPageHeader * hdr) {
    assert(hdr->pgno != kFreePageMark);
    s_perfWrites += 1;
    hdr->checksum = 0;
    hdr->checksum = hash_crc32c(hdr, m_pageSize);
    fileWriteWait(m_fdata, hdr->pgno * m_pageSize, hdr, m_pageSize);
}


/****************************************************************************
*
*   DbPage - query and update
*
***/

//===========================================================================
void DbPage::growToFit(pgno_t pgno) {
    unique_lock lk{m_workMut};
    if (pgno < m_pages.size())
        return;
    assert(pgno == m_pages.size());
    m_vdata.growToFit(pgno);
    m_pages.resize(pgno + 1);
}

//===========================================================================
const void * DbPage::rptr(uint64_t lsn, pgno_t pgno) const {
    unique_lock lk{m_workMut};
    assert(pgno < m_pages.size());
    auto pi = m_pages[pgno];
    if (pi && pi->hdr)
        return pi->hdr;

    return m_vdata.rptr(pgno);
}

//===========================================================================
DbPage::WorkPageInfo * DbPage::allocWorkInfo_LK() {
    auto pi = m_freeInfos.back();
    if (!pi)
        pi = new WorkPageInfo;
    pi->hdr = nullptr;
    pi->firstTime = {};
    pi->firstLsn = 0;
    pi->flags = {};
    pi->pgno = {};
    return pi;
}

//===========================================================================
void DbPage::freeWorkInfo_LK(WorkPageInfo * pi) {
    m_freeInfos.link(pi);
}

//===========================================================================
void * DbPage::onLogGetRedoPtr(
    pgno_t pgno,
    uint64_t lsn,
    uint16_t localTxn
) {
    if (pgno >= m_pages.size()) {
        m_vdata.growToFit(pgno);
        m_pages.resize(pgno + 1);
    }
    auto pi = m_pages[pgno];
    if (!pi || !pi->hdr) {
        // create new dirty page from clean page
        auto src = reinterpret_cast<const DbPageHeader *>(m_vdata.rptr(pgno));
        if (lsn <= src->lsn)
            return nullptr;
    } else if (lsn <= pi->hdr->lsn) {
        return nullptr;
    }
    return dirtyPage_LK(pgno, lsn);
}

//===========================================================================
void * DbPage::onLogGetUpdatePtr(
    pgno_t pgno,
    uint64_t lsn,
    uint16_t localTxn
) {
    assert(lsn);
    unique_lock lk{m_workMut};
    assert(pgno < m_pages.size());
    return dirtyPage_LK(pgno, lsn);
}

//===========================================================================
DbPageHeader * DbPage::dupPage_LK(const DbPageHeader * hdr) {
    pgno_t wpno = {};
    if (m_freeWorkPages) {
        wpno = (pgno_t) m_freeWorkPages.pop_front();
        s_perfFreePages -= 1;
    } else {
        wpno = (pgno_t) m_workPages++;
        m_vwork.growToFit(wpno);
        s_perfPages += 1;
    }
    auto ptr = (DbPageHeader *) m_vwork.wptr(wpno);
    memcpy(ptr, hdr, m_pageSize);
    return ptr;
}

//===========================================================================
void * DbPage::dirtyPage_LK(pgno_t pgno, uint64_t lsn) {
    auto pi = m_pages[pgno];
    if (!pi || !pi->hdr) {
        // create new dirty page from clean page
        auto src = reinterpret_cast<const DbPageHeader *>(m_vdata.rptr(pgno));
        if (!pi) {
            pi = allocWorkInfo_LK();
            m_pages[pgno] = pi;
            m_pageDebt += 1;
            s_perfAmortized += 1;
        }
        pi->hdr = dupPage_LK(src);
        pi->pgno = {};
    }
    assert(pi->hdr && !pi->pgno);
    if (~pi->flags & fDbPageDirty) {
        pi->firstTime = Clock::now();
        pi->firstLsn = lsn;
        pi->flags |= fDbPageDirty;
        m_dirtyPages.link(pi);
        s_perfDirtyPages += 1;
        if (m_dirtyPages.front() == pi)
            queueSaveWork_LK();
    }
    pi->hdr->pgno = pgno;
    pi->hdr->lsn = lsn;
    return pi->hdr;
}
