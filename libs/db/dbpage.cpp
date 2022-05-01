// Copyright Glen Knowles 2017 - 2022.
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

// Must be a multiple of fileViewAlignment().
size_t const kViewSize = 0x100'0000; // 16MiB
size_t const kDefaultFirstViewSize = 2 * kViewSize;


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

uint32_t const kWorkPageTypeZero = 'wZ';

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
static auto & s_perfPinnedPages = uperf("db.work pages (pinned)");
static auto & s_perfFreePages = uperf("db.work pages (free)");
static auto & s_perfDirtyPages = uperf("db.work pages (dirty)");
static auto & s_perfCleanPages = uperf("db.work pages (clean)");
static auto & s_perfOldPages = uperf("db.work pages (old)");
static auto & s_perfBonds = uperf("db.work bonds");
static auto & s_perfWrites = uperf("db.work writes (total)");
static auto & s_perfDurableBytes = uperf(
    "db.wal durable bytes",
    PerfFormat::kSiUnits
);
static auto & s_perfReqWalPages = uperf("db.wal pages (required)");


/****************************************************************************
*
*   DbPage
*
***/

//===========================================================================
DbPage::DbPage()
    : m_maxWalAge{kDefaultMaxCheckpointInterval}
    , m_maxWalBytes{kDefaultMaxCheckpointData}
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
    size_t walPageSize,
    EnumFlags<DbOpenFlags> flags
) {
    assert(pageSize);
    assert(pageSize == bit_ceil(pageSize));
    assert(pageSize >= kMinPageSize);
    assert(walPageSize / pageSize * pageSize == walPageSize);

    m_pageSize = pageSize;
    m_walPageSize = walPageSize;
    m_flags = flags;
    if (m_flags.any(fDbOpenVerbose))
        logMsgInfo() << "Open data files";
    if (!openData(datafile))
        return false;
    if (!openWork(workfile)) {
        close();
        return false;
    }
    m_currentWal.push_back({0, timeNow()});

    return true;
}

//===========================================================================
bool DbPage::openData(string_view datafile) {
    using enum File::OpenMode;
    auto oflags = fReadWrite | fDenyWrite | fRandom;
    if (m_flags.any(fDbOpenCreat))
        oflags |= fCreat;
    if (m_flags.any(fDbOpenTrunc))
        oflags |= fTrunc;
    if (m_flags.any(fDbOpenExcl))
        oflags |= fExcl;
    auto ec = fileOpen(&m_fdata, datafile, oflags);
    if (!m_fdata)
        return false;
    Finally fin([fh = m_fdata, fl = m_flags, datafile]() {
        fileClose(fh);
        if (fl.all(fDbOpenCreat | fDbOpenExcl))
            fileRemove(datafile);
    });

    uint64_t len = 0;
    if (fileSize(&len, m_fdata))
        return false;
    if (!len) {
        DbPageHeader hdr = {};
        if (fileWriteWait(nullptr, m_fdata, 0, &hdr, sizeof(hdr))) {
            logMsgError() << "Open new failed, " << datafile;
            return false;
        }
        m_newFiles = true;
    }
    if (!m_vdata.open(m_fdata, kViewSize, m_pageSize)) {
        logMsgError() << "Open view failed, " << datafile;
        return false;
    }

    // Remove trailing blank pages from page count.
    auto lastPage = (pgno_t) (len / m_pageSize);
    while (lastPage) {
        lastPage = pgno_t(lastPage - 1);
        auto p = static_cast<const DbPageHeader *>(m_vdata.rptr(lastPage));
        if (p->type != DbPageType::kInvalid)
            break;
    }
    m_pages.resize(lastPage + 1);

    fin.release();
    return true;
}

//===========================================================================
bool DbPage::openWork(string_view workfile) {
    using enum File::OpenMode;
    auto oflags = fTemp | fReadWrite | fDenyWrite | fBlocking | fRandom;
    // Opening the data file has already succeeded, so always create the work
    // file (if not exist).
    oflags |= fCreat;
    if (m_flags.any(fDbOpenExcl))
        oflags |= fExcl;
    auto ec = fileOpen(&m_fwork, workfile, oflags);
    if (!m_fwork)
        return false;
    Finally fin([fh = m_fwork, fl = m_flags, workfile]() {
        fileClose(fh);
        if (fl.all(fDbOpenCreat | fDbOpenExcl))
            fileRemove(workfile);
    });

    uint64_t len = 0;
    fileSize(&len, m_fwork);
    ZeroPage zp{};
    if (!len) {
        zp.hdr.type = (DbPageType) kWorkPageTypeZero;
        memcpy(zp.signature, kWorkFileSig, sizeof(zp.signature));
        zp.pageSize = (unsigned) m_pageSize;
        if (fileWriteWait(nullptr, m_fwork, 0, &zp, sizeof(zp))) {
            logMsgError() << "Open new failed, " << workfile;
            return false;
        }
        len = m_pageSize;
    } else {
        fileReadWait(nullptr, &zp, sizeof(zp), m_fwork, 0);
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

    fin.release();
    return true;
}

//===========================================================================
DbConfig DbPage::configure(const DbConfig & conf) {
    // Checkpoint configuration is assumed to have already been validated by
    // DbWal.
    assert(conf.checkpointMaxInterval.count());
    assert(conf.checkpointMaxData);

    unique_lock lk{m_workMut};
    m_maxWalAge = conf.checkpointMaxInterval;
    m_maxWalBytes = conf.checkpointMaxData;
    queueSaveWork_LK();

    return conf;
}

//===========================================================================
void DbPage::close() {
    m_pages.clear();
    m_dirtyPages.clear();
    m_oldPages.clear();
    m_cleanPages.clear();
    m_pageBonds = 0;
    m_freeInfos.clear();
    m_referencePages.clear();
    m_currentWal.clear();
    m_overflowWal.clear();
    m_durableWalBytes = 0;
    m_overflowWalBytes = 0;

    m_vdata.close();
    fileClose(m_fdata);
    m_vwork.close();

    // TODO: Resize to number of dirty pages at start of last checkpoint.
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
*   In order to ensure consistency, interdependent changes to multiple pages are
*   grouped togather in transactions.
*
*   An incrementing log sequence number (LSN) is assigned to each record written
*   to the write-ahead log (WAL).
*
*   Life cycle of page update (short story):
*    1. Data page updated in memory.
*    2. Record of update saved to WAL, update is now fully durable (will survive
*       a crash).
*    3. Data page saved.
*    4. WAL record discarded.
*
*   Life cycle of page update (long story):
*    1.  Record of update created, added to the in memory page of the
*        write-ahead log (WAL).
*    2.  Update applied to in memory data page (making it dirty) by processing
*        the WAL record.
*    3.  WAL page containing record is saved to stable storage, thus becoming
*        durable. WAL pages are written when they become full or after a short
*        time (500ms) of WAL inactivity.
*    4.  Now that it's corresponding WAL record has been saved the update is
*        durable (will survive a crash) and the in memory data page is eligible
*        to be saved to stable storage.
*    5.  Data page becomes most senior (smallest LSN) eligible page.
*    6a. If page has been updated by a newer, not yet durable, WAL record:
*        1. Copy of page added to old pages list.
*        2. Data page is marked as no longer dirty and therefore no longer
*           eligible to be saved, promoting next eldest to most senior. But it
*           is not discarded.
*        3. WAL page containing newer update becomes durable.
*        4. Copy of page in old pages list discarded.
*    6b. Otherwise (all changes to page are from durable WAL records):
*        1. Page is written and discarded from memory, promoting next eldest to
*           new most senior.
*    7.  Eventually the next checkpoint begins. Either enough time passed (or
*        WAL data written) since the last checkpoint to trigger one.
*    8.  Checkpoint ensures that all written pages are written to stable storage
*        and not just to the operating system's cache.
*    9.  Record of checkpoint created, added to in memory WAL page.
*   10.  WAL page containing checkpoint record becomes durable.
*   11.  The WAL is truncated, freeing all pages older than the checkpoint.
*   12.  Update is fully incorporated into the data pages and no longer exists
*        in the WAL.
*
***/

//===========================================================================
void DbPage::onWalDurable(uint64_t lsn, size_t bytes) {
    unique_lock lk{m_workMut};
    m_durableLsn = lsn;
    if (bytes) {
        s_perfDurableBytes += (unsigned) bytes;
        s_perfReqWalPages += (unsigned) (bytes / m_walPageSize);
        m_durableWalBytes += bytes;
    }
    m_currentWal.push_back({lsn, timeNow(), bytes});

    // If adding this new wal page caused the limit to be exceeded; then move
    // oldest entries to overflow until it's back within the limit.
    while (m_durableWalBytes - m_overflowWalBytes > m_maxWalBytes) {
        auto wi = m_currentWal.front();
        m_overflowWal.push_back(wi);
        m_currentWal.pop_front();
        m_overflowWalBytes += wi.bytes;
    }

    queueSaveWork_LK();
}

//===========================================================================
uint64_t DbPage::onWalCheckpointPages(uint64_t lsn) {
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

    if (fileFlush(m_fdata))
        logMsgFatal() << "Checkpointing failed.";
    return lsn;
}

//===========================================================================
Duration DbPage::untilNextSave_LK() {
    if (m_oldPages && m_durableLsn >= m_oldPages.front()->hdr->lsn)
        return 0ms;
    if (!m_dirtyPages)
        return kTimerInfinite;
    auto front = m_dirtyPages.front();
    if (m_overflowWalBytes && m_durableLsn >= front->hdr->lsn)
        return 0ms;

    auto minTime = timeNow() - m_maxWalAge;
    auto maxWait = front->firstTime - minTime;
    auto wait = m_maxWalAge / m_pageBonds;
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
    auto now = timeNow();
    auto lastTime = m_lastSaveTime;
    m_lastSaveTime = now;

    unique_lock lk{m_workMut};
    if (m_saveInProgress)
        return;
    m_saveInProgress = true;

    saveOldPages_LK();

    if (!m_dirtyPages) {
        m_saveInProgress = false;
        return;
    }

    auto minTime = now - m_maxWalAge;
    auto minDataLsn = m_overflowWalBytes
        ? m_currentWal.front().lsn
        : 0;
    size_t minSaves = 1;
    if (!empty(lastTime)) {
        if (auto elapsed = now - lastTime; elapsed > 0ms) {
            if (auto multiple = m_maxWalAge / elapsed; multiple > 0) {
                minSaves = m_pageBonds
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
        // Make sure that we've saved:
        //  - at least one page.
        //  - a percentage of pages equal to the percentage of time remaining
        //      that it's been since the last save event.
        //  - all pages older than max age.
        //  - enough pages to clear out the overflow bytes.
        auto pi = m_dirtyPages.front();
        assert(pi->hdr);
        if (saved >= minSaves
            && pi->firstTime > minTime
            && pi->firstLsn >= minDataLsn
        ) {
            break;
        }
        saved += 1;
        m_cleanPages.link(pi);
        pi->flags.reset(fDbPageDirty);
        s_perfDirtyPages -= 1;
        s_perfCleanPages += 1;

        while (pi->updates)
            m_workCv.wait(lk);

        if (pi->hdr->lsn > m_durableLsn) {
            // Page needs to be saved, but has been updated by an LSN that is
            // not yet durable. The page is copied to old pages, where it is
            // held until the all it's updates become durable. Meanwhile, new
            // updates continue to be made to the original copy.
            auto npi = allocWorkInfo_LK();
            m_oldPages.link(npi);
            npi->hdr = dupPage_LK(pi->hdr);
            npi->firstTime = pi->firstTime;
            npi->firstLsn = pi->firstLsn;
            npi->flags = pi->flags;
            s_perfOldPages += 1;
        } else {
            // Page needs to be saved and doesn't have an unsaved LSN.
            savedLsn = pi->firstLsn;
            auto was = pi->hdr->lsn;
            memcpy(tmpHdr, pi->hdr, m_pageSize);

            lk.unlock();
            writePageWait(tmpHdr);
            lk.lock();
            assert(m_pages[pi->hdr->pgno] == pi);
            if (!pi->pins && was == pi->hdr->lsn) {
                // The page didn't change, free it.
                pi->pgno = pi->hdr->pgno;
                freePage_LK(pi->hdr);
                pi->hdr = nullptr;
                s_perfCleanPages -= 1;
            }
        }
    }

    if (!m_oldPages && savedLsn)
        removeWalPages_LK(savedLsn);

    m_saveInProgress = false;
    queueSaveWork_LK();
}

//===========================================================================
void DbPage::saveOldPages_LK() {
    if (!m_oldPages)
        return;

    List<WorkPageInfo> pages;
    uint64_t savedLsn = 0;
    while (auto pi = m_oldPages.front()) {
        if (pi->hdr->lsn > m_durableLsn)
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
            s_perfOldPages -= 1;
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
// Remove WAL info entries that have had all their pages committed.
void DbPage::removeWalPages_LK(uint64_t lsn) {
    assert(lsn);
    size_t bytes = 0;
    size_t matured = 0;

    while (m_overflowWal.size() > 1 && lsn >= m_overflowWal[1].lsn
        || m_overflowWal.size() == 1 && lsn >= m_currentWal.front().lsn
    ) {
        auto & pi = m_overflowWal.front();
        if (auto val = pi.bytes)
            bytes += val;
        m_overflowWal.pop_front();
    }
    m_overflowWalBytes -= bytes;

    if (m_overflowWal.empty()) {
        while (m_currentWal.size() > 1 && lsn >= m_currentWal[1].lsn) {
            auto & pi = m_currentWal.front();
            if (auto val = pi.bytes)
                bytes += val;
            m_currentWal.pop_front();
        }
    }

    if (m_cleanPages) {
        auto minTime = timeNow() - m_maxWalAge;
        while (auto pi = m_cleanPages.front()) {
            if (pi->firstTime > minTime)
                break;
            matured += 1;
            auto pgno = pi->hdr ? pi->hdr->pgno : pi->pgno;
            assert(m_pages[pgno] == pi);
            m_pages[pgno] = nullptr;
            if (pi->hdr) {
                freePage_LK(pi->hdr);
                s_perfCleanPages -= 1;
            }
            freeWorkInfo_LK(pi);
        }
    }

    s_perfDurableBytes -= (unsigned) bytes;
    m_durableWalBytes -= bytes;
    s_perfReqWalPages -= (unsigned) (bytes / m_walPageSize);
    m_pageBonds -= matured;
    s_perfBonds -= (unsigned) matured;
}

//===========================================================================
void DbPage::writePageWait(DbPageHeader * hdr) {
    assert(hdr->pgno != kFreePageMark);
    s_perfWrites += 1;
    hdr->checksum = 0;
    hdr->checksum = hash_crc32c(hdr, m_pageSize);
    fileWriteWait(nullptr, m_fdata, hdr->pgno * m_pageSize, hdr, m_pageSize);
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
const void * DbPage::rptr(uint64_t lsn, pgno_t pgno, bool withPin) {
    unique_lock lk{m_workMut};
    assert(pgno < m_pages.size());
    auto pi = m_pages[pgno];
    if (!pi) {
        assert(withPin);
        pi = allocWorkInfo_LK();
        m_pages[pgno] = pi;
        m_referencePages.link(pi);
        pi->pgno = pgno;
    }
    if (withPin) {
        while (pi->updates)
            m_workCv.wait(lk);
        if (!pi->pins)
            s_perfPinnedPages += 1;
        pi->pins += 1;
    } else {
        assert(pi->pins);
    }
    return pi->hdr
        ? pi->hdr
        : m_vdata.rptr(pgno);
}

//===========================================================================
void DbPage::unpin(const UnsignedSet & pages) {
    unique_lock lk{m_workMut};
    bool notify = false;
    for (auto&& pgno : pages) {
        auto pi = m_pages[pgno];
        assert(pi);
        if (!--pi->pins) {
            s_perfPinnedPages -= 1;
            if (!pi->hdr) {
                // Don't track reference only pages that are no longer pinned.
                freeWorkInfo_LK(pi);
                m_pages[pgno] = nullptr;
            }
            notify = true;
        }
    }
    lk.unlock();
    if (notify)
        m_workCv.notify_all();
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
    pi->pins = 0;
    pi->updates = 0;
    return pi;
}

//===========================================================================
void DbPage::freeWorkInfo_LK(WorkPageInfo * pi) {
    m_freeInfos.link(pi);
}

//===========================================================================
void * DbPage::onWalGetPtrForRedo(
    pgno_t pgno,
    uint64_t lsn,
    uint16_t localTxn
) {
    // Only used during recovery, which is inherently single threaded, so no
    // locking needed.

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
    pi = dirtyPage_LK(pgno, lsn);
    return pi->hdr;
}

//===========================================================================
void * DbPage::onWalGetPtrForUpdate(
    pgno_t pgno,
    uint64_t lsn,
    uint16_t localTxn
) {
    assert(lsn);
    unique_lock lk{m_workMut};
    assert(pgno < m_pages.size());
    auto pi = m_pages[pgno];
    while (pi && (pi->pins > 1 || pi->updates))
        m_workCv.wait(lk);
    assert(pi->pins == 1);
    pi = dirtyPage_LK(pgno, lsn);
    pi->updates = true;
    return pi->hdr;
}

//===========================================================================
void DbPage::onWalUnlockPtr(pgno_t pgno) {
    unique_lock lk{m_workMut};
    assert(pgno < m_pages.size());
    auto pi = m_pages[pgno];
    assert(pi->updates);
    pi->updates = false;
    lk.unlock();
    m_workCv.notify_all();
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
DbPage::WorkPageInfo * DbPage::dirtyPage_LK(pgno_t pgno, uint64_t lsn) {
    auto pi = m_pages[pgno];
    if (!pi) {
        // Page was untracked, create page info for it.
        pi = allocWorkInfo_LK();
        m_pages[pgno] = pi;
    }
    if (!pi->hdr) {
        // Create new dirty page from clean or referrence page.
        auto src = reinterpret_cast<const DbPageHeader *>(m_vdata.rptr(pgno));
        pi->hdr = dupPage_LK(src);
        pi->pgno = {};
        if (!pi->firstLsn) {
            // If dirtying reference or untracked page, add page bond.
            m_pageBonds += 1;
            s_perfBonds += 1;
        }
    }
    assert(pi->hdr && !pi->pgno);
    pi->hdr->pgno = pgno;
    pi->hdr->lsn = lsn;
    if (pi->flags.none(fDbPageDirty)) {
        pi->firstTime = timeNow();
        pi->firstLsn = lsn;
        pi->flags |= fDbPageDirty;
        m_dirtyPages.link(pi);
        s_perfDirtyPages += 1;
        if (m_dirtyPages.front() == pi)
            queueSaveWork_LK();
    }
    return pi;
}
