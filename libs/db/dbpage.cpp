// Copyright Glen Knowles 2017 - 2023.
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

const Guid kWorkFileSig = "51fde6a6-6438-444a-ae02-438b4b07b284"_Guid;

uint32_t const kWorkPageTypeZero = 'wZ';

struct ZeroPage {
    DbPageHeader hdr;
    Guid signature;
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
static auto & s_perfCleanToDirty = uperf("db.work clean to dirty");
static auto & s_perfCleanToFree = uperf("db.work clean to free");
static auto & s_perfOverduePages = uperf("db.work pages (overdue)");
static auto & s_perfBonds = uperf("db.work bonds");
static auto & s_perfWrites = uperf("db.work writes (total)");
static auto & s_perfDurableBytes = uperf(
    "db.wal durable bytes",
    PerfFormat::kSiUnits
);
// Saved WAL pages that are referenced by unsaved work pages.
static auto & s_perfRefWalPages = uperf("db.wal pages (referenced)");


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
    m_newFiles = false;
    if (m_flags.any(fDbOpenVerbose))
        logMsgInfo() << "Open data files";
    if (!openData(datafile))
        return false;
    if (!openWork(workfile)) {
        close();
        return false;
    }
    // There must always be at least one current WAL reference.
    m_currentWal.push_back({.time = timeNow()});

    return true;
}

//===========================================================================
bool DbPage::openData(string_view datafile) {
    using enum File::OpenMode;
    auto oflags = fReadWrite | fDenyWrite | fRandom;
    if (m_flags.any(fDbOpenCreat))
        oflags |= fCreat | fRemove;
    if (m_flags.any(fDbOpenTrunc))
        oflags |= fTrunc;
    if (m_flags.any(fDbOpenExcl))
        oflags |= fExcl;
    auto ec = fileOpen(&m_fdata, datafile, oflags);
    if (!m_fdata) {
        logMsgError() << "Open failed, " << datafile;
        return false;
    }

    // If opened with exclusive create the file is obviously new, otherwise
    // assume it already existed until we know better.
    m_newFiles = m_flags.all(fDbOpenCreat | fDbOpenExcl);

    // Auto-close file on failure of initial processing of the opened file.
    Finally fin([&fh = m_fdata, &newf = m_newFiles]() {
        if (newf && fileMode(fh).any(fRemove)) {
            // File was created, but not completely. Remove the remnants.
            fileRemoveOnClose(fh);
        }
        fileClose(fh);
        fh = {};
    });

    uint64_t len = 0;
    if (fileSize(&len, m_fdata))
        return false;
    if (!len) {
        // Newly created file.
        m_newFiles = true;
    }
    if (!m_vdata.open(m_fdata, kViewSize, m_pageSize)) {
        logMsgError() << "Open view failed, " << datafile;
        return false;
    }

    // Open successful, don't auto-close or auto-delete.
    fin.release();

    // Remove trailing blank pages from page count.
    auto lastPage = (pgno_t) (len / m_pageSize);
    while (lastPage) {
        lastPage = pgno_t(lastPage - 1);
        auto p = static_cast<const DbPageHeader *>(m_vdata.rptr(lastPage));
        if (p->type != DbPageType::kInvalid)
            break;
    }
    m_pages.resize(lastPage + 1);

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
    if (!m_fwork) {
        logMsgError() << "Open failed, " << workfile;
        return false;
    }

    // Auto-close file on failure of initial processing of the opened file.
    Finally fin([&fh = m_fwork]() {
        // Because it's opened with fTemp, file will be auto-removed on close.
        fileClose(fh);
        fh = {};
    });

    uint64_t len = 0;
    fileSize(&len, m_fwork);
    ZeroPage zp{};
    if (!len) {
        zp.hdr.type = (DbPageType) kWorkPageTypeZero;
        zp.signature = kWorkFileSig;
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
    if (zp.signature != kWorkFileSig) {
        logMsgError() << "Bad signature, " << workfile;
        return false;
    }
    if (m_pageSize < kMinPageSize || kViewSize % m_pageSize != 0) {
        logMsgError() << "Invalid page size, " << workfile;
        return false;
    }
    m_workPages = len / m_pageSize;
    m_freeWorkPages.insert(1, (unsigned) m_workPages - 1);
    if (!m_vwork.open(m_fwork, kViewSize, m_pageSize)) {
        logMsgError() << "Open view failed, " << workfile;
        return false;
    }

    // Open successful, don't auto-close or auto-delete.
    fin.release();

    s_perfPages += (unsigned) m_workPages;
    s_perfFreePages += (unsigned) m_workPages - 1;

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
    s_perfPages -= (unsigned) m_workPages;
    s_perfFreePages -= (unsigned) m_freeWorkPages.size();

    m_pages.clear();
    m_dirtyPages.clear();
    m_overduePages.clear();
    m_cleanPages.clear();
    m_pageBonds = 0;
    m_freeInfos.clear();
    m_referencePages.clear();
    m_durableLsn = {};
    m_currentWal.clear();
    m_overflowWal.clear();
    m_overflowWalBytes = 0;
    m_durableWalBytes = 0;
    m_workPages = 0;
    m_freeWorkPages.clear();

    // Close Data File
    m_vdata.close();
    if (m_newFiles
        && !m_fwork
        && fileMode(m_fdata).any(File::OpenMode::fRemove)
    ) {
        fileRemoveOnClose(m_fdata);
    }
    fileClose(m_fdata);

    // Close Work File
    m_vwork.close();
    fileClose(m_fwork);

    // Initialized by open.
    m_pageSize = 0;
    m_walPageSize = 0;
    m_flags = {};
    m_newFiles = false;
}


/****************************************************************************
*
*   DbPage - save and checkpoint
*
*   In order to ensure consistency, interdependent changes to multiple pages
*   are grouped together in transactions.
*
*   An incrementing log sequence number (LSN) is assigned to each record
*   written to the write-ahead log (WAL).
*
*   Life cycle of page update (short story):
*    1. Data page updated in memory.
*    2. Record of update saved to WAL, update is now fully durable (will
*       survive a crash).
*    3. Data page saved.
*    4. WAL record discarded.
*
*   Life cycle of page update (long story):
*    1.  Record of update created, added to buffer of write-ahead log (WAL).
*    2.  Update applied to in memory data page (making it dirty) by processing
*        the WAL record.
*    3.  WAL page containing record is saved to stable storage, thus becoming
*        durable. WAL pages are written when they become full or after a short
*        time (500ms) of WAL inactivity.
*    4.  If the update is part of a transaction, wait until the transaction's
*        commit record is added to the WAL and that WAL page also becomes
*        durable. A single transaction may involve updates to multiple data
*        pages.
*    5.  Now that it's corresponding WAL records have been saved the update is
*        durable (will survive a crash) and the in memory data page is eligible
*        to be saved to stable storage.
*    6.  Data page becomes most senior (smallest LSN) eligible page.
*    7a. If page has been updated by a newer, not yet durable, WAL record:
*        1. Copy of page added to old pages list.
*        2. Data page is marked as no longer dirty and therefore no longer
*           eligible to be saved, promoting next eldest to most senior. But it
*           is not discarded.
*        3. WAL page containing newer update becomes durable.
*        4. Copy of page in old pages list written and discarded.
*    7b. Otherwise (all changes to page are from durable WAL records):
*        1. Page is written and discarded from memory, promoting next eldest to
*           new most senior.
*    8.  Eventually the next checkpoint begins. Either enough time passed (or
*        WAL data written) since the last checkpoint.
*    9.  Checkpoint ensures that all written pages are written to stable
*        storage and not just to the OS cache.
*   10.  Record of checkpoint created, added to in memory WAL page.
*   11.  WAL page containing checkpoint record becomes durable.
*   12.  The WAL is truncated, freeing all WAL pages older than the checkpoint.
*   13.  Update is fully incorporated into the data pages and no longer exists
*        in the WAL.
*
***/

//===========================================================================
// Called after WAL pages become durable. Reports the new durable LSN and
// number of bytes that were written to get there.
void DbPage::onWalDurable(Lsn lsn, size_t bytes) {
    unique_lock lk{m_workMut};
    m_durableLsn = lsn;
    if (bytes) {
        s_perfDurableBytes += (unsigned) bytes;
        s_perfRefWalPages += (unsigned) (bytes / m_walPageSize);
        m_durableWalBytes += bytes;
    }
    m_currentWal.push_back({lsn, timeNow(), bytes});

    // If adding this new WAL page caused the limit to be exceeded; then move
    // oldest entries to the overflow list until it's back within the limit.
    while (m_durableWalBytes - m_overflowWalBytes > m_maxWalBytes) {
        auto wi = m_currentWal.front();
        m_overflowWal.push_back(wi);
        m_currentWal.pop_front();
        m_overflowWalBytes += wi.bytes;
    }

    queueSaveWork_LK();
}

//===========================================================================
// Called when checkpointing to determine the first durable LSN that must be
// kept to protect the existing dirty pages.
Lsn DbPage::onWalCheckpointPages(Lsn lsn) {
    // Find oldest LSN that still has dirty pages relying on it. This reflects
    // the lag between changes written to WAL and written to the data file.
    Lsn oldest = {};
    {
        scoped_lock lk{m_workMut};
        if (!m_overflowWal.empty()) {
            oldest = m_overflowWal.front().lsn;
        } else if (!m_currentWal.empty()) {
            oldest = m_currentWal.front().lsn;
        }
    }
    if (oldest) {
        // If oldest is less than LSN, that means there are dirty pages relying
        // on WAL records that may no longer exist.
        assert(oldest >= lsn);

        lsn = oldest;
    }
    if (fileFlush(m_fdata))
        logMsgFatal() << "Checkpointing failed.";
    return lsn;
}

//===========================================================================
// Calculate how long to wait until another set of dirty pages should be saved.
Duration DbPage::untilNextSave_LK() {
    if (!m_durableLsn) {
        // Recovery hasn't completed, save must not be scheduled.
        return kTimerInfinite;
    }
    if (m_overduePages && m_durableLsn >= m_overduePages.front()->hdr->lsn) {
        // Pages already passed their time limit just had all their
        // dependencies become durable. Save them immediately.
        return 0ms;
    }
    if (!m_dirtyPages) {
        // There's nothing to save, so no need to schedule a save.
        return kTimerInfinite;
    }
    auto front = m_dirtyPages.front();
    if (m_overflowWalBytes && m_durableLsn >= front->hdr->lsn) {
        // Maximum WAL bytes has been exceeded and there are durably logged
        // dirty pages. Start saving them immediately so the total WAL bytes
        // can be reduced.
        return 0ms;
    }

    auto now = timeNow();
    // Earliest time at which pages that can still be left dirty could have
    // become dirty.
    auto minTime = now - m_maxWalAge;
    // How long until the first dirty page reaches it's max allowed age.
    auto maxWait = front->firstTime - minTime;
    // Interval between saves that would clear all the outstanding bonds at
    // their maturity.
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
// Saves eligible pages, frees expired data, and schedules next save.
void DbPage::saveWork() {
    unique_lock lk{m_workMut};
    if (m_saveInProgress) {
        // Abort if there's already a saveWork() in progress on another thread.
        return;
    }
    m_saveInProgress = true;

    auto lastTime = m_lastSaveTime;
    m_lastSaveTime = timeNow();
    saveOverduePages_LK();
    auto savedLsn = saveDirtyPages_LK(lastTime);
    if (savedLsn)
        removeWalPages_LK(savedLsn);
    removeCleanPages_LK();

    m_saveInProgress = false;
    queueSaveWork_LK();
}

//===========================================================================
// Save (and then free) overdue pages whose modifying LSNs have been saved.
void DbPage::saveOverduePages_LK() {
    if (!m_overduePages)
        return;

    // Get set of old pages whose WAL are durable.
    List<WorkPageInfo> pages;
    Lsn savedLsn = {};
    while (auto pi = m_overduePages.front()) {
        if (pi->hdr->lsn > m_durableLsn)
            break;
        savedLsn = pi->firstLsn;
        pages.link(pi);
    }

    if (pages) {
        // Write the selected old pages.
        unique_lock lk(m_workMut, adopt_lock);
        lk.unlock();
        for (auto && pi : pages)
            writePageWait(pi.hdr);
        lk.lock();
        lk.release();

        // Free the selected pages.
        while (auto pi = pages.front()) {
            assert(m_pages[pi->hdr->pgno] != pi);
            freePage_LK(pi->hdr);
            freeWorkInfo_LK(pi);
            s_perfOverduePages -= 1;
        }

        // Remove WAL info for the freed pages.
        removeWalPages_LK(savedLsn);
    }
}

//===========================================================================
// Cleans dirty pages either by saving or by adding a copy to overdue pages for
// a later save.
//
// Returns LSN of most recent page saved, or 0 if no pages saved. Additional
// unsaved pages for the same LSN may exist, but all prior LSNs have had their
// pages saved.
Lsn DbPage::saveDirtyPages_LK(TimePoint lastTime) {
    if (!m_dirtyPages)
        return {};

    unique_lock lk(m_workMut, adopt_lock);

    // Use the time this save was started as now.
    auto now = m_lastSaveTime;

    auto minTime = now - m_maxWalAge; // Save pages older than this.
    auto minDataLsn = m_overflowWalBytes  // Save LSNs older than this.
        ? m_currentWal.front().lsn
        : Lsn{};

    size_t minSaves = 1;    // saves required to payoff bonds on time
    if (empty(lastTime)) {
        // Must be the first call to saveWork(), elapsed time unknown, can't
        // calculate percentage of term remaining. Use minSaves of 1 so some
        // progress is made.
    } else {
        auto elapsed = now - lastTime;
        if (elapsed <= 0ms) {
            // No time elapsed since last save, leave min saves at 1.
        } else if (elapsed > m_maxWalAge) {
            // Elapsed time greater than max WAL age. Likely the configured max
            // WAL age was just reduced. Leave min saves at 1, a larger value
            // will be calculated next time around if needed.
        } else {
            // Ensure the percentage of bonds saved is at least equal to the
            // percentage of max WAL age that has elapsed since the last save.
            auto multiple = m_maxWalAge / elapsed;
            minSaves = m_pageBonds
                + now.time_since_epoch().count() % multiple;
            minSaves /= multiple;
            if (minSaves == 0)
                minSaves = 1;
        }
    }

    // Buffer to hold copy of page while it's being written.
    auto buf = make_unique<char[]>(m_pageSize);
    auto tmpHdr = reinterpret_cast<DbPageHeader *>(buf.get());

    Lsn savedLsn = {};
    unsigned saved = 0;
    while (m_dirtyPages) {
        // Make sure that we've saved:
        //  - at least one page.
        //  - a number of pages equal to a percentage of page bonds equal to
        //      the percentage of max age that it's been since the last save
        //      event.
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

        // Wait until the page is not pinned for update.
        while (pi->writePin)
            m_workCv.wait(lk);
        // Update page status from dirty to clean.
        saved += 1;
        m_cleanPages.link(pi);
        pi->flags.reset(fDbPageDirty);
        s_perfDirtyPages -= 1;
        s_perfCleanPages += 1;

        if (pi->hdr->lsn > m_durableLsn) {
            // Page needs to be saved, but has been updated by an LSN that is
            // not yet durable. Copy the page to overdue pages, where it will
            // be held until all it's updates become durable. Meanwhile, the
            // original copy will either get dirtied with new updates or freed
            // by removeCleanPages after waiting for the overdue copy to be
            // saved.
            auto npi = allocWorkInfo_LK();
            m_overduePages.link(npi);
            npi->hdr = dupPage_LK(pi->hdr);
            npi->firstTime = pi->firstTime;
            npi->firstLsn = pi->firstLsn;
            npi->flags = pi->flags;
            s_perfOverduePages += 1;
        } else {
            // Page needs to be saved and doesn't have an unsaved LSN.
            savedLsn = pi->firstLsn;
            memcpy(tmpHdr, pi->hdr, m_pageSize);

            lk.unlock();
            writePageWait(tmpHdr);
            lk.lock();
            assert(m_pages[pi->hdr->pgno] == pi);
            if (pi->flags.any(fDbPageDirty)) {
                // The page was dirtied while the mutex was unlocked, has been
                // moved out of clean pages, and we don't need to free it.
            } else {
                // The page stayed in clean pages and will eventually be either
                // dirtied or freed by removeCleanPages().
            }
        }
    }

    lk.release();
    return savedLsn;
}

//===========================================================================
// Remove clean pages that are no longer needed to proxy unsaved old pages.
void DbPage::removeCleanPages_LK() {
    if (!m_cleanPages)
        return;

    size_t freed = 0;

    // THe minimum time of first modification that clean pages must have in
    // order to be kept. They must be kept until they are older than any
    // overdue pages they may be shadowing.
    auto minTime = m_overduePages
        ? m_overduePages.front()->firstTime
        : m_lastSaveTime;

    auto next = m_cleanPages.front();
    while (next) {
        auto pi = next;
        next = m_cleanPages.next(pi);
        if (pi->firstTime >= minTime)
            break;
        if (pi->readPin) {
            // Page is pinned for reading (and maybe writing if pi->writePin is
            // also true) so it can't be freed now, maybe next time.
            continue;
        }

        // Free the page.
        freed += 1;
        auto pgno = pi->hdr ? pi->hdr->pgno : pi->pgno;
        assert(m_pages[pgno] == pi);
        m_pages[pgno] = nullptr;
        assert(pi->hdr);
        freePage_LK(pi->hdr);
        freeWorkInfo_LK(pi);
    }

    s_perfCleanPages -= (unsigned) freed;
    s_perfCleanToFree += (unsigned) freed;
    m_pageBonds -= freed;
    s_perfBonds -= (unsigned) freed;
}

//===========================================================================
// Remove WAL info entries that have had all their dependent pages committed.
// This is done by removing the entries whose LSNs are all older than the
// passed in threshold, which is based on the most recent LSN that has no older
// WAL records belonging to uncommitted transactions and for which all pages
// have been written.
void DbPage::removeWalPages_LK(Lsn lsn) {
    assert(lsn);
    size_t bytes = 0;

    // Remove overflow WAL infos below the threshold. Overflow infos are below
    // the threshold if the threshold is at or beyond the starting LSN of the
    // next overflow WAL info. Or, for the last overflow WAL info, if the
    // threshold is at or beyond the LSN of the first current WAL info.
    while (m_overflowWal.size() > 1 && lsn >= m_overflowWal[1].lsn
        || m_overflowWal.size() == 1 && lsn >= m_currentWal.front().lsn
    ) {
        auto & pi = m_overflowWal.front();
        if (auto val = pi.bytes)
            bytes += val;
        m_overflowWal.pop_front();
    }
    m_overflowWalBytes -= bytes;

    // If all overflow WAL infos are gone, remove all current WAL infos below
    // the threshold.
    if (m_overflowWal.empty()) {
        // Infos are under the threshold if the threshold is at or after the
        // starting LSN of the next info.
        while (m_currentWal.size() > 1 && lsn >= m_currentWal[1].lsn) {
            auto & pi = m_currentWal.front();
            if (auto val = pi.bytes)
                bytes += val;
            m_currentWal.pop_front();
        }
    }

    s_perfDurableBytes -= (unsigned) bytes;
    m_durableWalBytes -= bytes;
    s_perfRefWalPages -= (unsigned) (bytes / m_walPageSize);
}

//===========================================================================
// Write the page with checksum.
void DbPage::writePageWait(DbPageHeader * hdr) {
    assert(hdr->pgno != kFreePageMark);
    s_perfWrites += 1;
    hdr->checksum = 0;
    hdr->checksum = hash_crc32c(hdr, m_pageSize);
    fileWriteWait(nullptr, m_fdata, hdr->pgno * m_pageSize, hdr, m_pageSize);
}

//===========================================================================
// Mark page as free and add it to the pool of free pages.
void DbPage::freePage_LK(DbPageHeader * hdr) {
    hdr->pgno = kFreePageMark;
    auto wpno = m_vwork.pgno(hdr);
    m_freeWorkPages.insert(wpno);
    s_perfFreePages += 1;
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
const void * DbPage::rptr(Lsn lsn, pgno_t pgno, bool withPin) {
    unique_lock lk{m_workMut};
    assert(pgno < m_pages.size());
    auto pi = m_pages[pgno];
    if (!pi) {
        // Add reference page to track the pins. Reference pages are
        // distinguished by having .hdr set to null.
        //
        // NOTE: The tracking is only to assert correctness and is no more than
        //       a fancy assert.
        assert(withPin);
        pi = allocWorkInfo_LK();
        m_pages[pgno] = pi;
        m_referencePages.link(pi);
        pi->pgno = pgno;
    }
    if (withPin) {
        assert(!pi->readPin && !pi->writePin);
        pi->readPin = true;
        s_perfPinnedPages += 1;
    } else {
        // To be safely accessed a page must be pinned, otherwise the work
        // saver may choose to discard the page at a very inconvenient time.
        assert(pi->readPin);
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
        assert(pi->readPin && !pi->writePin);
        pi->readPin = false;
        s_perfPinnedPages -= 1;
        if (!pi->hdr) {
            // Don't keep reference only page info that is no longer pinned.
            freeWorkInfo_LK(pi);
            m_pages[pgno] = nullptr;
        }
        notify = true;
    }
    lk.unlock();
    if (notify) {
        // Pins were released, announce it in case the work saver was waiting.
        m_workCv.notify_all();
    }
}

//===========================================================================
DbPage::WorkPageInfo * DbPage::allocWorkInfo_LK() {
    auto pi = m_freeInfos.back();
    if (!pi)
        pi = new WorkPageInfo;
    static_cast<WorkPageInfoBase &>(*pi) = {};
    return pi;
}

//===========================================================================
void DbPage::freeWorkInfo_LK(WorkPageInfo * pi) {
    m_freeInfos.link(pi);
}

//===========================================================================
void * DbPage::onWalGetPtrForRedo(
    pgno_t pgno,
    Lsn lsn,
    LocalTxn localTxn
) {
    // Only used during recovery, which is inherently single threaded, so no
    // locking/pinning needed.

    if (pgno >= m_pages.size()) {
        m_vdata.growToFit(pgno);
        m_pages.resize(pgno + 1);
    }
    auto pi = m_pages[pgno];
    if (!pi || !pi->hdr) {
        // Create new dirty page from clean page.
        auto src = reinterpret_cast<const DbPageHeader *>(m_vdata.rptr(pgno));
        if (lsn <= src->lsn) {
            // Page has already incorporated the WAL record with this LSN.
            return nullptr;
        }
    } else if (lsn <= pi->hdr->lsn) {
        // Page has already incorporated the WAL record with this LSN.
        return nullptr;
    }
    pi = dirtyPage_LK(pgno, lsn);
    return pi->hdr;
}

//===========================================================================
void * DbPage::onWalGetPtrForUpdate(
    pgno_t pgno,
    Lsn lsn,
    LocalTxn localTxn
) {
    assert(lsn);
    unique_lock lk{m_workMut};
    assert(pgno < m_pages.size());
    auto pi = m_pages[pgno];
    assert(pi->readPin && !pi->writePin);
    pi = dirtyPage_LK(pgno, lsn);
    pi->writePin = true;
    return pi->hdr;
}

//===========================================================================
void DbPage::onWalUnlockPtr(pgno_t pgno) {
    unique_lock lk{m_workMut};
    assert(pgno < m_pages.size());
    auto pi = m_pages[pgno];
    assert(pi->readPin && pi->writePin);
    pi->writePin = false;
    lk.unlock();

    // Pins were released, announce it in case the work saver was waiting.
    m_workCv.notify_all();
}

//===========================================================================
DbPageHeader * DbPage::dupPage_LK(const DbPageHeader * hdr) {
    pgno_t wpno = {};
    if (m_freeWorkPages) {
        // Reuse existing free page.
        wpno = (pgno_t) m_freeWorkPages.pop_front();
        s_perfFreePages -= 1;
    } else {
        // Use new page off the end of the work file, extending it as needed.
        wpno = (pgno_t) m_workPages++;
        m_vwork.growToFit(wpno);
        s_perfPages += 1;
    }
    auto ptr = (DbPageHeader *) m_vwork.wptr(wpno);
    memcpy(ptr, hdr, m_pageSize);
    return ptr;
}

//===========================================================================
DbPage::WorkPageInfo * DbPage::dirtyPage_LK(pgno_t pgno, Lsn lsn) {
    auto pi = m_pages[pgno];
    if (!pi) {
        // Page was untracked, create page info for it.
        pi = allocWorkInfo_LK();
        m_pages[pgno] = pi;
    }
    if (!pi->hdr) {
        // Create new dirty page from free or reference page.
        auto src = reinterpret_cast<const DbPageHeader *>(m_vdata.rptr(pgno));
        pi->hdr = dupPage_LK(src);
        pi->pgno = {};
        if (!pi->firstLsn) {
            // If dirtying reference or untracked page, add page bond.
            m_pageBonds += 1;
            s_perfBonds += 1;
        }
    } else {
        if (!pi->flags.any(fDbPageDirty)) {
            // Was a clean but allocated page.
            assert(pi->hdr->pgno == pgno);
            s_perfCleanPages -= 1;
            s_perfCleanToDirty += 1;
        }
    }
    assert(pi->hdr && !pi->pgno);
    pi->hdr->pgno = pgno;
    pi->hdr->lsn = lsn;
    if (!pi->flags.any(fDbPageDirty)) {
        // Page is newly dirty.
        pi->firstTime = timeNow();
        pi->firstLsn = lsn;
        pi->flags |= fDbPageDirty;
        m_dirtyPages.link(pi);
        s_perfDirtyPages += 1;
        if (m_dirtyPages.front() == pi) {
            // There were no dirty pages, so no save is scheduled, do so now.
            queueSaveWork_LK();
        }
    }
    return pi;
}
