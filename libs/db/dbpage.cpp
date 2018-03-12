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

const unsigned kDefaultPageSize = 4096;
static_assert(kDefaultPageSize == pow2Ceil(kDefaultPageSize));

// Must be a multiple of fileViewAlignment()
const size_t kViewSize = 0x100'0000; // 16MiB
const size_t kDefaultFirstViewSize = 2 * kViewSize;

const Duration kDefaultPageMaxAge = 30min;
const Duration kDefaultPageScanInterval = 1min;

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
static auto & s_perfPurges = uperf("db.work dirty scans (total)");
static auto & s_perfCurPurges = uperf("db.work dirty scans (current)");


/****************************************************************************
*
*   DbPage
*
***/

//===========================================================================
DbPage::DbPage()
    : m_pageMaxAge{kDefaultPageMaxAge}
    , m_pageScanInterval{kDefaultPageScanInterval}
    , m_flushTask([&]{ flushStalePages(); })
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
    assert(pageSize == pow2Ceil(pageSize));
    if (!pageSize)
        pageSize = kDefaultPageSize;
    assert(pageSize >= kMinPageSize);
    assert(kViewSize % fileViewAlignment() == 0);

    m_flags = flags;
    if (m_flags & fDbOpenVerbose)
        logMsgInfo() << "Open data files";
    if (!openData(datafile, pageSize))
        return false;
    if (!openWork(workfile)) {
        close();
        return false;
    }

    return true;
}

//===========================================================================
bool DbPage::openData(string_view datafile, size_t pageSize) {
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
        m_pageSize = pageSize;
        m_newFiles = true;
    } else {
        m_pageSize = DbData::queryPageSize(m_fdata);
        if (!m_pageSize)
            m_pageSize = pageSize;
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

    m_stableLsns.clear();
    m_stableLsns.push_back(0);
    return true;
}

//===========================================================================
void DbPage::configure(const DbConfig & conf) {
    auto maxAge = conf.pageMaxAge.count() ? conf.pageMaxAge : m_pageMaxAge;
    auto scanInterval = conf.pageScanInterval.count()
        ? conf.pageScanInterval
        : m_pageScanInterval;
    if (maxAge < 1min) {
        logMsgError() << "Max work page age must be at least 1 minute.";
        return;
    }
    if (maxAge < scanInterval) {
        logMsgError() << "Max work page age must be greater than scan "
            "interval.";
        return;
    }
    auto count = maxAge / scanInterval;
    if (count > 1000) {
        logMsgError() << "Work page scan interval must be at least 1/1000th "
            "of the max age.";
        return;
    }
    m_pageMaxAge = maxAge;
    m_pageScanInterval = scanInterval;
    queuePageScan();
}

//===========================================================================
void DbPage::close() {
    m_stableLsns.clear();
    m_pages.clear();
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
    unique_lock<mutex> lk{m_workMut};
    if (pgno < m_pages.size())
        return;
    assert(pgno == m_pages.size());
    m_vdata.growToFit(pgno);
    m_pages.resize(pgno + 1);
}

//===========================================================================
void DbPage::queuePageScan() {
    auto now = Clock::now().time_since_epoch();
    auto next = (now / m_pageScanInterval + 1) * m_pageScanInterval;
    timerUpdate(this, next - now, true);
}

//===========================================================================
Duration DbPage::onTimer(TimePoint now) {
    size_t count = m_pageMaxAge / m_pageScanInterval;

    {
        unique_lock<mutex> lk{m_workMut};
        if (m_flushLsn)
            return kTimerInfinite;
        m_stableLsns.push_back(0);
        while (m_stableLsns.size() > count) {
            m_flushLsn = m_stableLsns.front();
            m_stableLsns.pop_front();
        }
    }

    if (m_flushLsn) {
        taskPushCompute(&m_flushTask);
    } else {
        queuePageScan();
    }
    return kTimerInfinite;
}

//===========================================================================
bool DbPage::enablePageScan(bool enable) {
    swap(m_pageScanEnabled, enable);
    return enable;
}

//===========================================================================
void DbPage::flushStalePages() {
    s_perfPurges += 1;
    s_perfCurPurges += 1;
    if (m_flags & fDbOpenVerbose)
        logMsgInfo() << "Dirty page scan started";
    uint32_t wpno = 1;
    auto buf = make_unique<char[]>(m_pageSize);
    auto tmpHdr = reinterpret_cast<DbPageHeader *>(buf.get());
    m_workMut.lock();
    for (;; ++wpno) {
        if (auto i = m_freeWorkPages.find(wpno); i) {
            i = m_freeWorkPages.lastContiguous(i);
            wpno = *i + 1;
        }
        bool done = (wpno >= m_workPages);
        m_workMut.unlock();

        if (!m_pageScanEnabled)
            break;
        if (done) {
            queuePageScan();
            break;
        }
        auto hdr = reinterpret_cast<DbPageHeader *>(m_vwork.wptr(wpno));

        m_workMut.lock();
        if (m_flushLsn < hdr->lsn || hdr->pgno == kFreePageMark) {
            // stay locked
            continue;
        }
        if (hdr->flags & fDbPageDirty) {
            memcpy(tmpHdr, hdr, m_pageSize);
            m_workMut.unlock();
            writePageWait(tmpHdr);
            m_workMut.lock();
            if (tmpHdr->lsn != hdr->lsn) {
                // the page changed, so don't mark it free
                // stay locked
                continue;
            }
        }

        if (m_pages[hdr->pgno] == hdr)
            m_pages[hdr->pgno] = nullptr;
        if (hdr->flags & fDbPageDirty) {
            hdr->flags &= ~fDbPageDirty;
            s_perfDirtyPages -= 1;
        }
        hdr->pgno = kFreePageMark;
        m_freeWorkPages.insert(wpno);
        s_perfFreePages += 1;
    }

    m_flushLsn = 0;
    s_perfCurPurges -= 1;
    if (m_flags & fDbOpenVerbose)
        logMsgInfo() << "Dirty page scan completed";
}

//===========================================================================
void DbPage::onLogStable(uint64_t lsn) {
    unique_lock<mutex> lk{m_workMut};
    m_stableLsns.back() = lsn;
    m_stableLsn = lsn;
}

//===========================================================================
void DbPage::onLogCheckpointPages() {
    assert(m_oldPages.empty());
    uint32_t wpno = 1;
    auto buf = make_unique<char[]>(m_pageSize);
    auto tmpHdr = reinterpret_cast<DbPageHeader *>(buf.get());
    m_workMut.lock();
    for (;; ++wpno) {
        if (auto i = m_freeWorkPages.find(wpno); i) {
            i = m_freeWorkPages.lastContiguous(i);
            wpno = *i + 1;
        }
        if (wpno >= m_workPages)
            break;

        m_workMut.unlock();
        auto hdr = reinterpret_cast<DbPageHeader *>(m_vwork.wptr(wpno));
        m_workMut.lock();

        if ((~hdr->flags & fDbPageDirty) || hdr->pgno == kFreePageMark) {
            // stay locked
            continue;
        }
        hdr->flags &= ~fDbPageDirty;
        s_perfDirtyPages -= 1;

        if (hdr->lsn > m_stableLsn) {
            hdr = dupPage_LK(hdr);
            m_oldPages[hdr->pgno] = hdr;
            // stay locked
            continue;
        }

        memcpy(tmpHdr, hdr, m_pageSize);

        m_workMut.unlock();
        writePageWait(tmpHdr);
        m_workMut.lock();
    }
    m_workMut.unlock();
}

//===========================================================================
void DbPage::onLogCheckpointStablePages() {
    for (auto && pgno_hdr : m_oldPages) {
        writePageWait(pgno_hdr.second);
        pgno_hdr.second->pgno = kFreePageMark;
    }

    unique_lock<mutex> lk{m_workMut};
    for (auto && pgno_hdr : m_oldPages) {
        assert(m_pages[pgno_hdr.first] != pgno_hdr.second);
        auto wpno = m_vwork.pgno(pgno_hdr.second);
        m_freeWorkPages.insert(wpno);
        s_perfFreePages += 1;
    }
    m_oldPages.clear();
}

//===========================================================================
void DbPage::writePageWait(DbPageHeader * hdr) {
    assert(hdr->pgno != (uint32_t) -1);
    // TODO: update checksum
    hdr->flags = 0;
    fileWriteWait(m_fdata, hdr->pgno * m_pageSize, hdr, m_pageSize);
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
    if (~hdr->flags & fDbPageDirty) {
        hdr->flags |= fDbPageDirty;
        s_perfDirtyPages += 1;
    }
    hdr->pgno = pgno;
    hdr->lsn = lsn;
    return hdr;
}

//===========================================================================
void * DbPage::onLogGetUpdatePtr(
    uint32_t pgno,
    uint64_t lsn,
    uint16_t localTxn
) {
    assert(lsn);
    unique_lock<mutex> lk{m_workMut};
    assert(pgno < m_pages.size());
    auto hdr = m_pages[pgno];
    if (!hdr) {
        // create new dirty page from clean page
        auto src = reinterpret_cast<const DbPageHeader *>(m_vdata.rptr(pgno));
        hdr = dupPage_LK(src);
        m_pages[pgno] = hdr;
    }
    if (~hdr->flags & fDbPageDirty) {
        hdr->flags |= fDbPageDirty;
        s_perfDirtyPages += 1;
    }
    hdr->pgno = pgno;
    hdr->lsn = lsn;
    return hdr;
}

//===========================================================================
const void * DbPage::rptr(uint64_t lsn, uint32_t pgno) const {
    unique_lock<mutex> lk{m_workMut};
    assert(pgno < m_pages.size());
    if (auto hdr = m_pages[pgno])
        return hdr;

    return m_vdata.rptr(pgno);
}
