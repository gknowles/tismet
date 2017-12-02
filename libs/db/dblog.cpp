// Copyright Glen Knowles 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// dblog.cpp - tismet db
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace Dim;


/****************************************************************************
*
*   Tuning parameters
*
***/

constexpr auto kDirtyWriteBufferTimeout = 500ms;

const unsigned kLogWriteBuffers = 2;
static_assert(kLogWriteBuffers > 1);

const unsigned kMaxLogWrites = 1;
static_assert(kMaxLogWrites < kLogWriteBuffers);


/****************************************************************************
*
*   Declarations
*
***/

enum class DbLog::Buffer : int {
    Empty,
    PartialDirty,
    PartialWriting,
    PartialClean,
    FullWriting,
};

enum class DbLog::Checkpoint : int {
    Complete,
    WaitForTxns,
    WaitForStableCommits,
    WaitForPageFlush,
    TruncateLogs,
};

namespace {

const unsigned kLogFileSig[] = {
    0xee4b1a59,
    0x4ba38e05,
    0xc589d585,
    0xaf750c2f,
};

enum PageType {
    kPageTypeZero = 'lZ',
    kPageTypeLog = 'l',
    kPageTypeFree = 'F',
};

#pragma pack(push)
#pragma pack(1)

struct PageHeader {
    PageType type;
    uint32_t pgno;
    uint64_t firstLsn; // first lsn started on page
    uint64_t lastLsn; // last lsn started on page
    uint16_t lastLsnPos; // position on page of start of last lsn
};

struct ZeroPage {
    DbPageHeader hdr;
    char signature[sizeof(kLogFileSig)];
    unsigned pageSize;
};

#pragma pack(pop)

} // namespace


/****************************************************************************
*
*   Variables
*
***/

static auto & s_perfWrites = uperf("log writes (total)");
static auto & s_perfReorderedWrites = uperf("log writes (out of order)");
static auto & s_perfCurTxns = uperf("log transactions (current)");
static auto & s_perfOldTxns = uperf("log transactions (old)");
static auto & s_perfCps = uperf("checkpoints (total)");
static auto & s_perfCurCps = uperf("checkpoints (current)");


/****************************************************************************
*
*   DbLog
*
***/

//===========================================================================
DbLog::DbLog(DbData & data, DbPage & work)
    : m_data(data)
    , m_page(work)
{}

//===========================================================================
DbLog::~DbLog() {
    close();
}

//===========================================================================
char * DbLog::bufPtr(size_t ibuf) {
    assert(ibuf < m_numBufs);
    return m_buffers.get() + ibuf * m_pageSize;
}

//===========================================================================
bool DbLog::open(string_view logfile) {
    m_pageSize = 2 * m_page.pageSize();
    m_numBufs = kLogWriteBuffers;
    m_bufStates.resize(m_numBufs, Buffer::Empty);
    m_emptyBufs = m_numBufs;
    m_buffers.reset(new char[m_numBufs * m_pageSize]);
    memset(m_buffers.get(), 0, m_numBufs * m_pageSize);
    m_curBuf = 0;
    for (unsigned i = 0; i < m_numBufs; ++i) {
        auto * lp = (PageHeader *) bufPtr(i);
        lp->type = kPageTypeFree;
    }
    m_logPos = m_pageSize;
    m_bufPos = m_pageSize;

    m_flog = fileOpen(
        logfile,
        File::fCreat | File::fReadWrite | File::fDenyWrite
    );
    if (!m_flog)
        return false;
    auto len = fileSize(m_flog);
    ZeroPage zp{};
    if (!len) {
        zp.hdr.type = (DbPageType) kPageTypeZero;
        memcpy(zp.signature, kLogFileSig, sizeof(zp.signature));
        zp.pageSize = (unsigned) m_pageSize;
        fileWriteWait(m_flog, 0, &zp, sizeof(zp));
        s_perfWrites += 1;
        m_numPages = 1;
        m_lastLsn = 0;
        m_lastLocalTxn = 0;
        auto cpLsn = logCheckpointStart();
        logCheckpointEnd(cpLsn);
        return true;
    }

    fileReadWait(&zp, sizeof(zp), m_flog, 0);
    if (memcmp(zp.signature, kLogFileSig, sizeof(zp.signature)) != 0) {
        logMsgError() << "Bad signature in " << logfile;
        return false;
    }
    if (zp.pageSize != m_pageSize) {
        logMsgError() << "Mismatched page size in " << logfile;
        return false;
    }
    m_numPages = (len + m_pageSize - 1) / m_pageSize;
    if (!loadPages())
        return false;
    if (!recover())
        return false;
    return true;
}

//===========================================================================
void DbLog::close() {
    checkpoint();
    timerUpdate(this, kTimerInfinite);
    onTimer(Clock::now());
    m_bufMut.lock();
    for (;;) {
        if (m_phase == Checkpoint::Complete) {
            if (m_emptyBufs == m_numBufs)
                break;
            auto bst = m_bufStates[m_curBuf];
            if (m_emptyBufs == m_numBufs - 1) {
                if (bst == Buffer::PartialClean)
                    break;
                if (bst == Buffer::PartialDirty) {
                    auto * lp = (PageHeader *) bufPtr(m_curBuf);
                    lp->lastLsn = m_lastLsn;
                    lp->lastLsnPos = (uint16_t) m_logPos;
                    auto offset = lp->pgno * m_pageSize;
                    auto bytes = m_bufPos;
                    fileWriteWait(m_flog, offset, lp, bytes);
                    s_perfWrites += 1;
                    break;
                }
            }
        }
        flushWriteBuffer_UNLK();
        unique_lock<mutex> lk{m_bufMut};
        m_bufAvailCv.wait(lk);
        lk.release();
    }
    m_bufMut.unlock();
    fileClose(m_flog);
}

//===========================================================================
bool DbLog::loadPages() {
    PageHeader hdr;
    for (uint32_t i = 1; i < m_numPages; ++i) {
        fileReadWait(&hdr, sizeof(hdr), m_flog, i * m_pageSize);
        if (!hdr.type) {
            break;
        } else if (hdr.type == kPageTypeLog) {
            auto & pi = m_pages.emplace_back();
            pi.pgno = hdr.pgno;
            pi.firstLsn = hdr.firstLsn;
            pi.lastLsn = hdr.lastLsn;
        } else {
            logMsgError() << "Invalid page type(" << hdr.type << ") on page #"
                << i << " of " << filePath(m_flog);
            return false;
        }
    }
    if (m_pages.empty())
        return true;

    auto first = m_pages.begin();
    sort(
        first,
        m_pages.end(),
        [](auto & a, auto & b){ return a.firstLsn < b.firstLsn; }
    );
    auto rlast = adjacent_find(
        m_pages.rbegin(),
        m_pages.rend(),
        [](auto & a, auto & b){ return a.firstLsn - 1 != b.lastLsn; }
    );
    auto base = rlast.base();
    for_each(first, base, [&](auto & a){ m_freePages.insert(a.pgno); });
    m_pages.erase(first, base);
    return true;
}

//===========================================================================
bool DbLog::recover() {
    if (m_pages.empty())
        return true;

    m_stableLsn = m_pages.back().lastLsn;
    m_lastLsn = m_stableLsn;
    return true;
}

//===========================================================================
uint64_t DbLog::beginTxn() {
    for (;;) {
        if (++m_lastLocalTxn)
            break;
    }
    m_curTxns += 1;
    s_perfCurTxns += 1;
    return logBeginTxn(m_lastLocalTxn);
}

//===========================================================================
void DbLog::commit(uint64_t txn) {
    logCommit(txn);
    if (getLsn(txn) >= m_checkpointLsn) {
        assert(m_curTxns);
        m_curTxns -= 1;
        s_perfCurTxns -= 1;
        return;
    }

    assert(m_phase == Checkpoint::WaitForTxns);
    assert(m_oldTxns);
    m_oldCommitLsn = m_lastLsn;
    if (--m_oldTxns == 0)
        m_phase = Checkpoint::WaitForStableCommits;
    s_perfOldTxns -= 1;
}

//===========================================================================
void DbLog::checkpoint() {
    assert(m_phase == Checkpoint::Complete);
    m_checkpointLsn = logCheckpointStart();
    m_phase = Checkpoint::WaitForTxns;
    m_oldTxns = m_curTxns;
    m_curTxns = 0;
    s_perfCurTxns -= m_oldTxns;
    s_perfOldTxns += m_oldTxns;
    s_perfCps += 1;
    s_perfCurCps += 1;
    if (m_oldTxns == 0)
        m_phase = Checkpoint::WaitForStableCommits;
}

//===========================================================================
void DbLog::flushWriteBuffer_UNLK() {
    unique_lock<mutex> lk{m_bufMut, adopt_lock};
    auto * lp = (PageHeader *) bufPtr(m_curBuf);
    if (m_bufStates[m_curBuf] != Buffer::PartialDirty)
        return;

    m_bufStates[m_curBuf] = Buffer::PartialWriting;
    lp->lastLsn = m_lastLsn;
    lp->lastLsnPos = (uint16_t) m_logPos;
    auto offset = lp->pgno * m_pageSize;
    auto bytes = m_bufPos;

    auto * nlp = new char[bytes + sizeof(PageHeader *)];
    *(PageHeader **) nlp = lp;
    nlp += sizeof(PageHeader *);
    memcpy(nlp, lp, bytes);

    lk.unlock();
    fileWrite(this, m_flog, offset, nlp, bytes, taskComputeQueue());
}

//===========================================================================
Duration DbLog::onTimer(TimePoint now) {
    m_bufMut.lock();
    flushWriteBuffer_UNLK();
    return kTimerInfinite;
}

//===========================================================================
void DbLog::updateStableLsn_LK(uint64_t first, uint64_t last) {
    if (first != m_stableLsn + 1) {
        s_perfReorderedWrites += 1;
        auto lsns = make_pair(first, last);
        auto i = lower_bound(
            m_stableLsns.begin(),
            m_stableLsns.end(),
            lsns
        );
        m_stableLsns.insert(i, lsns);
        return;
    }

    m_stableLsn = last;
    if (!m_stableLsns.empty()) {
        auto i = m_stableLsns.begin();
        for (; i != m_stableLsns.end(); ++i) {
            if (i->first != m_stableLsn + 1)
                break;
            m_stableLsn = i->second;
        }
        m_stableLsns.erase(m_stableLsns.begin(), i);
    }

    if (m_phase == Checkpoint::WaitForStableCommits
        && m_stableLsn >= m_oldCommitLsn
    ) {
        m_phase = Checkpoint::WaitForPageFlush;
        taskPushCompute(*this);
    }
}

//===========================================================================
void DbLog::onTask() {
    m_page.flush();
    logCheckpointEnd(m_checkpointLsn);
    m_phase = Checkpoint::Complete;
    m_bufAvailCv.notify_one();
}

//===========================================================================
void DbLog::onFileWrite(
    int written,
    string_view data,
    int64_t offset,
    FileHandle f
) {
    s_perfWrites += 1;
    auto * lp = (PageHeader *) data.data();
    unique_lock<mutex> lk{m_bufMut};
    updateStableLsn_LK(lp->firstLsn, lp->lastLsn);
    if (data.size() == m_pageSize) {
        m_emptyBufs += 1;
        auto ibuf = (data.data() - m_buffers.get()) / m_pageSize;
        m_bufStates[ibuf] = Buffer::Empty;
        lp->type = kPageTypeFree;
        lk.unlock();
        m_bufAvailCv.notify_one();
        return;
    }

    // it's a partial from onTimer
    auto buf = data.data() - sizeof(PageHeader *);
    auto * olp = *(PageHeader **) buf;
    auto ibuf = ((char *) olp - m_buffers.get()) / m_pageSize;
    if (m_bufStates[ibuf] == Buffer::PartialWriting) {
        if (olp->lastLsn == lp->lastLsn) {
            m_bufStates[ibuf] = Buffer::PartialClean;
            m_bufAvailCv.notify_one();
        } else {
            m_bufStates[ibuf] = Buffer::PartialDirty;
            timerUpdate(this, kDirtyWriteBufferTimeout);
        }
    } else if (m_bufStates[ibuf] == Buffer::FullWriting) {
        fileWrite(this, m_flog, offset, olp, m_pageSize, taskComputeQueue());
    }
    delete[] buf;
}

//===========================================================================
void DbLog::prepareBuffer_LK(
    const void * log,
    size_t bytesOnOldPage,
    size_t bytesOnNewPage
) {
    assert(m_emptyBufs);
    for (;;) {
        if (++m_curBuf == m_numBufs)
            m_curBuf = 0;
        if (m_bufStates[m_curBuf] == Buffer::Empty)
            break;
    }

    auto * lp = (PageHeader *) bufPtr(m_curBuf);
    lp->type = kPageTypeLog;
    if (auto i = m_freePages.find(0); i) {
        lp->pgno = *i;
        m_freePages.erase(i);
    } else {
        lp->pgno = (uint32_t) m_numPages++;
    }
    lp->firstLsn = m_lastLsn + (bool) bytesOnOldPage;
    lp->lastLsn = 0;
    lp->lastLsnPos = 0;

    m_bufStates[m_curBuf] = Buffer::PartialDirty;
    m_emptyBufs -= 1;
    memcpy(
        (char *) lp + sizeof(PageHeader),
        (const char *) log + bytesOnOldPage,
        bytesOnNewPage
    );
    m_logPos = (uint16_t) -(int)bytesOnOldPage;
    m_bufPos = sizeof(PageHeader) + bytesOnNewPage;

    timerUpdate(this, kDirtyWriteBufferTimeout);
}

//===========================================================================
void DbLog::log(Record * log, size_t bytes, bool setLsn) {
    assert(bytes < m_pageSize - sizeof(PageHeader));
    assert(bytes == size(log));

    unique_lock<mutex> lk{m_bufMut};
    while (m_bufPos + bytes > m_pageSize && !m_emptyBufs)
        m_bufAvailCv.wait(lk);
    m_lastLsn += 1;
    if (setLsn)
        setLogPos(log, m_lastLsn, getLocalTxn(log));

    if (m_bufPos == m_pageSize)
        return prepareBuffer_LK(log, 0, bytes);

    size_t overflow = 0;
    if (auto avail = m_pageSize - m_bufPos; bytes > avail) {
        overflow = bytes - avail;
        bytes = avail;
    }
    auto base = bufPtr(m_curBuf) + m_bufPos;
    memcpy(base, log, bytes);
    m_logPos = m_bufPos;
    m_bufPos += bytes;

    if (m_bufPos != m_pageSize) {
        if (m_bufStates[m_curBuf] == Buffer::PartialClean) {
            m_bufStates[m_curBuf] = Buffer::PartialDirty;
            timerUpdate(this, kDirtyWriteBufferTimeout);
        }
    } else {
        bool writeInProgress = m_bufStates[m_curBuf] == Buffer::PartialWriting;
        m_bufStates[m_curBuf] = Buffer::FullWriting;
        auto * lp = (PageHeader *) bufPtr(m_curBuf);
        lp->lastLsn = m_lastLsn;
        lp->lastLsnPos = (uint16_t) m_logPos;
        auto offset = lp->pgno * m_pageSize;

        if (overflow)
            prepareBuffer_LK(log, bytes, overflow);

        lk.unlock();
        if (!writeInProgress)
            fileWrite(this, m_flog, offset, lp, m_pageSize, taskComputeQueue());
    }
}

//===========================================================================
void DbLog::applyRedo(const Record * log) {
    auto pgno = getPgno(log);
    auto lsn = getLsn(log);
    auto ptr = m_page.wptr(lsn, pgno);
    applyRedo(ptr, log);
}

//===========================================================================
void DbLog::applyCheckpointStart(uint64_t lsn) {
}

//===========================================================================
void DbLog::applyCheckpointEnd(uint64_t lsn, uint64_t startLsn) {
}

//===========================================================================
void DbLog::applyBeginTxn(uint16_t txn) {
}

//===========================================================================
void DbLog::applyCommit(uint16_t txn) {
}


/****************************************************************************
*
*   DbTxn
*
***/

//===========================================================================
DbTxn::DbTxn(DbLog & log, DbPage & work)
    : m_log{log}
    , m_page{work}
{}

//===========================================================================
DbTxn::~DbTxn() {
    if (m_txn)
        m_log.commit(m_txn);
}
