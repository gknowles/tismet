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

const unsigned kDefaultMaxCheckpointData = 1'048'576; // 1 MiB
const Duration kDefaultMaxCheckpointInterval = 1h;


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
    WaitForTxnCommits,
    WaitForPageFlush,
    WaitForCheckpointCommit,
};

struct DbLog::AnalyzeData {
    bool analyze{true};
    unordered_map<uint16_t, uint64_t> txns;
    vector<uint64_t> incompleteTxnLsns;
    uint64_t stableCheckpoint{0};
    uint64_t volatileCheckpoint{0};

    UnsignedSet activeTxns;
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
    uint64_t firstLsn; // LSN of first record started on page
    uint16_t numLogs; // number of log records started on page
    uint16_t firstPos; // position of first log started on page
    uint16_t lastPos; // position after last log record ended on page
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
    , m_maxCheckpointData{kDefaultMaxCheckpointData}
    , m_maxCheckpointInterval{kDefaultMaxCheckpointInterval}
    , m_checkpointTimer([&](auto){ checkpoint(); return kTimerInfinite; })
    , m_checkpointPagesTask([&]{ checkpointPages(); })
    , m_flushTimer([&](auto){ flushWriteBuffer(); return kTimerInfinite; })
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
        auto cpLsn = logBeginCheckpoint();
        logCommitCheckpoint(cpLsn);
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
    if (!recover())
        return false;
    return true;
}

//===========================================================================
void DbLog::close() {
    m_closing = true;
    m_page.enablePageScan(false);
    checkpoint();
    flushWriteBuffer();
    unique_lock<mutex> lk{m_bufMut};
    for (;;) {
        if (m_phase == Checkpoint::Complete) {
            if (m_emptyBufs == m_numBufs)
                break;
            auto bst = m_bufStates[m_curBuf];
            if (m_emptyBufs == m_numBufs - 1 && bst == Buffer::PartialClean)
                break;
        }
        m_bufAvailCv.wait(lk);
    }
    lk.unlock();
    fileClose(m_flog);
}

//===========================================================================
void DbLog::configure(const DbConfig & conf) {
    auto maxData = conf.checkpointMaxData
        ? conf.checkpointMaxData
        : m_maxCheckpointData;
    auto maxInterval = conf.checkpointMaxInterval.count()
        ? conf.checkpointMaxInterval
        : m_maxCheckpointInterval;
    if (maxData < m_pageSize) {
        logMsgError() << "Max data before checkpoint must be at least "
            "page size (" << m_pageSize << ")";
        maxData = m_pageSize;
    }
    maxInterval = ceil<chrono::minutes>(maxInterval);

    m_maxCheckpointData = maxData;
    m_maxCheckpointInterval = maxInterval;
    timerUpdate(&m_checkpointTimer, maxInterval, true);
}

//===========================================================================
// Creates array of references to last page and its contiguous predecessors
bool DbLog::loadPages() {
    PageHeader hdr;
    // load info for each page
    for (uint32_t i = 1; i < m_numPages; ++i) {
        fileReadWait(&hdr, sizeof(hdr), m_flog, i * m_pageSize);
        if (!hdr.type) {
            break;
        } else if (hdr.type == kPageTypeLog) {
            auto & pi = m_pages.emplace_back();
            pi.pgno = hdr.pgno;
            pi.firstLsn = hdr.firstLsn;
            pi.numLogs = hdr.numLogs;
        } else {
            logMsgError() << "Invalid page type(" << hdr.type << ") on page #"
                << i << " of " << filePath(m_flog);
            return false;
        }
    }
    if (m_pages.empty())
        return true;

    // Sort and remove all pages that are not contiguously connected with the
    // last page.
    auto first = m_pages.begin();
    sort(first, m_pages.end());
    auto rlast = adjacent_find(
        m_pages.rbegin(),
        m_pages.rend(),
        [](auto & a, auto & b){ return a.firstLsn != b.firstLsn + b.numLogs; }
    );
    auto base = rlast.base();
    for_each(first, base, [&](auto & a){ m_freePages.insert(a.pgno); });
    m_pages.erase(first, base);
    return true;
}

//===========================================================================
void DbLog::applyAll(AnalyzeData & data) {
    auto buf = make_unique<char[]>(2 * m_pageSize);
    auto buf2 = make_unique<char[]>(2 * m_pageSize);
    int bytesBefore{0};
    int logPos{0};
    auto lsn = uint64_t{0};
    auto log = (Record *) nullptr;

    for (auto & pi : m_pages) {
        fileReadWait(buf2.get(), m_pageSize, m_flog, pi.pgno * m_pageSize);
        auto hdr = (PageHeader *) buf2.get();
        if (bytesBefore) {
            auto bytesAfter = hdr->firstPos - sizeof(*hdr);
            memcpy(
                buf.get() + m_pageSize,
                buf2.get() + sizeof(*hdr),
                bytesAfter
            );
            log = (Record *) (buf.get() + m_pageSize - bytesBefore);
            assert(size(log) == bytesBefore + bytesAfter);
            apply(hdr->firstLsn - 1, log, &data);
        }
        swap(buf, buf2);

        logPos = hdr->firstPos;
        lsn = hdr->firstLsn;
        while (logPos < hdr->lastPos) {
            log = (Record *) (buf.get() + logPos);
            apply(lsn, log, &data);
            logPos += size(log);
            lsn += 1;
        }
        assert(logPos == hdr->lastPos);
        bytesBefore = (int) (m_pageSize - logPos);
    }

    // Initialize log write buffers with last buffer (if partial) found
    // during analyze.
    if (data.analyze && logPos < m_pageSize) {
        memcpy(m_buffers.get(), buf.get(), logPos);
        m_bufPos = logPos;
        m_bufStates[m_curBuf] = Buffer::PartialClean;
        m_emptyBufs -= 1;
    }
}

//===========================================================================
bool DbLog::recover() {
    if (!loadPages())
        return false;
    if (m_pages.empty())
        return true;

    // Go through log entries looking for last committed checkpoint and the
    // set of incomplete transactions (so we can avoid trying to redo them
    // later).
    AnalyzeData data;
    applyAll(data);
    if (!data.stableCheckpoint)
        logMsgCrash() << "Invalid tsl file, no checkpoint found";

    for (auto && kv : data.txns) {
        data.incompleteTxnLsns.push_back(kv.second);
    }
    sort(
        data.incompleteTxnLsns.begin(),
        data.incompleteTxnLsns.end(),
        [](auto & a, auto & b) { return a > b; }
    );

    // Go through log entries starting with the last committed checkpoint and
    // redo all complete transactions found.
    data.analyze = false;
    applyAll(data);
    assert(data.incompleteTxnLsns.empty());
    assert(data.activeTxns.empty());

    auto & back = m_pages.back();
    m_stableLsn = back.firstLsn + back.numLogs - 1;
    m_lastLsn = m_stableLsn;
    return true;
}

//===========================================================================
void DbLog::applyBeginCheckpoint(AnalyzeData & data, uint64_t lsn) {
    if (data.analyze)
        data.volatileCheckpoint = lsn;
}

//===========================================================================
void DbLog::applyCommitCheckpoint(
    AnalyzeData & data,
    uint64_t lsn,
    uint64_t startLsn
) {
    if (data.analyze && startLsn == data.volatileCheckpoint)
        data.stableCheckpoint = startLsn;
}

//===========================================================================
void DbLog::applyBeginTxn(
    AnalyzeData & data,
    uint64_t lsn,
    uint16_t localTxn
) {
    if (data.analyze) {
        auto & txnLsn = data.txns[localTxn];
        if (txnLsn)
            data.incompleteTxnLsns.push_back(txnLsn);
        txnLsn = lsn;
        return;
    }

    // redo
    if (lsn < data.stableCheckpoint)
        return;
    if (!data.incompleteTxnLsns.empty()
        && lsn == data.incompleteTxnLsns.back()
    ) {
        data.incompleteTxnLsns.pop_back();
        return;
    }
    data.activeTxns.insert(localTxn);
}

//===========================================================================
void DbLog::applyCommit(AnalyzeData & data, uint64_t lsn, uint16_t localTxn) {
    if (data.analyze) {
        data.txns.erase(localTxn);
    } else {
        data.activeTxns.erase(localTxn);
    }
}

//===========================================================================
void DbLog::applyRedo(AnalyzeData & data, uint64_t lsn, const Record * log) {
    if (data.analyze)
        return;

    if (!data.activeTxns.count(getLocalTxn(log)))
        return;

    auto pgno = getPgno(log);
    if (auto ptr = m_page.wptrRedo(lsn, pgno))
        applyUpdate(ptr, log);
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
        m_phase = Checkpoint::WaitForTxnCommits;
    s_perfOldTxns -= 1;
}

//===========================================================================
void DbLog::checkpoint() {
    if (m_phase != Checkpoint::Complete)
        return;
    m_checkpointStart = Clock::now();
    m_checkpointLsn = logBeginCheckpoint();
    m_page.checkpoint(m_checkpointLsn);
    m_checkpointData = 0;
    m_phase = Checkpoint::WaitForTxns;
    m_oldTxns = m_curTxns;
    m_curTxns = 0;
    s_perfCurTxns -= m_oldTxns;
    s_perfOldTxns += m_oldTxns;
    s_perfCps += 1;
    s_perfCurCps += 1;
    if (m_oldTxns == 0)
        m_phase = Checkpoint::WaitForTxnCommits;
}

//===========================================================================
void DbLog::flushWriteBuffer() {
    unique_lock<mutex> lk{m_bufMut};
    auto * lp = (PageHeader *) bufPtr(m_curBuf);
    if (m_bufStates[m_curBuf] != Buffer::PartialDirty)
        return;

    m_bufStates[m_curBuf] = Buffer::PartialWriting;
    lp->numLogs = (uint16_t) (m_lastLsn - lp->firstLsn + 1);
    lp->lastPos = (uint16_t) m_bufPos;
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
void DbLog::updatePages_LK(const PageInfo & pi) {
    auto i = lower_bound(m_pages.begin(), m_pages.end(), pi);
    if (i != m_pages.end() && i->firstLsn == pi.firstLsn) {
        *i = pi;
    } else {
        i = m_pages.insert(i, pi);
    }

    auto last = i->firstLsn + i->numLogs - 1;
    while (i->firstLsn <= m_stableLsn + 1
        && last >= m_stableLsn + 1
    ) {
        m_stableLsn = last;
        if (++i == m_pages.end())
            break;
        last = i->firstLsn + i->numLogs - 1;
    }

    m_page.stable(m_stableLsn);
    if (m_stableLsn >= m_oldCommitLsn) {
        if (m_phase == Checkpoint::WaitForTxnCommits) {
            m_phase = Checkpoint::WaitForPageFlush;
            taskPushCompute(m_checkpointPagesTask);
        } else if (m_phase == Checkpoint::WaitForCheckpointCommit) {
            truncateLogs_LK();
            completeCheckpoint_LK();
        }
    }
}

//===========================================================================
void DbLog::checkpointPages() {
    assert(m_phase == Checkpoint::WaitForPageFlush);
    m_page.checkpointPages();
    logCommitCheckpoint(m_checkpointLsn);
    m_oldCommitLsn = m_lastLsn;
    m_phase = Checkpoint::WaitForCheckpointCommit;
    flushWriteBuffer();
}

//===========================================================================
void DbLog::completeCheckpoint_LK() {
    m_phase = Checkpoint::Complete;
    if (!m_closing) {
        Duration wait = 0ms;
        auto elapsed = Clock::now() - m_checkpointStart;
        if (elapsed < m_maxCheckpointInterval)
            wait = m_maxCheckpointInterval - elapsed;
        if (m_checkpointData >= m_maxCheckpointData)
            wait = 0ms;
        timerUpdate(&m_checkpointTimer, wait);
    }
    m_bufAvailCv.notify_one();
}

//===========================================================================
void DbLog::truncateLogs_LK() {
    auto lastTxn = m_pages.back().firstLsn;
    for (;;) {
        auto && pi = m_pages.front();
        if (pi.firstLsn >= lastTxn)
            break;
        if (pi.firstLsn + pi.numLogs > m_checkpointLsn)
            break;
        m_freePages.insert(pi.pgno);
        m_pages.pop_front();
    }
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
    PageInfo pi = { lp->pgno, lp->firstLsn, lp->numLogs };
    unique_lock<mutex> lk{m_bufMut};
    updatePages_LK(pi);
    if (data.size() == m_pageSize) {
        m_emptyBufs += 1;
        auto ibuf = (data.data() - m_buffers.get()) / m_pageSize;
        m_bufStates[ibuf] = Buffer::Empty;
        lp->type = kPageTypeFree;
        m_checkpointData += m_pageSize;
        bool needCheckpoint = m_checkpointData >= m_maxCheckpointData;
        lk.unlock();
        m_bufAvailCv.notify_one();
        if (needCheckpoint)
            timerUpdate(&m_checkpointTimer, 0ms);
        return;
    }

    // it's a partial
    auto buf = data.data() - sizeof(PageHeader *);
    auto * olp = *(PageHeader **) buf;
    auto ibuf = ((char *) olp - m_buffers.get()) / m_pageSize;
    if (m_bufStates[ibuf] == Buffer::PartialWriting) {
        if (olp->numLogs == lp->numLogs) {
            m_bufStates[ibuf] = Buffer::PartialClean;
            m_bufAvailCv.notify_one();
        } else {
            m_bufStates[ibuf] = Buffer::PartialDirty;
            timerUpdate(&m_flushTimer, kDirtyWriteBufferTimeout);
        }
    } else if (m_bufStates[ibuf] == Buffer::FullWriting) {
        fileWrite(this, m_flog, offset, olp, m_pageSize, taskComputeQueue());
    }
    delete[] buf;
}

//===========================================================================
void DbLog::prepareBuffer_LK(
    const Record * log,
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
    if (bytesOnOldPage) {
        lp->firstLsn = m_lastLsn + 1;
        lp->firstPos = (uint16_t) (sizeof(PageHeader) + bytesOnNewPage);
    } else {
        lp->firstLsn = m_lastLsn;
        lp->firstPos = (uint16_t) sizeof(PageHeader);
    }
    lp->numLogs = 0;
    lp->lastPos = 0;

    m_bufStates[m_curBuf] = Buffer::PartialDirty;
    m_emptyBufs -= 1;
    memcpy(
        (char *) lp + sizeof(PageHeader),
        (const char *) log + bytesOnOldPage,
        bytesOnNewPage
    );
    m_bufPos = sizeof(PageHeader) + bytesOnNewPage;

    timerUpdate(&m_flushTimer, kDirtyWriteBufferTimeout);
}

//===========================================================================
uint64_t DbLog::log(Record * log, size_t bytes) {
    assert(bytes < m_pageSize - sizeof(PageHeader));
    assert(bytes == size(log));

    unique_lock<mutex> lk{m_bufMut};
    while (m_bufPos + bytes > m_pageSize && !m_emptyBufs)
        m_bufAvailCv.wait(lk);
    auto lsn = ++m_lastLsn;

    if (m_bufPos == m_pageSize) {
        prepareBuffer_LK(log, 0, bytes);
        return lsn;
    }

    size_t overflow = 0;
    if (auto avail = m_pageSize - m_bufPos; bytes > avail) {
        overflow = bytes - avail;
        bytes = avail;
    }
    auto base = bufPtr(m_curBuf) + m_bufPos;
    memcpy(base, log, bytes);
    m_bufPos += bytes;

    if (m_bufPos != m_pageSize) {
        if (m_bufStates[m_curBuf] == Buffer::PartialClean
            || m_bufStates[m_curBuf] == Buffer::Empty
        ) {
            m_bufStates[m_curBuf] = Buffer::PartialDirty;
            timerUpdate(&m_flushTimer, kDirtyWriteBufferTimeout);
        }
    } else {
        bool writeInProgress = m_bufStates[m_curBuf] == Buffer::PartialWriting;
        m_bufStates[m_curBuf] = Buffer::FullWriting;
        auto * lp = (PageHeader *) bufPtr(m_curBuf);
        lp->numLogs = (uint16_t) (m_lastLsn - lp->firstLsn + 1);
        lp->lastPos = (uint16_t) m_bufPos;
        auto offset = lp->pgno * m_pageSize;

        if (overflow) {
            lp->lastPos -= (uint16_t) bytes;
            prepareBuffer_LK(log, bytes, overflow);
        }

        lk.unlock();
        if (!writeInProgress)
            fileWrite(this, m_flog, offset, lp, m_pageSize, taskComputeQueue());
    }
    return lsn;
}

//===========================================================================
void DbLog::applyUpdate(uint64_t lsn, const Record * log) {
    auto pgno = getPgno(log);
    auto ptr = (void *) nullptr;
    if (interleaveSafe(log)) {
        void * newPage = nullptr;
        ptr = m_page.wptr(lsn, pgno, &newPage);
        if (newPage)
            applyUpdate(newPage, log);
    } else {
        ptr = m_page.wptr(lsn, pgno, nullptr);
    }
    applyUpdate(ptr, log);
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
