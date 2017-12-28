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

const unsigned kLogWriteBuffers = 3;
static_assert(kLogWriteBuffers > 1);

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
    WaitForPageFlush,
    WaitForTxnCommits,
    WaitForStablePageFlush,
    WaitForCheckpointCommit,
    WaitForTruncateCommit,
};

struct DbLog::AnalyzeData {
    bool analyze{true};
    unordered_map<uint16_t, uint64_t> txns;
    vector<uint64_t> incompleteTxnLsns;
    uint64_t checkpoint{0};

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

static auto & s_perfCps = uperf("db checkpoints (total)");
static auto & s_perfCurCps = uperf("db checkpoints (current)");
static auto & s_perfCurTxns = uperf("db transactions (current)");
static auto & s_perfVolatileTxns = uperf("db transactions (volatile)");
static auto & s_perfPages = uperf("db wal pages (total)");
static auto & s_perfFreePages = uperf("db wal pages (free)");
static auto & s_perfWrites = uperf("db wal writes (total)");
static auto & s_perfReorderedWrites = uperf("db wal writes (out of order)");


/****************************************************************************
*
*   DbLog::TaskInfo
*
***/

//===========================================================================
bool DbLog::TaskInfo::operator<(const TaskInfo & right) const {
    return waitTxn < right.waitTxn;
}

//===========================================================================
bool DbLog::TaskInfo::operator>(const TaskInfo & right) const {
    return waitTxn > right.waitTxn;
}


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
    , m_checkpointStablePagesTask([&]{ checkpointStablePages(); })
    , m_checkpointStableCommitTask([&]{ checkpointStableCommit(); })
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
        auto lp = (PageHeader *) bufPtr(i);
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
        s_perfPages += (unsigned) m_numPages;
        m_lastLsn = 0;
        m_lastLocalTxn = 0;
        logCommitCheckpoint(m_lastLsn + 1);
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
    s_perfPages += (unsigned) m_numPages;
    if (!recover())
        return false;
    return true;
}

//===========================================================================
void DbLog::close() {
    m_closing = true;
    if (m_numBufs) {
        checkpoint();
        flushWriteBuffer();
    }
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
    s_perfPages -= (unsigned) m_numPages;
    s_perfFreePages -= (unsigned) m_freePages.size();
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
        } else if (hdr.type == kPageTypeFree) {
            m_freePages.insert(hdr.pgno);
            s_perfFreePages += 1;
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
    if (rlast != m_pages.rend()) {
        auto base = rlast.base() - 1;
        for_each(first, base, [&](auto & a){ m_freePages.insert(a.pgno); });
        s_perfFreePages += unsigned(base - first);
        m_pages.erase(first, base);
    }
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
        auto & pi = m_pages.back();
        auto lp = (PageHeader *) bufPtr(m_curBuf);
        assert(lp->firstLsn == pi.firstLsn);
        pi.commitTxns.emplace_back(lp->firstLsn, 0);
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
    m_checkpointLsn = m_pages.front().firstLsn;
    AnalyzeData data;
    applyAll(data);
    if (!data.checkpoint)
        logMsgCrash() << "Invalid .tsl file, no checkpoint found";
    m_checkpointLsn = data.checkpoint;

    auto i = lower_bound(
        data.incompleteTxnLsns.begin(),
        data.incompleteTxnLsns.end(),
        data.checkpoint
    );
    data.incompleteTxnLsns.erase(data.incompleteTxnLsns.begin(), i);
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
    m_stableTxn = back.firstLsn + back.numLogs - 1;
    m_lastLsn = m_stableTxn;
    m_page.stable(m_stableTxn);
    return true;
}

//===========================================================================
void DbLog::applyCommitCheckpoint(
    AnalyzeData & data,
    uint64_t lsn,
    uint64_t startLsn
) {
    if (data.analyze && startLsn >= m_checkpointLsn)
        data.checkpoint = startLsn;
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
    if (lsn < data.checkpoint)
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

    auto localTxn = getLocalTxn(log);
    if (localTxn && !data.activeTxns.count(localTxn))
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
    s_perfCurTxns += 1;
    s_perfVolatileTxns += 1;

    // Count the begin transaction on the page were its log record started.
    // This means the current page before logging (since logging can advance
    // to the next page), UNLESS it's exactly at the end of the page. In that
    // case the transaction actually starts on the next page, which is where
    // we'll be after logging.
    uint64_t txn = 0;
    if (m_bufPos < m_pageSize) {
        m_pages.back().beginTxns += 1;
        txn = logBeginTxn(m_lastLocalTxn);
    } else {
        txn = logBeginTxn(m_lastLocalTxn);
        m_pages.back().beginTxns += 1;
    }

    return txn;
}

//===========================================================================
void DbLog::commit(uint64_t txn) {
    // Log the commit first, so the commit transaction is counted on the page
    // it finished on.
    logCommit(txn);
    s_perfCurTxns -= 1;

    auto lsn = getLsn(txn);
    auto & commitTxns = m_pages.back().commitTxns;
    for (auto & lsn_txns : commitTxns) {
        if (lsn >= lsn_txns.first) {
            lsn_txns.second += 1;
            return;
        }
    }
    auto i = m_pages.end() - commitTxns.size() - 1;
    for (;; --i) {
        auto & lsn_txns = commitTxns.emplace_back(i->firstLsn, 0);
        if (lsn >= lsn_txns.first) {
            lsn_txns.second += 1;
            break;
        }
        assert(i != m_pages.begin());
    }
}

//===========================================================================
// How checkpointing works
//
// Walk all the dirty pages
//  - finds the LSN range of the pages
//  - flushes ones that are stable, makes copies of the rest
// Wait for the high LSN of the pages to become stable
// Write the pages from the copies
// Write the checkpoint record, specifying the low LSN as it's start
void DbLog::checkpoint() {
    if (m_phase != Checkpoint::Complete)
        return;
    m_checkpointStart = Clock::now();
    m_checkpointData = 0;
    m_phase = Checkpoint::WaitForPageFlush;
    s_perfCps += 1;
    s_perfCurCps += 1;
    taskPushCompute(m_checkpointPagesTask);
}

//===========================================================================
void DbLog::checkpointPages() {
    assert(m_phase == Checkpoint::WaitForPageFlush);
    m_checkpointLsn = m_lastLsn;
    m_page.checkpointPages();
    m_phase = Checkpoint::WaitForTxnCommits;
    if (m_checkpointLsn > m_stableTxn) {
        queueTask(&m_checkpointStablePagesTask, m_checkpointLsn);
    } else {
        checkpointStablePages();
    }
}

//===========================================================================
void DbLog::checkpointStablePages() {
    assert(m_phase == Checkpoint::WaitForTxnCommits);
    m_page.checkpointStablePages();
    logCommitCheckpoint(m_checkpointLsn);
    m_phase = Checkpoint::WaitForCheckpointCommit;
    queueTask(&m_checkpointStableCommitTask, m_lastLsn);
    flushWriteBuffer();
}

//===========================================================================
void DbLog::checkpointStableCommit() {
    assert(m_phase == Checkpoint::WaitForCheckpointCommit);

    auto lastPgno = uint32_t{0};
    {
        unique_lock<mutex> lk{m_bufMut};
        auto lastTxn = m_pages.back().firstLsn;
        auto before = m_pages.size();
        for (;;) {
            auto && pi = m_pages.front();
            if (pi.firstLsn >= lastTxn)
                break;
            if (pi.firstLsn + pi.numLogs > m_checkpointLsn)
                break;
            if (lastPgno)
                m_freePages.insert(lastPgno);
            lastPgno = pi.pgno;
            m_pages.pop_front();
        }
        s_perfFreePages += (unsigned) (before - m_pages.size() - (bool) lastPgno);
    }

    m_phase = Checkpoint::WaitForTruncateCommit;
    if (!lastPgno) {
        checkpointTruncateCommit();
    } else {
        static PageHeader hdr = { kPageTypeFree };
        hdr.pgno = lastPgno;
        fileWrite(
            this,
            m_flog,
            lastPgno * m_pageSize,
            &hdr,
            offsetof(PageHeader, pgno) + sizeof(hdr.pgno),
            taskComputeQueue()
        );
    }
}

//===========================================================================
void DbLog::checkpointTruncateCommit() {
    assert(m_phase == Checkpoint::WaitForTruncateCommit);
    m_phase = Checkpoint::Complete;
    s_perfCurCps -= 1;
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
void DbLog::queueTask(
    ITaskNotify * task,
    uint64_t waitTxn,
    TaskQueueHandle hq
) {
    if (!hq)
        hq = taskComputeQueue();
    unique_lock<mutex> lk{m_bufMut};
    if (m_stableTxn >= waitTxn) {
        taskPush(hq, *task);
    } else {
        auto ti = TaskInfo{task, waitTxn, hq};
        m_tasks.push(ti);
    }
}

//===========================================================================
void DbLog::flushWriteBuffer() {
    unique_lock<mutex> lk{m_bufMut};
    auto lp = (PageHeader *) bufPtr(m_curBuf);
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
void DbLog::updatePages_LK(const PageInfo & pi, bool partialWrite) {
    auto i = lower_bound(m_pages.begin(), m_pages.end(), pi);
    assert(i != m_pages.end() && i->firstLsn == pi.firstLsn);
    i->numLogs = pi.numLogs;

    auto base = i + 1;
    for (auto && lsn_txns : i->commitTxns) {
        base -= 1;
        assert(base->firstLsn == lsn_txns.first);
        if (lsn_txns.second) {
            assert(base->beginTxns >= lsn_txns.second);
            base->beginTxns -= lsn_txns.second;
            s_perfVolatileTxns -= lsn_txns.second;
        }
    }
    i->commitTxns.clear();
    if (partialWrite)
        i->commitTxns.emplace_back(pi.firstLsn, 0);

    if (base->firstLsn > m_stableTxn + 1) {
        s_perfReorderedWrites += 1;
        return;
    }

    uint64_t last = 0;
    for (i = base; i != m_pages.end(); ++i) {
        auto & npi = *i;
        if (npi.beginTxns
            || !npi.commitTxns.empty()
                && !(partialWrite && npi.firstLsn == pi.firstLsn)
        ) {
            break;
        }
        if (!npi.numLogs) {
            // The only page can have no logs on it is the very last page that
            // timed out waiting for more logs with just the second half of the
            // last log started on the previous page.
            assert(i + 1 == m_pages.end());
            continue;
        }
        last = npi.firstLsn + npi.numLogs - 1;
    }
    if (!last)
        return;
    assert(last > m_stableTxn);

    m_stableTxn = last;
    m_page.stable(m_stableTxn);
    while (!m_tasks.empty()) {
        auto & ti = m_tasks.top();
        if (m_stableTxn < ti.waitTxn)
            break;
        taskPush(ti.hq, *ti.notify);
        m_tasks.pop();
    }
}

//===========================================================================
void DbLog::onFileWrite(
    int written,
    string_view data,
    int64_t offset,
    FileHandle f
) {
    if (written != data.size())
        logMsgCrash() << "Write to .tsl failed, " << errno << ", " << _doserrno;

    s_perfWrites += 1;
    auto * lp = (PageHeader *) data.data();
    PageInfo pi = { lp->pgno, lp->firstLsn, lp->numLogs };
    unique_lock<mutex> lk{m_bufMut};
    if (lp->type == kPageTypeFree) {
        m_freePages.insert(lp->pgno);
        s_perfFreePages += 1;
        lk.unlock();
        checkpointTruncateCommit();
        return;
    }

    bool partialWrite = (data.size() < m_pageSize);
    updatePages_LK(pi, partialWrite);
    if (!partialWrite) {
        assert(data.size() == m_pageSize);
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

    auto lp = (PageHeader *) bufPtr(m_curBuf);
    lp->type = kPageTypeLog;
    if (!m_freePages.empty()) {
        lp->pgno = m_freePages.pop_front();
        s_perfFreePages -= 1;
    } else {
        lp->pgno = (uint32_t) m_numPages++;
        s_perfPages += 1;
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

    auto & pi = m_pages.emplace_back(PageInfo{});
    pi.pgno = lp->pgno;
    pi.firstLsn = lp->firstLsn;
    pi.numLogs = 0;
    pi.commitTxns.emplace_back(lp->firstLsn, 0);

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
        auto lp = (PageHeader *) bufPtr(m_curBuf);
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
