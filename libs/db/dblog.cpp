// Copyright Glen Knowles 2017 - 2018.
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

const unsigned kLogWriteBuffers = 10;
static_assert(kLogWriteBuffers > 1);


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
    StartRecovery,
    Complete,
    WaitForPageFlush,
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
    kPageTypeInvalid = 0,
    kPageTypeZero = 'lZ',
    kPageTypeLog = '2l',
    kPageTypeFree = 'F',

    // deprecated 2018-03-23
    kPageTypeLogV1 = 'l',
};

struct LogPage {
    PageType type;
    pgno_t pgno;
    uint32_t checksum;
    uint64_t firstLsn; // LSN of first record started on page
    uint16_t numLogs; // number of log records started on page
    uint16_t firstPos; // position of first log started on page
    uint16_t lastPos; // position after last log record ended on page
};

#pragma pack(push)
#pragma pack(1)

struct ZeroPage {
    DbPageHeader hdr;
    char signature[sizeof(kLogFileSig)];
    uint32_t pageSize;
};

struct MinimumPage {
    PageType type;
    pgno_t pgno;
};

struct PageHeaderRawV2 {
    PageType type;
    pgno_t pgno;
    uint32_t checksum;
    uint64_t firstLsn;
    uint16_t numLogs;
    uint16_t firstPos;
    uint16_t lastPos;
};

// deprecated 2018-03-23
struct PageHeaderRawV1 {
    PageType type;
    pgno_t pgno;
    uint64_t firstLsn;
    uint16_t numLogs;
    uint16_t firstPos;
    uint16_t lastPos;
};

#pragma pack(pop)

constexpr size_t kMaxHdrLen =
    max(sizeof(PageHeaderRawV1), sizeof(PageHeaderRawV2));

} // namespace


/****************************************************************************
*
*   Variables
*
***/

static auto & s_perfCps = uperf("db.checkpoints (total)");
static auto & s_perfCurCps = uperf("db.checkpoints (current)");
static auto & s_perfCurTxns = uperf("db.transactions (current)");
static auto & s_perfVolatileTxns = uperf("db.transactions (volatile)");
static auto & s_perfPages = uperf("db.wal pages (total)");
static auto & s_perfFreePages = uperf("db.wal pages (free)");
static auto & s_perfWrites = uperf("db.wal writes (total)");
static auto & s_perfReorderedWrites = uperf("db.wal writes (out of order)");
static auto & s_perfPartialWrites = uperf("db.wal writes (partial)");


/****************************************************************************
*
*   Helpers
*
***/

//===========================================================================
static TaskQueueHandle logQueue() {
    static TaskQueueHandle s_hq = taskCreateQueue("Log IO", 2);
    return s_hq;
}

//===========================================================================
static void pack(void * ptr, const LogPage & lp) {
    auto mp = (MinimumPage *) ptr;
    mp->type = lp.type;
    mp->pgno = lp.pgno;
    auto v1 = (PageHeaderRawV1 *) ptr;
    auto v2 = (PageHeaderRawV2 *) ptr;
    switch (lp.type) {
    case kPageTypeFree:
        break;
    case kPageTypeLog:
        assert(v2->type == lp.type);
        v2->checksum = lp.checksum;
        v2->firstLsn = lp.firstLsn;
        v2->numLogs = lp.numLogs;
        v2->firstPos = lp.firstPos;
        v2->lastPos = lp.lastPos;
        break;
    case kPageTypeLogV1:
        assert(v1->type == lp.type);
        v1->firstLsn = lp.firstLsn;
        v1->numLogs = lp.numLogs;
        v1->firstPos = lp.firstPos;
        v1->lastPos = lp.lastPos;
        break;
    default:
        logMsgFatal() << "pack log page " << lp.pgno
            << ", unknown type: " << lp.type;
        break;
    }
}

//===========================================================================
static void unpack(LogPage * out, const void * ptr) {
    auto mp = (const MinimumPage *) ptr;
    out->type = mp->type;
    out->pgno = mp->pgno;
    auto v1 = (const PageHeaderRawV1 *) ptr;
    auto v2 = (const PageHeaderRawV2 *) ptr;
    switch (mp->type) {
    case kPageTypeFree:
        out->checksum = 0;
        out->firstLsn = 0;
        out->numLogs = 0;
        out->firstPos = 0;
        out->lastPos = 0;
        break;
    case kPageTypeLog:
        assert(mp->type == v2->type);
        out->checksum = v2->checksum;
        out->firstLsn = v2->firstLsn;
        out->numLogs = v2->numLogs;
        out->firstPos = v2->firstPos;
        out->lastPos = v2->lastPos;
        break;
    case kPageTypeLogV1:
        assert(mp->type == v1->type);
        out->checksum = 0;
        out->firstLsn = v1->firstLsn;
        out->numLogs = v1->numLogs;
        out->firstPos = v1->firstPos;
        out->lastPos = v1->lastPos;
        break;
    default:
        logMsgFatal() << "unpack log page " << mp->pgno
            << ", unknown type: " << mp->type;
        break;
    }
}

//===========================================================================
static size_t logHdrLen(PageType type) {
    switch (type) {
    case kPageTypeLog:
        return sizeof(PageHeaderRawV2);
    case kPageTypeLogV1:
        return sizeof(PageHeaderRawV1);
    default:
        logMsgFatal() << "logHdrLen, unknown page type: " << type;
        return 0;
    }
}


/****************************************************************************
*
*   DbLog::LsnTaskInfo
*
***/

//===========================================================================
bool DbLog::LsnTaskInfo::operator<(const LsnTaskInfo & right) const {
    return waitLsn < right.waitLsn;
}

//===========================================================================
bool DbLog::LsnTaskInfo::operator>(const LsnTaskInfo & right) const {
    return waitLsn > right.waitLsn;
}


/****************************************************************************
*
*   DbLog
*
***/

//===========================================================================
DbLog::DbLog(IApplyNotify * data, IPageNotify * page)
    : m_data(data)
    , m_page(page)
    , m_maxCheckpointData{kDefaultMaxCheckpointData}
    , m_maxCheckpointInterval{kDefaultMaxCheckpointInterval}
    , m_checkpointTimer([&](auto){ checkpoint(); return kTimerInfinite; })
    , m_checkpointPagesTask([&]{ checkpointPages(); })
    , m_checkpointStableCommitTask([&]{ checkpointStableCommit(); })
    , m_flushTimer([&](auto){ flushWriteBuffer(); return kTimerInfinite; })
{}

//===========================================================================
DbLog::~DbLog() {
    if (m_flog)
        fileClose(m_flog);
    if (m_buffers)
        aligned_free(m_buffers);
    if (m_partialBuffers)
        aligned_free(m_partialBuffers);
}

//===========================================================================
char * DbLog::bufPtr(size_t ibuf) {
    assert(ibuf < m_numBufs);
    return m_buffers + ibuf * m_pageSize;
}

//===========================================================================
char * DbLog::partialPtr(size_t ibuf) {
    assert(ibuf < m_numBufs);
    return m_partialBuffers + ibuf * m_pageSize;
}

//===========================================================================
static FileHandle openDbFile(
    string_view logfile,
    DbOpenFlags flags,
    bool align
) {
    auto oflags = File::fDenyWrite;
    if (align)
        oflags |= File::fAligned;
    if (flags & fDbOpenReadOnly) {
        oflags |= File::fReadOnly;
    } else {
        oflags |= File::fReadWrite;
    }
    if (flags & fDbOpenCreat)
        oflags |= File::fCreat;
    if (flags & fDbOpenTrunc)
        oflags |= File::fTrunc;
    if (flags & fDbOpenExcl)
        oflags |= File::fExcl;
    auto f = fileOpen(logfile, oflags);
    if (!f)
        logMsgError() << "Open failed, " << logfile;
    return f;
}

//===========================================================================
bool DbLog::open(string_view logfile, size_t dataPageSize, DbOpenFlags flags) {
    assert(!m_closing && !m_flog);
    assert(dataPageSize == pow2Ceil(dataPageSize));
    assert(!dataPageSize || dataPageSize >= kMinPageSize);

    m_openFlags = flags;
    m_flog = openDbFile(logfile, flags, true);
    if (!m_flog)
        return false;

    auto fps = filePageSize(m_flog);
    auto len = fileSize(m_flog);
    ZeroPage zp{};
    if (!len) {
        if (!dataPageSize)
            dataPageSize = kDefaultPageSize;
    } else {
        auto rawbuf = aligned_alloc(fps, fps);
        fileReadWait(rawbuf, fps, m_flog, 0);
        memcpy(&zp, rawbuf, sizeof(zp));
        if (!dataPageSize)
            dataPageSize = zp.pageSize / 2;
        aligned_free(rawbuf);
    }
    if (dataPageSize < fps) {
        // Page size is smaller than minimum required for aligned access.
        // Reopen unaligned.
        fileClose(m_flog);
        m_flog = openDbFile(logfile, flags, false);
    }

    m_pageSize = 2 * dataPageSize;
    m_numBufs = kLogWriteBuffers;
    m_bufStates.resize(m_numBufs, Buffer::Empty);
    m_emptyBufs = m_numBufs;
    m_buffers = (char *) aligned_alloc(m_pageSize, m_numBufs * m_pageSize);
    memset(m_buffers, 0, m_numBufs * m_pageSize);
    m_partialBuffers = (char *) aligned_alloc(
        m_pageSize,
        m_numBufs * m_pageSize
    );
    memset(m_partialBuffers, 0, m_numBufs * m_pageSize);
    m_curBuf = 0;
    for (unsigned i = 0; i < m_numBufs; ++i) {
        auto mp = (MinimumPage *) bufPtr(i);
        mp->type = kPageTypeFree;
    }
    m_bufPos = m_pageSize;

    if (!len) {
        m_phase = Checkpoint::Complete;
        m_newFiles = true;

        zp.hdr.type = (DbPageType) kPageTypeZero;
        memcpy(zp.signature, kLogFileSig, sizeof(zp.signature));
        zp.pageSize = (unsigned) m_pageSize;
        zp.hdr.checksum = 0;
        auto nraw = partialPtr(0);
        memcpy(nraw, &zp, sizeof(zp));
        zp.hdr.checksum = hash_crc32c(nraw, m_pageSize);
        memcpy(nraw, &zp, sizeof(zp));
        fileWriteWait(m_flog, 0, nraw, m_pageSize);
        s_perfWrites += 1;
        m_numPages = 1;
        s_perfPages += (unsigned) m_numPages;
        m_lastLsn = 0;
        m_localTxns.clear();
        m_checkpointLsn = m_lastLsn + 1;
        logCommitCheckpoint(m_checkpointLsn);
        return true;
    }

    if (memcmp(zp.signature, kLogFileSig, sizeof(zp.signature)) != 0) {
        logMsgError() << "Bad signature, " << logfile;
        return false;
    }
    if (zp.pageSize != m_pageSize) {
        logMsgError() << "Mismatched page size, " << logfile;
        return false;
    }

    m_numPages = (len + m_pageSize - 1) / m_pageSize;
    s_perfPages += (unsigned) m_numPages;
    return true;
}

//===========================================================================
void DbLog::close() {
    if (!m_flog)
        return;

    m_closing = true;
    if (m_phase == Checkpoint::StartRecovery
        || (m_openFlags & fDbOpenReadOnly)
    ) {
        fileClose(m_flog);
        m_flog = {};
        return;
    }

    if (m_numBufs) {
        checkpoint();
        flushWriteBuffer();
    }
    unique_lock lk{m_bufMut};
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
    auto lastPage = (pgno_t) m_numPages - 1;
    while (m_freePages.count(lastPage))
        lastPage -= 1;
    fileResize(m_flog, (lastPage + 1) * m_pageSize);
    fileClose(m_flog);
    m_flog = {};
}

//===========================================================================
DbConfig DbLog::configure(const DbConfig & conf) {
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

    auto tmp = conf;
    tmp.checkpointMaxData = maxData;
    tmp.checkpointMaxInterval = maxInterval;
    return tmp;
}

//===========================================================================
void DbLog::blockCheckpoint(IDbProgressNotify * notify, bool enable) {
    if (enable) {
        DbProgressInfo info = {};
        m_checkpointBlocks.push_back(notify);
        if (m_phase == Checkpoint::Complete) {
            notify->onDbProgress(kRunStopped, info);
        } else {
            notify->onDbProgress(kRunStopping, info);
        }
        return;
    }

    // Remove the block
    auto i = find(m_checkpointBlocks.begin(), m_checkpointBlocks.end(), notify);
    if (i != m_checkpointBlocks.end())
        m_checkpointBlocks.erase(i);
    if (m_checkpointBlocks.empty() && m_phase == Checkpoint::Complete)
        checkpointWaitForNext();
}


/****************************************************************************
*
*   DbLog - recovery
*
***/

//===========================================================================
bool DbLog::recover(RecoverFlags flags) {
    if (m_phase != Checkpoint::StartRecovery)
        return true;

    m_phase = Checkpoint::Complete;
    m_checkpointStart = Clock::now();

    auto logfile = filePath(m_flog);
    auto flog = fileOpen(
        logfile,
        File::fReadOnly | File::fBlocking | File::fDenyNone | File::fSequential
    );
    if (!flog) {
        logMsgError() << "Open failed, " << logfile;
        return false;
    }
    Finally flog_f([&]() { fileClose(flog); });

    if (!loadPages(flog))
        return false;
    if (m_pages.empty())
        return true;

    // Go through log entries looking for last committed checkpoint and the
    // set of incomplete transactions (so we can avoid trying to redo them
    // later).
    if (m_openFlags & fDbOpenVerbose)
        logMsgInfo() << "Analyze database";
    m_checkpointLsn = m_pages.front().firstLsn;
    AnalyzeData data;
    if (~flags & fRecoverBeforeCheckpoint) {
        applyAll(&data, flog);
        if (!data.checkpoint)
            logMsgFatal() << "Invalid .tsl file, no checkpoint found";
        m_checkpointLsn = data.checkpoint;
    }

    if (flags & fRecoverIncompleteTxns) {
        data.incompleteTxnLsns.clear();
    } else {
        for (auto && kv : data.txns)
            data.incompleteTxnLsns.push_back(kv.second);
        sort(
            data.incompleteTxnLsns.begin(),
            data.incompleteTxnLsns.end(),
            [](auto & a, auto & b) { return a > b; }
        );
        auto i = lower_bound(
            data.incompleteTxnLsns.begin(),
            data.incompleteTxnLsns.end(),
            data.checkpoint
        );
        data.incompleteTxnLsns.erase(data.incompleteTxnLsns.begin(), i);
    }

    // Go through log entries starting with the last committed checkpoint and
    // redo all complete transactions found.
    if (m_openFlags & fDbOpenVerbose)
        logMsgInfo() << "Recover database";
    data.analyze = false;
    applyAll(&data, flog);
    if (~flags & fRecoverIncompleteTxns) {
        assert(data.incompleteTxnLsns.empty());
        assert(!data.activeTxns);
    }

    auto & back = m_pages.back();
    m_stableLsn = back.firstLsn + back.numLogs - 1;
    m_lastLsn = m_stableLsn;
    m_page->onLogStable(m_stableLsn, 0);
    return true;
}

//===========================================================================
// Creates array of references to last page and its contiguous predecessors
bool DbLog::loadPages(FileHandle flog) {
    if (m_openFlags & fDbOpenVerbose)
        logMsgInfo() << "Verify transaction log";

    auto rawbuf = partialPtr(0);
    LogPage lp;
    PageInfo * pi;
    uint32_t checksum;
    // load info for each page
    for (auto i = (pgno_t) 1; i < m_numPages; i = pgno_t(i + 1)) {
        fileReadWait(rawbuf, m_pageSize, flog, i * m_pageSize);
        auto mp = (MinimumPage *) rawbuf;
        switch (mp->type) {
        case kPageTypeInvalid:
            i = (pgno_t) m_numPages;
            break;
        case kPageTypeLogV1:
            unpack(&lp, rawbuf);
            pi = &m_pages.emplace_back();
            pi->pgno = lp.pgno;
            pi->firstLsn = lp.firstLsn;
            pi->numLogs = lp.numLogs;
            break;
        case kPageTypeLog:
            unpack(&lp, rawbuf);
            checksum = lp.checksum;
            lp.checksum = 0;
            pack(rawbuf, lp);
            lp.checksum = hash_crc32c(rawbuf, m_pageSize);
            if (checksum != lp.checksum) {
                logMsgError() << "Invalid checksum on page #"
                    << i << " of " << filePath(flog);
                goto MAKE_FREE;
            }
            pi = &m_pages.emplace_back();
            pi->pgno = lp.pgno;
            pi->firstLsn = lp.firstLsn;
            pi->numLogs = lp.numLogs;
            break;
        default:
            logMsgError() << "Invalid page type(" << mp->type << ") on page #"
                << i << " of " << filePath(flog);
        MAKE_FREE:
            mp->type = kPageTypeFree;
            mp->pgno = i;
            [[fallthrough]];
        case kPageTypeFree:
            m_freePages.insert(mp->pgno);
            s_perfFreePages += 1;
            break;
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
void DbLog::applyAll(AnalyzeData * data, FileHandle flog) {
    LogPage lp;
    auto buf = (char *) aligned_alloc(m_pageSize, 2 * m_pageSize);
    auto buf2 = (char *) aligned_alloc(m_pageSize, 2 * m_pageSize);
    auto finally = Finally([&] { aligned_free(buf); aligned_free(buf2); });
    int bytesBefore{0};
    int logPos{0};
    auto lsn = uint64_t{0};
    auto log = (Record *) nullptr;

    for (auto & pi : m_pages) {
        fileReadWait(buf2, m_pageSize, flog, pi.pgno * m_pageSize);
        unpack(&lp, buf2);
        if (bytesBefore) {
            auto bytesAfter = lp.firstPos - logHdrLen(lp.type);
            memcpy(
                buf + m_pageSize,
                buf2 + logHdrLen(lp.type),
                bytesAfter
            );
            log = (Record *) (buf + m_pageSize - bytesBefore);
            assert(size(*log) == bytesBefore + bytesAfter);
            apply(data, lp.firstLsn - 1, *log);
        }
        swap(buf, buf2);

        logPos = lp.firstPos;
        lsn = lp.firstLsn;
        while (logPos < lp.lastPos) {
            log = (Record *) (buf + logPos);
            apply(data, lsn, *log);
            logPos += size(*log);
            lsn += 1;
        }
        assert(logPos == lp.lastPos);
        bytesBefore = (int) (m_pageSize - logPos);
    }

    // Initialize log write buffers with last buffer (if partial) found
    // during analyze.
    if (data->analyze && logPos < m_pageSize) {
        memcpy(m_buffers, buf, logPos);
        m_bufPos = logPos;
        m_bufStates[m_curBuf] = Buffer::PartialClean;
        m_emptyBufs -= 1;
        auto & pi = m_pages.back();
        unpack(&lp, bufPtr(m_curBuf));
        assert(lp.firstLsn == pi.firstLsn);
        pi.commitTxns.emplace_back(lp.firstLsn, 0);
    }
}

//===========================================================================
void DbLog::applyCommitCheckpoint(
    AnalyzeData * data,
    uint64_t lsn,
    uint64_t startLsn
) {
    if (data->analyze) {
        if (startLsn >= m_checkpointLsn)
            data->checkpoint = startLsn;
        return;
    }

    // redo
    if (lsn < data->checkpoint)
        return;
    m_data->onLogApplyCommitCheckpoint(lsn, startLsn);
}

//===========================================================================
void DbLog::applyBeginTxn(
    AnalyzeData * data,
    uint64_t lsn,
    uint16_t localTxn
) {
    if (data->analyze) {
        auto & txnLsn = data->txns[localTxn];
        if (txnLsn)
            data->incompleteTxnLsns.push_back(txnLsn);
        txnLsn = lsn;
        return;
    }

    // redo
    if (lsn < data->checkpoint)
        return;
    if (!data->incompleteTxnLsns.empty()
        && lsn == data->incompleteTxnLsns.back()
    ) {
        data->incompleteTxnLsns.pop_back();
        return;
    }
    if (!data->activeTxns.insert(localTxn)) {
        logMsgError() << "Duplicate transaction id " << localTxn
            << " at LSN " << lsn;
    }
    m_data->onLogApplyBeginTxn(lsn, localTxn);
}

//===========================================================================
void DbLog::applyCommitTxn(
    AnalyzeData * data,
    uint64_t lsn,
    uint16_t localTxn
) {
    if (data->analyze) {
        data->txns.erase(localTxn);
        return;
    }

    // redo
    if (lsn < data->checkpoint)
        return;
    if (!data->activeTxns.erase(localTxn)) {
        // Commits for transaction ids with no preceding begin are allowed
        // and ignored under the assumption that they are the previously
        // played continuations of transactions that begin before the start
        // of this recovery.
        //
        // With some extra tracking, the rule that every commit of an id after
        // the first must have a matching begin could be enforced.
    }
    m_data->onLogApplyCommitTxn(lsn, localTxn);
}

//===========================================================================
void DbLog::applyUpdate(AnalyzeData * data, uint64_t lsn, const Record & log) {
    if (data->analyze)
        return;

    // redo
    if (lsn < data->checkpoint)
        return;

    auto localTxn = getLocalTxn(log);
    if (localTxn && !data->activeTxns.count(localTxn))
        return;

    auto pgno = getPgno(log);
    if (auto ptr = m_page->onLogGetRedoPtr(pgno, lsn, localTxn))
        applyUpdate(ptr, log);
}


/****************************************************************************
*
*   DbLog - checkpoint
*
***/

//===========================================================================
// Checkpointing places a marker in the log to indicate the start of entries
// that are needed to fully recover the database. Any entries before that point
// will subsequently be skipped and/or discarded.
void DbLog::checkpoint() {
    if (m_phase != Checkpoint::Complete
        || !m_checkpointBlocks.empty()
        || (m_openFlags & fDbOpenReadOnly)
    ) {
        return;
    }

    if (m_openFlags & fDbOpenVerbose)
        logMsgInfo() << "Checkpoint started";
    m_checkpointStart = Clock::now();
    m_checkpointData = 0;
    m_phase = Checkpoint::WaitForPageFlush;
    s_perfCps += 1;
    s_perfCurCps += 1;
    taskPushCompute(&m_checkpointPagesTask);
}

//===========================================================================
void DbLog::checkpointPages() {
    assert(m_phase == Checkpoint::WaitForPageFlush);
    if (!fileFlush(m_flog))
        logMsgFatal() << "Checkpointing failed.";
    auto nextLsn = m_page->onLogCheckpointPages(m_checkpointLsn);
    if (nextLsn == m_checkpointLsn) {
        m_phase = Checkpoint::WaitForTruncateCommit;
        checkpointTruncateCommit();
        return;
    }
    m_checkpointLsn = nextLsn;
    logCommitCheckpoint(m_checkpointLsn);
    m_phase = Checkpoint::WaitForCheckpointCommit;
    queueTask(&m_checkpointStableCommitTask, m_lastLsn);
    flushWriteBuffer();
}

//===========================================================================
void DbLog::checkpointStableCommit() {
    assert(m_phase == Checkpoint::WaitForCheckpointCommit);
    if (!fileFlush(m_flog))
        logMsgFatal() << "Checkpointing failed.";

    auto lastPgno = pgno_t{0};
    {
        unique_lock lk{m_bufMut};
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
        s_perfFreePages +=
            (unsigned) (before - m_pages.size() - (bool) lastPgno);
    }

    m_phase = Checkpoint::WaitForTruncateCommit;
    if (!lastPgno) {
        checkpointTruncateCommit();
    } else {
        auto vptr = aligned_alloc(m_pageSize, m_pageSize);
        auto mp = new(vptr) MinimumPage{ kPageTypeFree };
        mp->pgno = lastPgno;
        fileWrite(
            this,
            m_flog,
            lastPgno * m_pageSize,
            mp,
            m_pageSize,
            logQueue()
        );
    }
}

//===========================================================================
void DbLog::checkpointTruncateCommit() {
    assert(m_phase == Checkpoint::WaitForTruncateCommit);
    if (m_openFlags & fDbOpenVerbose)
        logMsgInfo() << "Checkpoint completed";
    m_phase = Checkpoint::Complete;
    s_perfCurCps -= 1;
    if (m_checkpointBlocks.empty()) {
        checkpointWaitForNext();
    } else {
        DbProgressInfo info = {};
        for (auto && block : m_checkpointBlocks)
            block->onDbProgress(kRunStopped, info);
    }
    m_bufAvailCv.notify_one();
}

//===========================================================================
void DbLog::checkpointWaitForNext() {
    if (!m_closing) {
        Duration wait = 0ms;
        auto elapsed = Clock::now() - m_checkpointStart;
        if (elapsed < m_maxCheckpointInterval)
            wait = m_maxCheckpointInterval - elapsed;
        if (m_checkpointData >= m_maxCheckpointData)
            wait = 0ms;
        timerUpdate(&m_checkpointTimer, wait);
    }
}


/****************************************************************************
*
*   DbLog - logging
*
***/

//===========================================================================
uint64_t DbLog::beginTxn() {
    uint16_t localTxn = 0;
    {
        scoped_lock lk{m_bufMut};
        if (!m_localTxns) {
            localTxn = 1;
        } else {
            auto txns = *m_localTxns.ranges().begin();
            localTxn = txns.first > 1 ? 1 : (uint16_t) txns.second + 1;
            if (localTxn == numeric_limits<uint16_t>::max())
                logMsgFatal() << "Too many concurrent transactions";
        }
        m_localTxns.insert(localTxn);
    }

    s_perfCurTxns += 1;
    s_perfVolatileTxns += 1;
    return logBeginTxn(localTxn);
}

//===========================================================================
void DbLog::commit(uint64_t txn) {
    logCommit(txn);
    s_perfCurTxns -= 1;

    auto localTxn = getLocalTxn(txn);
    scoped_lock lk{m_bufMut};
    [[maybe_unused]] auto found = m_localTxns.erase(localTxn);
    assert(found && "Commit of unknown transaction");
}

//===========================================================================
uint64_t DbLog::log(
    const Record & log,
    size_t bytes,
    TxnMode txnMode,
    uint64_t txn
) {
    assert(bytes < m_pageSize - kMaxHdrLen);
    assert(bytes == size(log));

    unique_lock lk{m_bufMut};
    while (m_bufPos + bytes > m_pageSize && !m_emptyBufs)
        m_bufAvailCv.wait(lk);
    auto lsn = ++m_lastLsn;

    // Count transaction beginnings on the page their log record started. This
    // means the current page before logging (since logging can advance to the
    // next page), UNLESS it's exactly at the end of the page. In that case the
    // transaction actually starts on the next page, which is where we'll be
    // after logging.
    // Transaction commits are counted after logging, so it's always on the
    // page where they finished.
    if (m_bufPos == m_pageSize) {
        prepareBuffer_LK(log, 0, bytes);
        if (txnMode == TxnMode::kBegin) {
            countBeginTxn_LK();
        } else if (txnMode == TxnMode::kCommit) {
            countCommitTxn_LK(txn);
        }
        return lsn;
    }
    if (txnMode == TxnMode::kBegin)
        countBeginTxn_LK();

    size_t overflow = 0;
    if (auto avail = m_pageSize - m_bufPos; bytes > avail) {
        overflow = bytes - avail;
        bytes = avail;
    }
    auto base = bufPtr(m_curBuf) + m_bufPos;
    memcpy(base, &log, bytes);
    m_bufPos += bytes;

    if (m_bufPos != m_pageSize) {
        if (m_bufStates[m_curBuf] == Buffer::PartialClean
            || m_bufStates[m_curBuf] == Buffer::Empty
        ) {
            m_bufStates[m_curBuf] = Buffer::PartialDirty;
            timerUpdate(&m_flushTimer, kDirtyWriteBufferTimeout);
        }
        if (txnMode == TxnMode::kCommit)
            countCommitTxn_LK(txn);
    } else {
        bool writeInProgress = m_bufStates[m_curBuf] == Buffer::PartialWriting;
        m_bufStates[m_curBuf] = Buffer::FullWriting;
        LogPage lp;
        auto rawbuf = bufPtr(m_curBuf);
        unpack(&lp, rawbuf);
        lp.numLogs = (uint16_t) (m_lastLsn - lp.firstLsn + 1);
        lp.lastPos = (uint16_t) m_bufPos;
        if (overflow)
            lp.lastPos -= (uint16_t) bytes;
        lp.checksum = 0;
        pack(rawbuf, lp);

        if (overflow)
            prepareBuffer_LK(log, bytes, overflow);
        if (txnMode == TxnMode::kCommit)
            countCommitTxn_LK(txn);

        lk.unlock();
        if (!writeInProgress) {
            lp.checksum = hash_crc32c(rawbuf, m_pageSize);
            pack(rawbuf, lp);
            auto offset = lp.pgno * m_pageSize;
            fileWrite(this, m_flog, offset, rawbuf, m_pageSize, logQueue());
        }
    }
    return lsn;
}

//===========================================================================
void DbLog::queueTask(
    ITaskNotify * task,
    uint64_t waitLsn,
    TaskQueueHandle hq
) {
    if (!hq)
        hq = taskComputeQueue();
    unique_lock lk{m_bufMut};
    if (m_stableLsn >= waitLsn) {
        taskPush(hq, task);
    } else {
        auto ti = LsnTaskInfo{task, waitLsn, hq};
        m_lsnTasks.push(ti);
    }
}

//===========================================================================
void DbLog::flushWriteBuffer() {
    unique_lock lk{m_bufMut};
    if (m_bufStates[m_curBuf] != Buffer::PartialDirty)
        return;

    m_bufStates[m_curBuf] = Buffer::PartialWriting;
    LogPage lp;
    auto rawbuf = bufPtr(m_curBuf);
    unpack(&lp, rawbuf);
    lp.numLogs = (uint16_t) (m_lastLsn - lp.firstLsn + 1);
    lp.lastPos = (uint16_t) m_bufPos;
    lp.checksum = 0;
    pack(rawbuf, lp);
    auto offset = lp.pgno * m_pageSize;

    // Write the entire page, not just the changed part, otherwise the
    // resulting page might not match the checksum.
    auto nraw = partialPtr(m_curBuf);
    memcpy(nraw, rawbuf, m_pageSize);

    lk.unlock();
    if (lp.type != kPageTypeFree) {
        assert(lp.type == kPageTypeLog || lp.type == kPageTypeLogV1);
        lp.checksum = hash_crc32c(nraw, m_pageSize);
        pack(nraw, lp);
    }
    fileWrite(this, m_flog, offset, nraw, m_pageSize, logQueue());
}

//===========================================================================
void DbLog::updatePages_LK(const PageInfo & pi, bool fullPageWrite) {
    auto i = lower_bound(m_pages.begin(), m_pages.end(), pi);
    assert(i != m_pages.end() && i->firstLsn == pi.firstLsn);
    i->numLogs = pi.numLogs;

    auto base = i + 1;
    for (auto && [lsn, txns] : i->commitTxns) {
        base -= 1;
        assert(base->firstLsn == lsn);
        if (txns) {
            assert(base->activeTxns >= txns);
            base->activeTxns -= txns;
            s_perfVolatileTxns -= txns;
        }
    }
    i->commitTxns.clear();
    // Mark page as incomplete after a partial write by putting an empty
    // placeholder.
    if (!fullPageWrite)
        i->commitTxns.emplace_back(pi.firstLsn, 0);

    if (base->firstLsn > m_stableLsn + 1) {
        s_perfReorderedWrites += 1;
        return;
    }

    uint64_t last = 0;
    for (i = base; i != m_pages.end(); ++i) {
        auto & npi = *i;
        if (npi.activeTxns || !npi.numLogs)
            break;
        if (!npi.commitTxns.empty()) {
            if (npi.commitTxns.size() != 1 || npi.commitTxns[0].second)
                break;
            assert(npi.firstLsn == npi.commitTxns[0].first);
        }
        if (!npi.numLogs) {
            // The only page that can have no logs on it is a very last page
            // that timed out waiting for more logs with just the second half
            // of the last log started on the previous page.
            assert(i + 1 == m_pages.end());
            continue;
        }
        last = npi.firstLsn + npi.numLogs - 1;
    }
    if (!last)
        return;
    assert(last > m_stableLsn);

    m_stableLsn = last;
    m_page->onLogStable(
        m_stableLsn,
        fullPageWrite ? m_pageSize * (i - base) : 0
    );
    while (!m_lsnTasks.empty()) {
        auto & ti = m_lsnTasks.top();
        if (m_stableLsn < ti.waitLsn)
            break;
        taskPush(ti.hq, ti.notify);
        m_lsnTasks.pop();
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
        logMsgFatal() << "Write to .tsl failed, " << errno << ", " << _doserrno;

    auto rawbuf = (char *) data.data();
    s_perfWrites += 1;
    LogPage lp;
    unpack(&lp, rawbuf);
    PageInfo pi = { lp.pgno, lp.firstLsn, lp.numLogs };
    unique_lock lk{m_bufMut};
    if (lp.type == kPageTypeFree) {
        m_freePages.insert(lp.pgno);
        s_perfFreePages += 1;
        lk.unlock();
        aligned_free(rawbuf);
        checkpointTruncateCommit();
        return;
    }

    bool fullPageWrite = rawbuf >= m_buffers
        && rawbuf < m_buffers + m_numBufs * m_pageSize;
    updatePages_LK(pi, fullPageWrite);
    if (fullPageWrite) {
        assert(data.size() == m_pageSize);
        m_emptyBufs += 1;
        auto ibuf = (rawbuf - m_buffers) / m_pageSize;
        m_bufStates[ibuf] = Buffer::Empty;
        lp.type = kPageTypeFree;
        pack(rawbuf, lp);
        m_checkpointData += m_pageSize;
        bool needCheckpoint = m_checkpointData >= m_maxCheckpointData;
        lk.unlock();
        m_bufAvailCv.notify_one();
        if (needCheckpoint)
            timerUpdate(&m_checkpointTimer, 0ms);
        return;
    }

    // it's a partial
    assert(rawbuf >= m_partialBuffers
        && rawbuf < m_partialBuffers + m_numBufs * m_pageSize
    );
    s_perfPartialWrites += 1;
    auto ibuf = (rawbuf - m_partialBuffers) / m_pageSize;
    rawbuf = bufPtr(ibuf);
    LogPage olp;
    unpack(&olp, rawbuf);
    if (m_bufStates[ibuf] == Buffer::PartialWriting) {
        if (olp.numLogs == lp.numLogs) {
            m_bufStates[ibuf] = Buffer::PartialClean;
            lk.unlock();
            m_bufAvailCv.notify_one();
        } else {
            m_bufStates[ibuf] = Buffer::PartialDirty;
            lk.unlock();
            timerUpdate(&m_flushTimer, kDirtyWriteBufferTimeout);
        }
    } else if (m_bufStates[ibuf] == Buffer::FullWriting) {
        lk.unlock();
        olp.checksum = hash_crc32c(rawbuf, m_pageSize);
        pack(rawbuf, olp);
        fileWrite(this, m_flog, offset, rawbuf, m_pageSize, logQueue());
    }
}

//===========================================================================
void DbLog::prepareBuffer_LK(
    const Record & log,
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

    LogPage lp;
    auto rawbuf = bufPtr(m_curBuf);
    lp.type = kPageTypeLog;
    lp.checksum = 0;
    auto hdrLen = logHdrLen(lp.type);
    if (m_freePages) {
        lp.pgno = (pgno_t) m_freePages.pop_front();
        s_perfFreePages -= 1;
    } else {
        lp.pgno = (pgno_t) m_numPages++;
        s_perfPages += 1;
    }
    if (bytesOnOldPage) {
        lp.firstLsn = m_lastLsn + 1;
        lp.firstPos = (uint16_t) (hdrLen + bytesOnNewPage);
    } else {
        lp.firstLsn = m_lastLsn;
        lp.firstPos = (uint16_t) hdrLen;
    }
    lp.numLogs = 0;
    lp.lastPos = 0;
    pack(rawbuf, lp);

    auto & pi = m_pages.emplace_back(PageInfo{});
    pi.pgno = lp.pgno;
    pi.firstLsn = lp.firstLsn;
    pi.numLogs = 0;
    pi.commitTxns.emplace_back(lp.firstLsn, 0);

    m_bufStates[m_curBuf] = Buffer::PartialDirty;
    m_emptyBufs -= 1;
    memcpy(
        rawbuf + hdrLen,
        (const char *) &log + bytesOnOldPage,
        bytesOnNewPage
    );
    m_bufPos = hdrLen + bytesOnNewPage;

    timerUpdate(&m_flushTimer, kDirtyWriteBufferTimeout);
}

//===========================================================================
void DbLog::countBeginTxn_LK() {
    m_pages.back().activeTxns += 1;
}

//===========================================================================
void DbLog::countCommitTxn_LK(uint64_t txn) {
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
