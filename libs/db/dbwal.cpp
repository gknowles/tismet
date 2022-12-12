// Copyright Glen Knowles 2017 - 2022.
// Distributed under the Boost Software License, Version 1.0.
//
// dbwal.cpp - tismet db
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

enum class DbWal::Buffer : int {
    kEmpty,
    kPartialDirty,
    kPartialWriting,
    kPartialClean,
    kFullWriting,
};

enum class DbWal::Checkpoint : int {
    kStartRecovery,
    kComplete,
    kFlushPages,
    kFlushCheckpoint,
    kTruncateWal,
};

struct DbWal::AnalyzeData {
    bool analyze{true};
    unordered_map<uint16_t, uint64_t> txns;
    vector<uint64_t> incompleteTxnLsns;
    uint64_t checkpoint{0};

    UnsignedSet activeTxns;
};

namespace {

const Guid kLogFileSig = "b45d8e5a-851d-42f5-ac31-9ca00158597b"_Guid;

enum class LogPageType {
    kInvalid = 0,
    kZero = 'lZ',
    kLog = '2l',
    kFree = 'F',

    // deprecated 2018-03-23
    kLogV1 = 'l',
};
ostream & operator<<(ostream & os, LogPageType type) {
    if ((unsigned) type > 0xff)
        os << (char) ((unsigned) type >> 8);
    os << (char) ((unsigned) type & 0xff);
    return os;
}

struct LogPage {
    LogPageType type;
    pgno_t pgno;
    uint32_t checksum;
    uint64_t firstLsn; // LSN of first record started on page
    uint16_t numRecs; // number of log records started on page
    uint16_t firstPos; // position of first log started on page
    uint16_t lastPos; // position after last log record ended on page
};

#pragma pack(push)
#pragma pack(1)

struct ZeroPage {
    DbPageHeader hdr;
    Guid signature;
    uint32_t walPageSize;
    uint32_t dataPageSize;
};

struct MinimumPage {
    LogPageType type;
    pgno_t pgno;
};

struct PageHeaderRawV2 {
    LogPageType type;
    pgno_t pgno;
    uint32_t checksum;
    uint64_t firstLsn;
    uint16_t numRecs;
    uint16_t firstPos;
    uint16_t lastPos;
};

// deprecated 2018-03-23
struct PageHeaderRawV1 {
    LogPageType type;
    pgno_t pgno;
    uint64_t firstLsn;
    uint16_t numRecs;
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
static TaskQueueHandle walQueue() {
    static TaskQueueHandle s_hq = taskCreateQueue("Wal IO", 2);
    return s_hq;
}

//===========================================================================
static void pack(void * ptr, const LogPage & lp, uint32_t checksum) {
    auto mp = (MinimumPage *) ptr;
    mp->type = lp.type;
    mp->pgno = lp.pgno;
    auto v1 = (PageHeaderRawV1 *) ptr;
    auto v2 = (PageHeaderRawV2 *) ptr;
    switch (lp.type) {
    case LogPageType::kFree:
        break;
    case LogPageType::kLog:
        assert(v2->type == lp.type);
        v2->checksum = checksum;
        v2->firstLsn = lp.firstLsn;
        v2->numRecs = lp.numRecs;
        v2->firstPos = lp.firstPos;
        v2->lastPos = lp.lastPos;
        break;
    case LogPageType::kLogV1:
        assert(v1->type == lp.type);
        v1->firstLsn = lp.firstLsn;
        v1->numRecs = lp.numRecs;
        v1->firstPos = lp.firstPos;
        v1->lastPos = lp.lastPos;
        break;
    default:
        logMsgFatal() << "pack wal page " << lp.pgno
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
    case LogPageType::kFree:
        out->checksum = 0;
        out->firstLsn = 0;
        out->numRecs = 0;
        out->firstPos = 0;
        out->lastPos = 0;
        break;
    case LogPageType::kLog:
        assert(mp->type == v2->type);
        out->checksum = v2->checksum;
        out->firstLsn = v2->firstLsn;
        out->numRecs = v2->numRecs;
        out->firstPos = v2->firstPos;
        out->lastPos = v2->lastPos;
        break;
    case LogPageType::kLogV1:
        assert(mp->type == v1->type);
        out->checksum = 0;
        out->firstLsn = v1->firstLsn;
        out->numRecs = v1->numRecs;
        out->firstPos = v1->firstPos;
        out->lastPos = v1->lastPos;
        break;
    default:
        logMsgFatal() << "unpack wal page " << mp->pgno
            << ", unknown type: " << mp->type;
        break;
    }
}

//===========================================================================
static size_t walHdrLen(LogPageType type) {
    switch (type) {
    case LogPageType::kLog:
        return sizeof(PageHeaderRawV2);
    case LogPageType::kLogV1:
        return sizeof(PageHeaderRawV1);
    default:
        logMsgFatal() << "walHdrLen, unknown page type: " << type;
        return 0;
    }
}


/****************************************************************************
*
*   DbWal::LsnTaskInfo
*
***/

//===========================================================================
bool DbWal::LsnTaskInfo::operator<(const LsnTaskInfo & right) const {
    return waitLsn < right.waitLsn;
}

//===========================================================================
bool DbWal::LsnTaskInfo::operator>(const LsnTaskInfo & right) const {
    return waitLsn > right.waitLsn;
}


/****************************************************************************
*
*   DbWal
*
***/

//===========================================================================
DbWal::DbWal(IApplyNotify * data, IPageNotify * page)
    : m_data(data)
    , m_page(page)
    , m_checkpointTimer([&](auto){ checkpoint(); return kTimerInfinite; })
    , m_checkpointPagesTask([&]{ checkpointPages(); })
    , m_checkpointDurableTask([&]{ checkpointDurable(); })
    , m_flushTimer([&](auto){ flushWriteBuffer(); return kTimerInfinite; })
{}

//===========================================================================
DbWal::~DbWal() {
    if (m_fwal)
        fileClose(m_fwal);
    if (m_buffers)
        freeAligned(m_buffers);
    if (m_partialBuffers)
        freeAligned(m_partialBuffers);
}

//===========================================================================
char * DbWal::bufPtr(size_t ibuf) {
    assert(ibuf < m_numBufs);
    return m_buffers + ibuf * m_pageSize;
}

//===========================================================================
char * DbWal::partialPtr(size_t ibuf) {
    assert(ibuf < m_numBufs);
    return m_partialBuffers + ibuf * m_pageSize;
}

//===========================================================================
static FileHandle openWalFile(
    string_view fname,
    EnumFlags<DbOpenFlags> flags,
    bool align
) {
    using enum File::OpenMode;
    EnumFlags oflags = fDenyWrite;
    if (align)
        oflags |= fAligned;
    if (flags.any(fDbOpenReadOnly)) {
        oflags |= fReadOnly;
    } else {
        oflags |= fReadWrite;
    }
    if (flags.any(fDbOpenCreat))
        oflags |= fCreat;
    if (flags.any(fDbOpenTrunc))
        oflags |= fTrunc;
    if (flags.any(fDbOpenExcl))
        oflags |= fExcl;
    FileHandle f;
    fileOpen(&f, fname, oflags);
    if (!f)
        logMsgError() << "Open failed, " << fname;
    return f;
}

//===========================================================================
bool DbWal::open(
    string_view fname,
    EnumFlags<DbOpenFlags> flags,
    size_t dataPageSize
) {
    assert(!m_closing && !m_fwal);
    if (dataPageSize) {
        assert(dataPageSize == bit_ceil(dataPageSize));
        assert(dataPageSize >= kMinPageSize);
    }

    m_openFlags = flags;
    m_fwal = openWalFile(fname, flags, true);
    if (!m_fwal)
        return false;
    FileAlignment walAlign;
    if (auto ec = fileAlignment(&walAlign, m_fwal); ec)
        return false;
    auto fps = walAlign.physicalSector;
    assert(fps > sizeof ZeroPage);
    uint64_t len;
    fileSize(&len, m_fwal);
    ZeroPage zp{};
    if (!len) {
        m_dataPageSize = dataPageSize ? dataPageSize : kDefaultPageSize;
        m_pageSize = max<size_t>(2 * m_dataPageSize, fps);
    } else {
        auto rawbuf = mallocAligned(fps, fps);
        assert(rawbuf);
        fileReadWait(nullptr, rawbuf, fps, m_fwal, 0);
        memcpy(&zp, rawbuf, sizeof zp);
        m_dataPageSize = zp.dataPageSize;
        m_pageSize = zp.walPageSize;
        freeAligned(rawbuf);
    }
    if (m_pageSize < fps) {
        // Page size is smaller than minimum required for aligned access.
        // Reopen unaligned.
        fileClose(m_fwal);
        m_fwal = openWalFile(fname, flags, false);
    }

    m_numBufs = kLogWriteBuffers;
    m_bufStates.resize(m_numBufs, Buffer::kEmpty);
    m_emptyBufs = m_numBufs;
    m_buffers = (char *) mallocAligned(m_pageSize, m_numBufs * m_pageSize);
    assert(m_buffers);
    memset(m_buffers, 0, m_numBufs * m_pageSize);
    m_partialBuffers = (char *) mallocAligned(
        m_pageSize,
        m_numBufs * m_pageSize
    );
    assert(m_partialBuffers);
    memset(m_partialBuffers, 0, m_numBufs * m_pageSize);
    m_curBuf = 0;
    for (unsigned i = 0; i < m_numBufs; ++i) {
        auto mp = (MinimumPage *) bufPtr(i);
        mp->type = LogPageType::kFree;
    }
    m_bufPos = m_pageSize;

    if (!len) {
        m_phase = Checkpoint::kComplete;
        m_newFiles = true;

        zp.hdr.type = (DbPageType) LogPageType::kZero;
        zp.signature = kLogFileSig;
        zp.walPageSize = (unsigned) m_pageSize;
        zp.dataPageSize = (unsigned) m_dataPageSize;
        zp.hdr.checksum = 0;
        auto nraw = partialPtr(0);
        memcpy(nraw, &zp, sizeof(zp));
        zp.hdr.checksum = hash_crc32c(nraw, m_pageSize);
        memcpy(nraw, &zp, sizeof(zp));
        fileWriteWait(nullptr, m_fwal, 0, nraw, m_pageSize);
        s_perfWrites += 1;
        m_numPages = 1;
        s_perfPages += (unsigned) m_numPages;
        m_lastLsn = 0;
        m_localTxns.clear();

        // Add "previous" checkpoint to newly created WAL file. At least one
        // checkpoint must always exist in the WAL.
        m_checkpointLsn = m_lastLsn + 1;
        walCheckpoint(m_checkpointLsn);
        return true;
    }

    if (zp.hdr.type != (DbPageType) LogPageType::kZero) {
        logMsgError() << "Unknown wal file type, " << fname;
        return false;
    }
    if (zp.signature != kLogFileSig) {
        logMsgError() << "Bad signature, " << fname;
        return false;
    }
    if (zp.walPageSize != m_pageSize) {
        logMsgError() << "Mismatched page size, " << fname;
        return false;
    }

    m_numPages = (len + m_pageSize - 1) / m_pageSize;
    s_perfPages += (unsigned) m_numPages;
    return true;
}

//===========================================================================
void DbWal::close() {
    if (!m_fwal)
        return;

    m_closing = true;
    if (m_phase == Checkpoint::kStartRecovery
        || m_openFlags.any(fDbOpenReadOnly)
    ) {
        fileClose(m_fwal);
        m_fwal = {};
        return;
    }

    if (m_numBufs) {
        checkpoint();
        flushWriteBuffer();
    }
    unique_lock lk{m_bufMut};
    for (;;) {
        if (m_phase == Checkpoint::kComplete) {
            if (m_emptyBufs == m_numBufs)
                break;
            auto bst = m_bufStates[m_curBuf];
            if (m_emptyBufs == m_numBufs - 1 && bst == Buffer::kPartialClean)
                break;
        }
        m_bufAvailCv.wait(lk);
    }
    lk.unlock();
    s_perfPages -= (unsigned) m_numPages;
    s_perfFreePages -= (unsigned) m_freePages.size();

    // Look for free pages at the end of the file, and if there are any
    // resize the file to get rid of them.
    auto lastPage = (pgno_t) m_numPages - 1;
    if (auto i = m_freePages.find(lastPage)) {
        i = i.firstContiguous();
        lastPage = *i - 1;
        fileResize(m_fwal, (lastPage + 1) * m_pageSize);
    }
    fileClose(m_fwal);
    m_fwal = {};
}

//===========================================================================
DbConfig DbWal::configure(const DbConfig & conf) {
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
void DbWal::blockCheckpoint(IDbProgressNotify * notify, bool enable) {
    if (enable) {
        // Add the block
        DbProgressInfo info = {};
        m_checkpointBlockers.push_back(notify);
        if (m_phase == Checkpoint::kComplete) {
            notify->onDbProgress(kRunStopped, info);
        } else {
            notify->onDbProgress(kRunStopping, info);
        }
        return;
    }

    // Remove the block
    erase(m_checkpointBlockers, notify);
    if (m_checkpointBlockers.empty() && m_phase == Checkpoint::kComplete)
        checkpointWaitForNext();
}


/****************************************************************************
*
*   DbWal - recovery
*
***/

//===========================================================================
bool DbWal::recover(EnumFlags<RecoverFlags> flags) {
    if (m_phase != Checkpoint::kStartRecovery)
        return true;

    m_phase = Checkpoint::kComplete;
    m_checkpointStart = timeNow();

    using enum File::OpenMode;
    FileHandle fwal;
    auto walfile = filePath(m_fwal);
    auto ec = fileOpen(
        &fwal,
        walfile,
        fReadOnly | fBlocking | fDenyNone | fSequential
    );
    if (ec) {
        logMsgError() << "Open failed, " << walfile;
        return false;
    }
    Finally fwalFin([&]() { fileClose(fwal); });

    if (!loadPages(fwal))
        return false;
    if (m_pages.empty())
        return true;

    // Go through wal entries looking for last committed checkpoint and the set
    // of incomplete transactions that were still uncommitted when the after
    // the end of avail WAL (so we can avoid trying to redo them later).
    if (m_openFlags.any(fDbOpenVerbose))
        logMsgInfo() << "Analyze database";
    m_checkpointLsn = m_pages.front().firstLsn;
    AnalyzeData data;
    if (flags.none(fRecoverBeforeCheckpoint)) {
        // Analyze data to find the last committed checkpoint and the
        // incomplete transactions that begin after it but never committed.
        data.analyze = true;
        applyAll(&data, fwal);
        if (!data.checkpoint)
            logMsgFatal() << "Invalid .tsl file, no checkpoint found";
        m_checkpointLsn = data.checkpoint;
    }

    if (flags.any(fRecoverIncompleteTxns)) {
        // Since processing incomplete transactions was requested, empty the
        // list that would be used to skip them.
        data.incompleteTxnLsns.clear();
    } else if (!data.incompleteTxnLsns.empty() || !data.txns.empty()) {
        // Add transactions that are still uncommitted at the end of WAL to the
        // already collected list of those that were orphaned (ids reused while
        // uncommitted).
        for (auto&& kv : data.txns) {
            data.incompleteTxnLsns.push_back(kv.second);
        }
        sort(
            data.incompleteTxnLsns.begin(),
            data.incompleteTxnLsns.end(),
            [](auto & a, auto & b) { return a > b; }
        );
        // Remove all incomplete transactions from before the checkpoint, they
        // won't be encountered when the WAL is applied to the database -
        // because the replay starts at the checkpoint.
        auto i = lower_bound(
            data.incompleteTxnLsns.begin(),
            data.incompleteTxnLsns.end(),
            data.checkpoint
        );
        // FIXME: Should this be erase(i, end) instead?! It appears to be
        // removing all but the ones from before the checkpoint that we don't
        // care about...
        data.incompleteTxnLsns.erase(data.incompleteTxnLsns.begin(), i);
    }

    // Go through wal entries starting with the last committed checkpoint and
    // redo all transactions that begin after the checkpoint and commit before
    // the end of the WAL.
    if (m_openFlags.any(fDbOpenVerbose))
        logMsgInfo() << "Recover database";
    data.analyze = false;
    applyAll(&data, fwal);
    if (flags.none(fRecoverIncompleteTxns)) {
        assert(data.incompleteTxnLsns.empty());
        assert(!data.activeTxns);
    }

    auto & back = m_pages.back();
    m_durableLsn = back.firstLsn + back.numRecs - 1;
    m_lastLsn = m_durableLsn;
    m_page->onWalDurable(m_durableLsn, 0);
    return true;
}

//===========================================================================
// Creates array of references to last page and its contiguous predecessors.
bool DbWal::loadPages(FileHandle fwal) {
    if (m_openFlags.any(fDbOpenVerbose))
        logMsgInfo() << "Verify transaction WAL (write-ahead log)";

    auto rawbuf = partialPtr(0);
    LogPage lp;
    PageInfo * pi;
    uint32_t checksum;
    // Load info for each page.
    for (auto i = (pgno_t) 1; i < m_numPages; i = pgno_t(i + 1)) {
        fileReadWait(nullptr, rawbuf, m_pageSize, fwal, i * m_pageSize);
        auto mp = (MinimumPage *) rawbuf;
        switch (mp->type) {
        case LogPageType::kInvalid:
            // No page type, skip the rest of the file under the assumption
            // that nothing was ever written to this and the following pages.
            i = (pgno_t) m_numPages;
            break;
        case LogPageType::kLogV1:
            unpack(&lp, rawbuf);
            pi = &m_pages.emplace_back();
            pi->pgno = lp.pgno;
            pi->firstLsn = lp.firstLsn;
            pi->numRecs = lp.numRecs;
            break;
        case LogPageType::kLog:
            unpack(&lp, rawbuf);
            pack(rawbuf, lp, 0);
            checksum = hash_crc32c(rawbuf, m_pageSize);
            if (checksum != lp.checksum) {
                logMsgError() << "Invalid checksum on page #"
                    << i << " of " << filePath(fwal);
                goto MAKE_FREE;
            }
            pi = &m_pages.emplace_back();
            pi->pgno = lp.pgno;
            pi->firstLsn = lp.firstLsn;
            pi->numRecs = lp.numRecs;
            break;
        default:
            logMsgError() << "Invalid page type(" << mp->type << ") on page #"
                << i << " of " << filePath(fwal);
        MAKE_FREE:
            mp->type = LogPageType::kFree;
            mp->pgno = i;
            [[fallthrough]];
        case LogPageType::kFree:
            m_freePages.insert(mp->pgno);
            s_perfFreePages += 1;
            break;
        }
    }
    if (m_pages.empty())
        return true;

    // Find the set of pages spanned by contiguous wal records that includes
    // the record with the single largest LSN. These pages contain the last
    // checkpoint and following records that need to be replayed to recover the
    // database. Free all other pages, they are indeterminate or from previous
    // checkpoints.

    // Sort pages into LSN order, largest at the end.
    auto first = m_pages.begin();
    sort(first, m_pages.end());
    // Search from largest to smallest for first page without a contiguous LSN.
    auto rlast = adjacent_find(
        m_pages.rbegin(),
        m_pages.rend(),
        [](auto & a, auto & b){ return a.firstLsn != b.firstLsn + b.numRecs; }
    );
    if (rlast != m_pages.rend()) {
        // There are old pages not in the contiguous set, free them.
        auto oldPages = ranges::subrange(first, rlast.base() - 1);
        for (auto&& pi : oldPages)
            m_freePages.insert(pi.pgno);
        s_perfFreePages += (unsigned) size(oldPages);
        m_pages.erase(oldPages.begin(), oldPages.end());
    }
    return true;
}

//===========================================================================
void DbWal::applyAll(AnalyzeData * data, FileHandle fwal) {
    LogPage lp;

    // Buffers are twice the page size so that a single page sized record
    // almost entirely on the next page can be dealt with contiguously.
    //
    // NOTE: WAL records may not span three pages. In other words, individual
    //       records must be less than or equal to page size in length.
    auto curBuf = (char *) mallocAligned(m_pageSize, 2 * m_pageSize);
    auto nextBuf = (char *) mallocAligned(m_pageSize, 2 * m_pageSize);

    auto finally = Finally([&] { freeAligned(curBuf); freeAligned(nextBuf); });
    int bytesBefore = 0;
    int walPos = 0;
    auto lsn = uint64_t{0};
    auto rec = (Record *) nullptr;

    for (auto&& pi : m_pages) {
        fileReadWait(nullptr, nextBuf, m_pageSize, fwal, pi.pgno * m_pageSize);
        unpack(&lp, nextBuf);
        if (bytesBefore) {
            // When a WAL record spans pages some bytes of that record are on
            // the current page (bytesBefore), and some are on the next page
            // (bytesAfter).
            //
            // Copy the after bytes to the end of the current buffer to form a
            // contiguous WAL record that we then apply.
            auto bytesAfter = lp.firstPos - walHdrLen(lp.type);
            memcpy(
                curBuf + m_pageSize,
                nextBuf + walHdrLen(lp.type),
                bytesAfter
            );
            rec = (Record *) (curBuf + m_pageSize - bytesBefore);
            assert(getSize(*rec) == bytesBefore + bytesAfter);
            apply(data, lp.firstLsn - 1, *rec);
        }
        // Now that we're done with the current buffer, the next buffer becomes
        // the new current.
        swap(curBuf, nextBuf);

        // Apply WAL records fully contained in the current buffer.
        walPos = lp.firstPos;
        lsn = lp.firstLsn;
        while (walPos < lp.lastPos) {
            rec = (Record *) (curBuf + walPos);
            apply(data, lsn, *rec);
            walPos += getSize(*rec);
            lsn += 1;
        }
        assert(walPos == lp.lastPos);

        // Save size of the fragment of the record at the end of this page so
        // it can be combined with the rest of the record at the beginning of
        // the next page.
        bytesBefore = (int) (m_pageSize - walPos);
    }

    // Initialize wal write buffers with the contents of the last buffer (if
    // partial) found during analyze.
    if (data->analyze && walPos < m_pageSize) {
        memcpy(m_buffers, curBuf, walPos);
        m_bufPos = walPos;
        m_bufStates[m_curBuf] = Buffer::kPartialClean;
        m_emptyBufs -= 1;
        auto & pi = m_pages.back();
        unpack(&lp, bufPtr(m_curBuf));
        assert(lp.firstLsn == pi.firstLsn);
        pi.commitTxns.emplace_back(lp.firstLsn, 0);
    }
}

//===========================================================================
void DbWal::applyCheckpoint(
    AnalyzeData * data,
    uint64_t lsn,
    uint64_t startLsn
) {
    if (data->analyze) {
        if (startLsn >= m_checkpointLsn)
            data->checkpoint = startLsn;
        return;
    }

    //-----------------------------------------------------------------------
    // redo
    if (lsn < data->checkpoint)
        return;
    m_data->onWalApplyCheckpoint(lsn, startLsn);
}

//===========================================================================
void DbWal::applyBeginTxn(
    AnalyzeData * data,
    uint64_t lsn,
    uint16_t localTxn
) {
    if (data->analyze) {
        auto & txnLsn = data->txns[localTxn];
        if (txnLsn) {
            // Add beginning LSN of transactions that have had their id reused
            // to begin a new tranaction, preventing them from ever getting
            // associated with a commit.
            //
            // Uncommitted tranactions left over from an abortive shutdown are
            // detected and skipped by recovery but then ignored. Normal
            // operation then creates new transactions, eventually reusing the
            // id. Which leaves this situation until the next checkpoint frees
            // these wal records. Or for the next recovery, if it's before that
            // checkpoint.
            data->incompleteTxnLsns.push_back(txnLsn);
        }
        txnLsn = lsn;
        return;
    }

    //-----------------------------------------------------------------------
    // redo
    if (lsn < data->checkpoint)
        return;

    // The incompleteTxnLsns are in descending order and the WAL is processed in
    // ascending order. So if the current LSN matches that last incompletes to
    // be skipped, remove it from the list and return.
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
    m_data->onWalApplyBeginTxn(lsn, localTxn);
}

//===========================================================================
void DbWal::applyCommitTxn(
    AnalyzeData * data,
    uint64_t lsn,
    uint16_t localTxn
) {
    if (data->analyze) {
        data->txns.erase(localTxn);
        return;
    }

    //-----------------------------------------------------------------------
    // redo
    if (lsn < data->checkpoint)
        return;

    if (!data->activeTxns.erase(localTxn)) {
        // Commits for transaction ids with no preceding begin are allowed and
        // ignored under the assumption that they are the previously played
        // continuations of transactions that begin before the start of this
        // recovery.
        //
        // With some extra tracking, the rule that every commit of an id after
        // the first must have a matching begin could be enforced.
    }
    m_data->onWalApplyCommitTxn(lsn, localTxn);
}

//===========================================================================
void DbWal::applyUpdate(
    AnalyzeData * data,
    uint64_t lsn,
    const Record & rec
) {
    if (data->analyze)
        return;

    //-----------------------------------------------------------------------
    // redo
    if (lsn < data->checkpoint)
        return;

    auto localTxn = getLocalTxn(rec);
    if (localTxn && !data->activeTxns.contains(localTxn)) {
        // The id is not in the active list, so it must belong to one of
        // the incomplete transactions that are being skipped.
        return;
    }

    auto pgno = getPgno(rec);
    if (auto ptr = m_page->onWalGetPtrForRedo(pgno, lsn, localTxn))
        applyUpdate(ptr, lsn, rec);
}


/****************************************************************************
*
*   DbWal - checkpoint
*
***/

//===========================================================================
// Checkpointing places a marker in the wal to indicate the start of entries
// that are needed to fully recover the database. Any entries before that point
// will be skipped by recovery and eventually discarded from the WAL.
void DbWal::checkpoint() {
    if (m_phase != Checkpoint::kComplete
        || !m_checkpointBlockers.empty()
        || m_openFlags.any(fDbOpenReadOnly)
    ) {
        // A checkpoint is already in progress, not allowed now (blocked), or
        // not allowed at all (readonly database).
        return;
    }

    if (m_openFlags.any(fDbOpenVerbose))
        logMsgInfo() << "Checkpoint started";
    m_checkpointStart = timeNow();
    m_checkpointData = 0;
    m_phase = Checkpoint::kFlushPages;
    s_perfCps += 1;
    s_perfCurCps += 1;
    taskPushCompute(&m_checkpointPagesTask);
}

//===========================================================================
void DbWal::checkpointPages() {
    assert(m_phase == Checkpoint::kFlushPages);
    // Get oldest LSN that has dirty data pages associated (also flushes OS
    // cache of any dirty data pages).
    auto nextCp = m_page->onWalCheckpointPages(m_checkpointLsn);
    if (nextCp == m_checkpointLsn) {
        // No additional WAL pages have become discardable since the last
        // checkpoint, so there's no need for a new checkpoint. WAL is already
        // as truncated as possible.
        m_phase = Checkpoint::kTruncateWal;
        checkpointWalTruncated();
        return;
    }
    m_checkpointLsn = nextCp;

    // Write the checkpoint record and queue a checkpointDurable() call for
    // when it's written.
    walCheckpoint(m_checkpointLsn);
    m_phase = Checkpoint::kFlushCheckpoint;
    queueTask(&m_checkpointDurableTask, m_lastLsn);
    flushWriteBuffer();
}

//===========================================================================
void DbWal::checkpointDurable() {
    assert(m_phase == Checkpoint::kFlushCheckpoint);
    // Flush any metadata (timestamps, file attributes, etc) changes to WAL.
    // The WAL pages themselves are already written with OS buffering disabled.
    if (auto ec = fileFlush(m_fwal))
        logMsgFatal() << "Checkpointing failed.";

    auto lastPgno = pgno_t{0}; // Page that most recently became discardable.
    {
        // Remove discardable pages from the info list and add their pgnos to
        // the free list.
        unique_lock lk{m_bufMut};
        auto lastLsn = m_pages.back().firstLsn;
        auto before = m_pages.size();
        for (;;) {
            auto && pi = m_pages.front();
            if (pi.firstLsn == lastLsn)
                break;
            if (pi.firstLsn + pi.numRecs > m_checkpointLsn)
                break;
            if (lastPgno)
                m_freePages.insert(lastPgno);
            lastPgno = pi.pgno;
            m_pages.pop_front();
        }
        s_perfFreePages +=
            (unsigned) (before - m_pages.size() - (bool) lastPgno);
    }

    m_phase = Checkpoint::kTruncateWal;
    if (!lastPgno) {
        checkpointWalTruncated();
    } else {
        // Mark truncation in WAL file by explicitly setting the most recently
        // discardable page to free. This is not required for correctness, but
        // can be useful for debugging.
        //
        // The call to checkpointWalTruncated() is made by the onFileWrite()
        // callback after the write.
        auto vptr = mallocAligned(m_pageSize, m_pageSize);
        auto mp = new(vptr) MinimumPage {
            .type = LogPageType::kFree,
            .pgno = lastPgno
        };
        fileWrite(
            this,
            m_fwal,
            lastPgno * m_pageSize,
            mp,
            m_pageSize,
            walQueue()
        );
    }
}

//===========================================================================
void DbWal::checkpointWalTruncated() {
    // Set checkpoint status to complete, notify things that were waiting, and
    // maybe schedule the next checkpoint.
    assert(m_phase == Checkpoint::kTruncateWal);
    if (m_openFlags.any(fDbOpenVerbose))
        logMsgInfo() << "Checkpoint completed";
    m_phase = Checkpoint::kComplete;
    s_perfCurCps -= 1;
    if (m_checkpointBlockers.empty()) {
        checkpointWaitForNext();
    } else {
        DbProgressInfo info = {};
        for (auto&& blocker : m_checkpointBlockers)
            blocker->onDbProgress(kRunStopped, info);
    }
    m_bufAvailCv.notify_one();
}

//===========================================================================
void DbWal::checkpointWaitForNext() {
    if (!m_closing) {
        Duration wait = 0ms;
        if (m_checkpointData >= m_maxCheckpointData) {
            // Do it now.
        } else {
            auto elapsed = timeNow() - m_checkpointStart;
            if (elapsed < m_maxCheckpointInterval) {
                // Wait for interval to expire.
                wait = m_maxCheckpointInterval - elapsed;
            }
        }
        timerUpdate(&m_checkpointTimer, wait);
    }
}


/****************************************************************************
*
*   DbWal - write-ahead logging
*
***/

//===========================================================================
// The local transaction id is given the lowest available value in the range of
// 1 to 65534 that isn't already assigned to an active transaction.
uint64_t DbWal::beginTxn() {
    uint16_t localTxn = 1;
    {
        scoped_lock lk{m_bufMut};
        if (!m_localTxns) {
            // There are no txns, so go ahead and use 1.
        } else {
            auto first = m_localTxns.lowerBound(1);
            if (*first > 1) {
                // No txns with id of 1, so go ahead and use it.
            } else {
                // Find the first available value greater than 1.
                localTxn = (uint16_t) *m_localTxns.lastContiguous(first) + 1;
                if (localTxn == numeric_limits<uint16_t>::max())
                    logMsgFatal() << "Too many concurrent transactions";
            }
        }
        m_localTxns.insert(localTxn);
    }

    s_perfCurTxns += 1;
    s_perfVolatileTxns += 1;
    return walBeginTxn(localTxn);
}

//===========================================================================
void DbWal::commit(uint64_t txn) {
    walCommitTxn(txn);
    s_perfCurTxns -= 1;

    auto localTxn = getLocalTxn(txn);
    scoped_lock lk{m_bufMut};
    [[maybe_unused]] auto found = m_localTxns.erase(localTxn);
    assert(found && "Commit of unknown transaction");
}

//===========================================================================
uint64_t DbWal::wal(
    const Record & rec,
    size_t bytes,
    TxnMode txnMode,
    uint64_t txn
) {
    assert(bytes < m_pageSize - kMaxHdrLen);
    assert(bytes == getSize(rec));

    unique_lock lk{m_bufMut};
    while (m_bufPos + bytes > m_pageSize && !m_emptyBufs)
        m_bufAvailCv.wait(lk);
    auto lsn = ++m_lastLsn;

    // Count transaction beginnings on the page their wal record started. This
    // means the current page before logging (since logging can advance to the
    // next page), UNLESS it's exactly at the end of the page. In that case the
    // transaction actually starts on the next page, which is where we'll be
    // after logging.
    //
    // Transaction commits are counted after logging, so it's always on the
    // page where they finished.
    if (m_bufPos == m_pageSize) {
        prepareBuffer_LK(rec, 0, bytes);
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
    memcpy(base, &rec, bytes);
    m_bufPos += bytes;

    if (m_bufPos != m_pageSize) {
        if (m_bufStates[m_curBuf] == Buffer::kPartialClean
            || m_bufStates[m_curBuf] == Buffer::kEmpty
        ) {
            m_bufStates[m_curBuf] = Buffer::kPartialDirty;
            timerUpdate(&m_flushTimer, kDirtyWriteBufferTimeout);
        }
        if (txnMode == TxnMode::kCommit)
            countCommitTxn_LK(txn);
        return lsn;
    }

    bool writeInProgress = m_bufStates[m_curBuf] == Buffer::kPartialWriting;
    m_bufStates[m_curBuf] = Buffer::kFullWriting;
    LogPage lp;
    auto rawbuf = bufPtr(m_curBuf);
    unpack(&lp, rawbuf);
    lp.numRecs = (uint16_t) (m_lastLsn - lp.firstLsn + 1);
    lp.lastPos = (uint16_t) m_bufPos;
    if (overflow)
        lp.lastPos -= (uint16_t) bytes;
    pack(rawbuf, lp, 0);

    if (overflow)
        prepareBuffer_LK(rec, bytes, overflow);
    if (txnMode == TxnMode::kCommit)
        countCommitTxn_LK(txn);

    lk.unlock();
    if (!writeInProgress) {
        pack(rawbuf, lp, hash_crc32c(rawbuf, m_pageSize));
        auto offset = lp.pgno * m_pageSize;
        fileWrite(this, m_fwal, offset, rawbuf, m_pageSize, walQueue());
    }
    return lsn;
}

//===========================================================================
void DbWal::queueTask(
    ITaskNotify * task,
    uint64_t waitLsn,
    TaskQueueHandle hq
) {
    if (!hq)
        hq = taskComputeQueue();
    unique_lock lk{m_bufMut};
    if (m_durableLsn >= waitLsn) {
        taskPush(hq, task);
    } else {
        auto ti = LsnTaskInfo{task, waitLsn, hq};
        m_lsnTasks.push(ti);
    }
}

//===========================================================================
void DbWal::flushWriteBuffer() {
    unique_lock lk{m_bufMut};
    if (m_bufStates[m_curBuf] != Buffer::kPartialDirty)
        return;

    m_bufStates[m_curBuf] = Buffer::kPartialWriting;
    LogPage lp;
    auto rawbuf = bufPtr(m_curBuf);
    unpack(&lp, rawbuf);
    lp.numRecs = (uint16_t) (m_lastLsn - lp.firstLsn + 1);
    lp.lastPos = (uint16_t) m_bufPos;
    pack(rawbuf, lp, 0);
    auto offset = lp.pgno * m_pageSize;

    // Write the entire page, not just the changed part, otherwise the
    // resulting page might not match the checksum.
    auto nraw = partialPtr(m_curBuf);
    memcpy(nraw, rawbuf, m_pageSize);

    lk.unlock();
    if (lp.type != LogPageType::kFree) {
        assert(lp.type == LogPageType::kLog || lp.type == LogPageType::kLogV1);
        pack(nraw, lp, hash_crc32c(nraw, m_pageSize));
    }
    fileWrite(this, m_fwal, offset, nraw, m_pageSize, walQueue());
}

//===========================================================================
void DbWal::updatePages_LK(const PageInfo & pi, bool fullPageWrite) {
    auto i = lower_bound(m_pages.begin(), m_pages.end(), pi);
    assert(i != m_pages.end() && i->firstLsn == pi.firstLsn);
    i->numRecs = pi.numRecs;

    auto base = i + 1;
    for (auto&& [lsn, txns] : i->commitTxns) {
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

    if (base->firstLsn > m_durableLsn + 1) {
        s_perfReorderedWrites += 1;
        return;
    }

    uint64_t last = 0;
    for (i = base; i != m_pages.end(); ++i) {
        auto & npi = *i;
        if (npi.activeTxns || !npi.numRecs)
            break;
        if (!npi.commitTxns.empty()) {
            if (npi.commitTxns.size() != 1 || npi.commitTxns[0].second)
                break;
            assert(npi.firstLsn == npi.commitTxns[0].first);
        }
        if (!npi.numRecs) {
            // The only page that can have no records on it is a very last page
            // that timed out waiting for more records with just the second
            // half of the last wal record started on the previous page.
            assert(i + 1 == m_pages.end());
            continue;
        }
        last = npi.firstLsn + npi.numRecs - 1;
    }
    if (!last)
        return;

    // FIXME: It is somehow possible for this to trigger. It did once when
    // running "tst db" with last and m_durableLsn both equal to 4272. Examine
    // m_pages next time it happens!
    assert(last > m_durableLsn);

    m_durableLsn = last;
    m_page->onWalDurable(
        m_durableLsn,
        fullPageWrite ? m_pageSize * (i - base) : 0
    );
    while (!m_lsnTasks.empty()) {
        auto & ti = m_lsnTasks.top();
        if (m_durableLsn < ti.waitLsn)
            break;
        taskPush(ti.hq, ti.notify);
        m_lsnTasks.pop();
    }
}

//===========================================================================
void DbWal::onFileWrite(const FileWriteData & data) {
    if (data.written != data.data.size()) {
        logMsgFatal() << "Write to .tsl failed, " << errno << ", "
            << _doserrno;
    }

    auto rawbuf = (char *) data.data.data();
    s_perfWrites += 1;
    LogPage lp;
    unpack(&lp, rawbuf);
    PageInfo pi = { lp.pgno, lp.firstLsn, lp.numRecs };
    unique_lock lk{m_bufMut};
    if (lp.type == LogPageType::kFree) {
        m_freePages.insert(lp.pgno);
        s_perfFreePages += 1;
        lk.unlock();
        freeAligned(rawbuf);
        checkpointWalTruncated();
        return;
    }

    bool fullPageWrite = rawbuf >= m_buffers
        && rawbuf < m_buffers + m_numBufs * m_pageSize;
    updatePages_LK(pi, fullPageWrite);
    if (fullPageWrite) {
        assert(data.data.size() == m_pageSize);
        m_emptyBufs += 1;
        auto ibuf = (rawbuf - m_buffers) / m_pageSize;
        m_bufStates[ibuf] = Buffer::kEmpty;
        lp.type = LogPageType::kFree;
        pack(rawbuf, lp, lp.checksum);
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
    if (m_bufStates[ibuf] == Buffer::kPartialWriting) {
        if (olp.numRecs == lp.numRecs) {
            m_bufStates[ibuf] = Buffer::kPartialClean;
            lk.unlock();
            m_bufAvailCv.notify_one();
        } else {
            m_bufStates[ibuf] = Buffer::kPartialDirty;
            lk.unlock();
            timerUpdate(&m_flushTimer, kDirtyWriteBufferTimeout);
        }
    } else if (m_bufStates[ibuf] == Buffer::kFullWriting) {
        lk.unlock();
        pack(rawbuf, olp, hash_crc32c(rawbuf, m_pageSize));
        fileWrite(this, m_fwal, data.offset, rawbuf, m_pageSize, walQueue());
    }
}

//===========================================================================
void DbWal::prepareBuffer_LK(
    const Record & rec,
    size_t bytesOnOldPage,
    size_t bytesOnNewPage
) {
    assert(m_emptyBufs);
    for (;;) {
        if (++m_curBuf == m_numBufs)
            m_curBuf = 0;
        if (m_bufStates[m_curBuf] == Buffer::kEmpty)
            break;
    }

    LogPage lp;
    auto rawbuf = bufPtr(m_curBuf);
    lp.type = LogPageType::kLog;
    lp.checksum = 0;
    auto hdrLen = walHdrLen(lp.type);
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
    lp.numRecs = 0;
    lp.lastPos = 0;
    pack(rawbuf, lp, 0);

    auto & pi = m_pages.emplace_back(PageInfo{});
    pi.pgno = lp.pgno;
    pi.firstLsn = lp.firstLsn;
    pi.numRecs = 0;
    pi.commitTxns.emplace_back(lp.firstLsn, 0);

    m_bufStates[m_curBuf] = Buffer::kPartialDirty;
    m_emptyBufs -= 1;
    memcpy(
        rawbuf + hdrLen,
        (const char *) &rec + bytesOnOldPage,
        bytesOnNewPage
    );
    m_bufPos = hdrLen + bytesOnNewPage;

    timerUpdate(&m_flushTimer, kDirtyWriteBufferTimeout);
}

//===========================================================================
void DbWal::countBeginTxn_LK() {
    m_pages.back().activeTxns += 1;
}

//===========================================================================
void DbWal::countCommitTxn_LK(uint64_t txn) {
    auto lsn = getLsn(txn);
    auto & commitTxns = m_pages.back().commitTxns;
    for (auto&& lsn_txns : commitTxns) {
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
DbTxn::DbTxn(DbWal & wal, DbPage & work)
    : m_wal{wal}
    , m_page{work}
{}

//===========================================================================
DbTxn::~DbTxn() {
    if (m_txn)
        m_wal.commit(m_txn);
    m_page.unpin(m_pinnedPages);
}

//===========================================================================
void DbTxn::wal(DbWal::Record * rec, size_t bytes) {
    if (!m_txn)
        m_txn = m_wal.beginTxn();
    if constexpr (DIMAPP_LIB_BUILD_DEBUG) {
        auto pgno = m_wal.getPgno(*rec);
        m_pinnedPages.contains(pgno);
    }
    m_wal.walAndApply(m_txn, rec, bytes);
}

//===========================================================================
std::pair<void *, size_t> DbTxn::alloc(
    DbWalRecType type,
    pgno_t pgno,
    size_t bytes
) {
    if (!m_txn)
        m_txn = m_wal.beginTxn();
    m_buffer.resize(bytes);
    auto * lr = (DbWal::Record *) m_buffer.data();
    lr->type = type;
    lr->pgno = pgno;
    lr->localTxn = 0;
    return {m_buffer.data(), bytes};
}
