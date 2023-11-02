// Copyright Glen Knowles 2017 - 2023.
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

const unsigned kWalWriteBuffers = 10;
static_assert(kWalWriteBuffers > 1);


/****************************************************************************
*
*   Declarations
*
***/

enum class DbWal::Buffer : int {
    kEmpty,             // Buffer available for reuse.

    kPartialDirty,      // Buffer has data but not full, not yet written.
    kPartialWriting,    // Buffer has data but not full, is being written.
    kPartialClean,      // Buffer has data but not full, already written.

    // Buffer is full, and is being written or is queued to be written.
    kFullWriting,
};

enum class DbWal::Checkpoint : int {
    kStartRecovery,
    kComplete,
    kFlushPages,
    kFlushCheckpoint,
    kReportComplete,
};

struct DbWal::AnalyzeData {
    bool analyze = true;
    unordered_map<LocalTxn, Lsn> txns;
    vector<Lsn> incompleteTxnLsns;
    Lsn checkpoint = {};

    UnsignedSet activeTxns;
};

namespace {

const Guid kWalFileSig = "b45d8e5a-851d-42f5-ac31-9ca00158597b"_Guid;

enum class WalPageType {
    kInvalid = 0,
    kZero = 'lZ',
    kLog = '2l',
    kFree = 'F',

    // deprecated 2018-03-23
    kLogV1 = 'l',
};
ostream & operator<<(ostream & os, WalPageType type) {
    if ((unsigned) type > 0xff)
        os << (char) ((unsigned) type >> 8);
    os << (char) ((unsigned) type & 0xff);
    return os;
}

struct WalPage {
    WalPageType type;
    pgno_t pgno;
    uint32_t checksum;
    Lsn firstLsn; // LSN of first record started on page.
    uint16_t numRecs; // Number of WAL records started on page.
    uint16_t firstPos; // Position of first log started on page.
    uint16_t lastPos; // Position after last WAL record ended on page.
};

#pragma pack(push, 1)

struct ZeroPage {
    DbPageHeader hdr;
    Guid signature;
    uint32_t walPageSize;
    uint32_t dataPageSize;
};

struct MinimumPage {
    WalPageType type;
    pgno_t pgno;
};

struct PageHeaderRawV2 {
    WalPageType type;
    pgno_t pgno;
    uint32_t checksum;
    Lsn firstLsn;
    uint16_t numRecs;
    uint16_t firstPos;
    uint16_t lastPos;
};

// deprecated 2018-03-23
struct PageHeaderRawV1 {
    WalPageType type;
    pgno_t pgno;
    Lsn firstLsn;
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
    static TaskQueueHandle s_hq = taskCreateQueue("WAL IO", 2);
    return s_hq;
}

//===========================================================================
// Encode page info into the WAL page header.
static void pack(void * ptr, const WalPage & lp, uint32_t checksum) {
    auto mp = (MinimumPage *) ptr;
    mp->type = lp.type;
    mp->pgno = lp.pgno;
    auto v1 = (PageHeaderRawV1 *) ptr;
    auto v2 = (PageHeaderRawV2 *) ptr;
    switch (lp.type) {
    case WalPageType::kFree:
        break;
    case WalPageType::kLog:
        assert(v2->type == lp.type);
        v2->checksum = checksum;
        v2->firstLsn = lp.firstLsn;
        v2->numRecs = lp.numRecs;
        v2->firstPos = lp.firstPos;
        v2->lastPos = lp.lastPos;
        break;
    case WalPageType::kLogV1:
        assert(v1->type == lp.type);
        v1->firstLsn = lp.firstLsn;
        v1->numRecs = lp.numRecs;
        v1->firstPos = lp.firstPos;
        v1->lastPos = lp.lastPos;
        break;
    default:
        logMsgFatal() << "pack WAL page " << lp.pgno
            << ", unknown type: " << lp.type;
        break;
    }
}

//===========================================================================
// Decode page info from WAL page header.
static void unpack(WalPage * out, const void * ptr) {
    auto mp = (const MinimumPage *) ptr;
    out->type = mp->type;
    out->pgno = mp->pgno;
    auto v1 = (const PageHeaderRawV1 *) ptr;
    auto v2 = (const PageHeaderRawV2 *) ptr;
    switch (mp->type) {
    case WalPageType::kFree:
        out->checksum = 0;
        out->firstLsn = {};
        out->numRecs = 0;
        out->firstPos = 0;
        out->lastPos = 0;
        break;
    case WalPageType::kLog:
        assert(mp->type == v2->type);
        out->checksum = v2->checksum;
        out->firstLsn = v2->firstLsn;
        out->numRecs = v2->numRecs;
        out->firstPos = v2->firstPos;
        out->lastPos = v2->lastPos;
        break;
    case WalPageType::kLogV1:
        assert(mp->type == v1->type);
        out->checksum = 0;
        out->firstLsn = v1->firstLsn;
        out->numRecs = v1->numRecs;
        out->firstPos = v1->firstPos;
        out->lastPos = v1->lastPos;
        break;
    default:
        logMsgFatal() << "unpack WAL page " << mp->pgno
            << ", unknown type: " << mp->type;
        break;
    }
}

//===========================================================================
static size_t walHdrLen(WalPageType type) {
    switch (type) {
    case WalPageType::kLog:
        return sizeof(PageHeaderRawV2);
    case WalPageType::kLogV1:
        return sizeof(PageHeaderRawV1);
    default:
        logMsgFatal() << "walHdrLen, unknown page type: " << type;
        return 0;
    }
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
    , m_flushTimer([&](auto){ flushPartialBuffer(); return kTimerInfinite; })
{}

//===========================================================================
DbWal::~DbWal() {
    assert(m_checkpointBlockers.empty());
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
        oflags |= fCreat | fRemove;
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

    // If opened with exclusive create the file is obviously new, otherwise
    // assume it already existed until we know better.
    m_newFiles = m_openFlags.all(fDbOpenCreat | fDbOpenExcl);

    // Auto-close file on failure of initial processing of the opened file.
    Finally fin([&fh = m_fwal, &newf = m_newFiles]() {
        if (newf && fileMode(fh).any(File::OpenMode::fRemove)) {
            // File was created, but not completely. Remove the remnants.
            fileRemoveOnClose(fh);
        }
        fileClose(fh);
        fh = {};
    });

    uint64_t len;
    if (fileSize(&len, m_fwal))
        return false;
    if (!len) {
        // Newly file (created or truncated).
        m_newFiles = true;
    }

    FileAlignment walAlign;
    if (auto ec = fileAlignment(&walAlign, m_fwal); ec)
        return false;
    auto fps = walAlign.physicalSector;
    assert(fps > sizeof ZeroPage);
    ZeroPage zp{};
    if (!len) {
        // New file, use requested dataPageSize and physical sector size to
        // derive page size for WAL.
        m_dataPageSize = dataPageSize ? dataPageSize : kDefaultPageSize;
        m_pageSize = max<size_t>(2 * m_dataPageSize, fps);
    } else {
        // Existing file, use data and WAL page sizes written in the file.
        auto rawbuf = mallocAligned(fps, fps);
        assert(rawbuf);
        fileReadWait(nullptr, rawbuf, fps, m_fwal, 0);
        memcpy(&zp, rawbuf, sizeof zp);
        m_dataPageSize = zp.dataPageSize;
        m_pageSize = zp.walPageSize;
        freeAligned(rawbuf);
        if (m_pageSize < fps) {
            // Page size is smaller than minimum required for aligned access.
            // Reopen unaligned.
            fileClose(m_fwal);
            m_fwal = openWalFile(fname, flags, false);
        }
        if (zp.hdr.type != (DbPageType) WalPageType::kZero) {
            logMsgError() << "Unknown WAL file type, " << fname;
            return false;
        }
        if (zp.signature != kWalFileSig) {
            logMsgError() << "Bad signature, " << fname;
            return false;
        }
        if (zp.walPageSize != m_pageSize) {
            logMsgError() << "Mismatched page size, " << fname;
            return false;
        }
    }

    // No more open failures possible.
    fin.release();

    // Allocate Aligned Buffers
    m_numBufs = kWalWriteBuffers;
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
        mp->type = WalPageType::kFree;
    }
    // Set position within buffer to end of the buffer.
    m_bufPos = m_pageSize;

    m_phase = Checkpoint::kStartRecovery;
    m_maxCheckpointData = kDefaultMaxCheckpointData;
    m_maxCheckpointInterval = kDefaultMaxCheckpointInterval;
    m_checkpointBlockers.clear();
    m_lsnTasks = {};

    if (len) {
        // Existing File
        assert(!m_newFiles);
        m_numPages = (len + m_pageSize - 1) / m_pageSize;
        m_peakUsedPages = m_numPages;
        s_perfPages += (unsigned) m_numPages;
        return true;
    }

    // New File
    assert(m_newFiles);
    zp.hdr.type = (DbPageType) WalPageType::kZero;
    zp.signature = kWalFileSig;
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
    m_peakUsedPages = m_numPages;
    s_perfPages += (unsigned) m_numPages;

    // Initialize the variables normally set by the recovery phase that we're
    // skipping.
    m_localTxns.clear();
    m_lastLsn = {};
    m_freePages.clear();
    m_pages.clear();
    m_durableLsn = {};

    // Fabricate "previous" checkpoint to newly created WAL file. At least one
    // checkpoint must always exist in the WAL for recovery to orient itself
    // around.
    m_checkpointStart = timeNow();
    m_checkpointLsn = m_lastLsn + 1;
    walCheckpoint(m_checkpointLsn);
    return true;
}

//===========================================================================
void DbWal::close() {
    timerCloseWait(&m_flushTimer);
    timerCloseWait(&m_checkpointTimer);

    unique_lock lk{m_bufMut};
    if (!m_fwal)
        return;

    m_closing = true;
    if (m_phase == Checkpoint::kStartRecovery
        || m_openFlags.any(fDbOpenReadOnly)
    ) {
        if (m_newFiles && m_phase == Checkpoint::kStartRecovery)
            fileRemoveOnClose(m_fwal);
        fileClose(m_fwal);
        m_fwal = {};
        return;
    }

    if (m_numBufs) {
        lk.unlock();
        flushPartialBuffer();
        checkpoint();
        lk.lock();
    }

    // Wait for checkpointing to finish.
    while (m_phase != Checkpoint::kComplete)
        m_bufCheckpointCv.wait(lk);
    // Wait for buffer flush to finish.
    for (;;) {
        if (m_emptyBufs == m_numBufs)
            break;
        auto bst = m_bufStates[m_curBuf];
        if (m_emptyBufs == m_numBufs - 1 && bst == Buffer::kPartialClean)
            break;
        m_bufAvailCv.wait(lk);
    }

    s_perfPages -= (unsigned) m_numPages;
    s_perfFreePages -= (unsigned) m_freePages.size();
    fileClose(m_fwal);
    m_fwal = {};
}

//===========================================================================
// Set and return adjusted values for checkpoint max data and max interval.
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
// While registered, blockers prevent future checkpoints from starting. This
// enables consistent backups to be taken without the risk of WAL needed by
// a slightly older database getting purged.
void DbWal::blockCheckpoint(IDbProgressNotify * notify, bool enable) {
    unique_lock lkBlock{m_blockMut};
    unique_lock lk{m_bufMut};
    bool complete = m_phase == Checkpoint::kComplete;
    lk.unlock();

    if (enable) {
        // Add the block
        m_checkpointBlockers.push_back(notify);
        DbProgressInfo info = {};
        if (complete) {
            notify->onDbProgress(kRunStopped, info);
        } else {
            notify->onDbProgress(kRunStopping, info);
        }
        return;
    }

    // Remove the block
    erase(m_checkpointBlockers, notify);
    if (m_checkpointBlockers.empty() && complete) {
        lkBlock.unlock();
        checkpointQueueNext();
    }
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
    if (m_newFiles)
        return true;

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

    m_localTxns.clear();
    if (!loadPages(fwal))
        return false;
    if (m_pages.empty())
        return true;

    // Go through WAL entries looking for last committed checkpoint and the set
    // of incomplete transactions that were still uncommitted when the after
    // the end of avail WAL (so we can avoid trying to redo them later).
    if (m_openFlags.any(fDbOpenVerbose))
        logMsgInfo() << "Analyze database";
    m_checkpointLsn = m_pages.front().firstLsn;
    AnalyzeData data;
    if (!flags.any(fRecoverBeforeCheckpoint)) {
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
            greater()
        );
        // Remove all incomplete transactions from before the checkpoint, they
        // won't be encountered when the WAL is applied to the database -
        // because the replay starts at the checkpoint.
        auto i = lower_bound(
            data.incompleteTxnLsns.begin(),
            data.incompleteTxnLsns.end(),
            data.checkpoint,
            greater()
        );
        // Remove TXNs from before the checkpoint. The TXNs are in reverse LSN
        // order, so erase from checkpoint to end of vector.
        data.incompleteTxnLsns.erase(i, data.incompleteTxnLsns.end());
    }

    // Go through WAL entries starting with the last committed checkpoint and
    // redo all transactions that begin after the checkpoint and commit before
    // the end of the WAL.
    if (m_openFlags.any(fDbOpenVerbose))
        logMsgInfo() << "Recover database";
    data.analyze = false;
    applyAll(&data, fwal);
    if (!flags.any(fRecoverIncompleteTxns)) {
        assert(data.incompleteTxnLsns.empty());
        assert(!data.activeTxns);
    }

    auto & back = m_pages.back();
    m_durableLsn = back.firstLsn + back.cleanRecs - 1;
    m_lastLsn = m_durableLsn;
    m_page->onWalDurable(m_durableLsn, 0);
    return true;
}

//===========================================================================
// Creates array of references to last page and its contiguous predecessors.
bool DbWal::loadPages(FileHandle fwal) {
    if (m_openFlags.any(fDbOpenVerbose))
        logMsgInfo() << "Verify transaction WAL (write-ahead log)";

    m_freePages.clear();
    m_pages.clear();
    auto rawbuf = partialPtr(0);
    WalPage wp;
    PageInfo * pi;
    uint32_t checksum;
    // Load info for each page.
    for (auto i = (pgno_t) 1; i < m_numPages; i = pgno_t(i + 1)) {
        fileReadWait(nullptr, rawbuf, m_pageSize, fwal, i * m_pageSize);
        auto mp = (MinimumPage *) rawbuf;
        switch (mp->type) {
        case WalPageType::kInvalid:
            // No page type, skip the rest of the file under the assumption
            // that nothing was ever written to this and the following pages.
            i = (pgno_t) m_numPages;
            break;
        case WalPageType::kLogV1:
            unpack(&wp, rawbuf);
            pi = &m_pages.emplace_back();
            pi->pgno = wp.pgno;
            pi->firstLsn = wp.firstLsn;
            pi->cleanRecs = wp.numRecs;
            break;
        case WalPageType::kLog:
            unpack(&wp, rawbuf);
            pack(rawbuf, wp, 0);
            checksum = hash_crc32c(rawbuf, m_pageSize);
            if (checksum != wp.checksum) {
                logMsgError() << "Invalid checksum on page #"
                    << i << " of " << filePath(fwal);
                goto MAKE_FREE;
            }
            pi = &m_pages.emplace_back();
            pi->pgno = wp.pgno;
            pi->firstLsn = wp.firstLsn;
            pi->cleanRecs = wp.numRecs;
            break;
        default:
            logMsgError() << "Invalid page type(" << mp->type << ") on page #"
                << i << " of " << filePath(fwal);
        MAKE_FREE:
            mp->type = WalPageType::kFree;
            mp->pgno = i;
            [[fallthrough]];
        case WalPageType::kFree:
            m_freePages.insert(mp->pgno);
            s_perfFreePages += 1;
            break;
        }
    }
    if (m_pages.empty())
        return true;

    // Find the set of pages spanned by contiguous WAL records that includes
    // the record with the single largest LSN. These pages contain the last
    // checkpoint record and the preceding and following records that need to
    // be replayed to recover the database. Free all other pages, they are
    // indeterminate or from previous checkpoints.

    // Sort pages into LSN order, largest at the end.
    auto first = m_pages.begin();
    sort(
        first,
        m_pages.end(),
        [](auto & a, auto & b) { return a.firstLsn < b.firstLsn; }
    );
    // Search from largest to smallest for first page without a contiguous LSN.
    auto rlast = adjacent_find(
        m_pages.rbegin(),
        m_pages.rend(),
        [](auto & a, auto & b) {
            return a.firstLsn != b.firstLsn + b.cleanRecs;
        }
    );
    if (rlast != m_pages.rend()) {
        // There are old pages not in the contiguous set, free them.
        auto oldPages = ranges::subrange(first, rlast.base() - 1);
        for (auto&& pi : oldPages)
            m_freePages.insert(pi.pgno);
        s_perfFreePages += (unsigned) size(oldPages);
        m_pages.erase(oldPages.begin(), oldPages.end());
    }
    // Mark all pages but the last as fully saved.
    for_each(m_pages.begin(), m_pages.end() - 1, [](auto & a) {
        a.fullPageSaved = true;
    });
    return true;
}

//===========================================================================
void DbWal::applyAll(AnalyzeData * data, FileHandle fwal) {
    WalPage wp;

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
    auto lsn = Lsn{};
    auto rec = (Record *) nullptr;

    for (auto&& pi : m_pages) {
        fileReadWait(nullptr, nextBuf, m_pageSize, fwal, pi.pgno * m_pageSize);
        unpack(&wp, nextBuf);
        if (bytesBefore) {
            // When a WAL record spans pages some bytes of that record are on
            // the current page (bytesBefore), and some are on the next page
            // (bytesAfter).
            //
            // Copy the after bytes to the end of the current buffer to form a
            // contiguous WAL record that we then apply.
            auto bytesAfter = wp.firstPos - walHdrLen(wp.type);
            memcpy(
                curBuf + m_pageSize,
                nextBuf + walHdrLen(wp.type),
                bytesAfter
            );
            rec = (Record *) (curBuf + m_pageSize - bytesBefore);
            assert(getSize(*rec) == bytesBefore + bytesAfter);
            apply(data, wp.firstLsn - 1, *rec);
        }
        // Now that we're done with the current buffer, the next buffer becomes
        // the new current.
        swap(curBuf, nextBuf);

        // Apply WAL records fully contained in the current buffer.
        walPos = wp.firstPos;
        lsn = wp.firstLsn;
        while (walPos < wp.lastPos) {
            rec = (Record *) (curBuf + walPos);
            apply(data, lsn, *rec);
            walPos += getSize(*rec);
            lsn += 1;
        }
        assert(walPos == wp.lastPos);

        // Save size of the fragment of the record at the end of this page so
        // it can be combined with the rest of the record at the beginning of
        // the next page.
        bytesBefore = (int) (m_pageSize - walPos);
    }

    // Initialize WAL write buffers with the contents of the last buffer (if
    // partial) found during analyze.
    if (data->analyze && walPos < m_pageSize) {
        memcpy(m_buffers, curBuf, walPos);
        m_bufPos = walPos;
        m_bufStates[m_curBuf] = Buffer::kPartialClean;
        m_emptyBufs -= 1;
        unpack(&wp, bufPtr(m_curBuf));
        [[maybe_unused]] auto & pi = m_pages.back();
        assert(wp.firstLsn == pi.firstLsn);
    }
}

//===========================================================================
void DbWal::apply(AnalyzeData * data, Lsn lsn, const Record & rec) {
    switch (rec.type) {
    case kRecTypeCheckpoint:
        applyCheckpoint(data, lsn, getStartLsn(rec));
        break;
    case kRecTypeTxnBegin:
        applyBeginTxn(data, lsn, getLocalTxn(rec));
        break;
    case kRecTypeTxnCommit:
        applyCommitTxn(data, lsn, getLocalTxn(rec));
        break;
    case kRecTypeTxnGroupCommit:
        applyGroupCommitTxn(data, lsn, getLocalTxns(rec));
        break;
    default:
        applyUpdate(data, lsn, rec);
        break;
    }
}

//===========================================================================
void DbWal::applyCheckpoint(
    AnalyzeData * data,
    Lsn lsn,
    Lsn startLsn
) {
    if (data->analyze) {
        // Checkpoint records come after the LSN guaranteed by the checkpoint.
        // Therefore only checkpoints referencing an LSN after the start of the
        // current WAL are still valid.
        //
        // Check this by comparing with m_checkpointLsn which was initialized
        // to the LSN of the first WAL record.
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
    Lsn lsn,
    LocalTxn localTxn
) {
    if (data->analyze) {
        auto & txnLsn = data->txns[localTxn];
        if (txnLsn) {
            // Add beginning LSN of transactions that have had their id reused
            // to begin a new transaction, preventing them from ever getting
            // associated with a commit.
            //
            // Uncommitted transactions left over from an abortive shutdown are
            // detected and skipped by recovery but then ignored. Normal
            // operation then creates new transactions, eventually reusing the
            // id. Which leaves this situation until the next checkpoint frees
            // these WAL records. Or for the next recovery, if it's before that
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

    // The incompleteTxnLsns are in descending order and the WAL is processed
    // in ascending order. So if the current LSN matches the last incomplete to
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
    Lsn lsn,
    LocalTxn localTxn
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
void DbWal::applyGroupCommitTxn(
    AnalyzeData * data,
    Lsn lsn,
    const vector<LocalTxn> & localTxns
) {
    if (data->analyze) {
        for (auto&& localTxn : localTxns)
            data->txns.erase(localTxn);
        return;
    }

    //-----------------------------------------------------------------------
    // redo
    if (lsn < data->checkpoint)
        return;

    for (auto&& localTxn : localTxns) {
        if (!data->activeTxns.erase(localTxn)) {
            // Commits for transaction ids with no preceding begin are allowed
            // and ignored under the assumption that they are the previously
            // played continuations of transactions that begin before the start
            // of this recovery.
        }
    }
    m_data->onWalApplyGroupCommitTxn(lsn, localTxns);
}

//===========================================================================
void DbWal::applyUpdate(
    AnalyzeData * data,
    Lsn lsn,
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
*   Old WAL records must be discarded or they would accumulate forever. The
*   purpose of checkpointing is to mark a point at which old WAL can be safely
*   removed and then discard that WAL.
*
*   Checkpointing writes a reference in the WAL to indicate the start of
*   entries that are needed to fully recover the database. Any entries before
*   that point will be skipped by recovery and eventually discarded from the
*   WAL.
*
*   1. Find oldest LSN that has dirty pages associated, all data pages last
*      modified by an LSN older then this have already been saved and the WAL
*      records of their modifications are no longer needed. To be sure they've
*      really been saved the data pages are also flushed from the OS cache.
*   2. Write checkpoint record to WAL with this LSN. Note that since this LSN
*      already exists it is always some distance before the checkpoint record
*      in the WAL. So proper recovery requires a checkpoint record, all WAL
*      records after it, and some of the records before it.
*   3. Flush WAL pages from OS cache. Since the WAL pages are written with no
*      buffering this may not be needed, but it does cause the OS to flush
*      metadata about the file (last modified time, etc).
*   4. Logically remove pages made up of no longer needed WAL records. Also, as
*      a debugging aid, save the most recent one as a free page. May also
*      truncate the WAL file itself if enough space is freed.
*
***/

//===========================================================================
void DbWal::checkpoint() {
    {
        unique_lock lkBlock(m_blockMut);
        if (!m_checkpointBlockers.empty()) {
            // Checkpoint is being blocked, presumably by a backup process
            // of some kind.
            return;
        }
    }

    unique_lock lk{m_bufMut};
    if (m_phase != Checkpoint::kComplete
        || m_openFlags.any(fDbOpenReadOnly)
    ) {
        // A checkpoint is already in progress, or not allowed at all
        // (read-only database).
        return;
    }

    // Start Checkpoint
    // Reset time and data accumulated since last checkpoint and queue first
    // phase of checkpoint.
    m_checkpointStart = timeNow();
    m_checkpointData = 0;
    m_phase = Checkpoint::kFlushPages;
    lk.unlock();

    if (m_openFlags.any(fDbOpenVerbose))
        logMsgInfo() << "Checkpoint started";
    s_perfCps += 1;
    s_perfCurCps += 1;
    taskPushCompute(&m_checkpointPagesTask);
}

//===========================================================================
void DbWal::checkpointPages() {
    unique_lock lk{m_bufMut};
    assert(m_phase == Checkpoint::kFlushPages);
    auto lsn = m_checkpointLsn;
    lk.unlock();
    // Get oldest LSN that has dirty data pages as dependents. Also flushes OS
    // cache of any saved data pages.
    auto pageLsn = m_page->onWalCheckpointPages(lsn);
    lk.lock();
    if (pageLsn == m_checkpointLsn) {
        // No additional WAL pages have become discardable since the last
        // checkpoint, so there's no need for a new checkpoint. WAL is already
        // as truncated as possible.
        m_phase = Checkpoint::kReportComplete;
        lk.unlock();
        // The discardable point hasn't moved, but flush the file in case of
        // new WAL that has affected the WAL file's metadata.
        if (auto ec = fileFlush(m_fwal))
            logMsgFatal() << "Checkpointing failed.";

        checkpointComplete();
        return;
    }
    assert(pageLsn > m_checkpointLsn);
    m_checkpointLsn = pageLsn;

    // Write the checkpoint record and queue a checkpointDurable() call for
    // when it's written.
    m_phase = Checkpoint::kFlushCheckpoint;
    auto closing = m_closing;
    lk.unlock();
    auto lastLsn = walCheckpoint(pageLsn);
    queueTask(&m_checkpointDurableTask, lastLsn);
    if (closing) {
        // Since we're closing we don't want to wait for the buffer inactivity
        // timer, and even if we did wait it triggers on the event thread which
        // is a deadlock if it's already suspended inside the call to close()
        // which triggered this checkpoint.
        flushPartialBuffer();
    }
}

//===========================================================================
void DbWal::checkpointDurable() {
    assert(m_phase == Checkpoint::kFlushCheckpoint);
    // Flush any metadata (timestamps, file attributes, etc) changes to WAL.
    // The WAL pages themselves are already written with OS buffering disabled.
    if (auto ec = fileFlush(m_fwal))
        logMsgFatal() << "Checkpointing failed.";

    auto lastDurable = pgno_t{}; // Page that most recently became discardable.
    {
        unique_lock lk{m_bufMut};

        // Update peak pages used.
        m_peakUsedPages = max(
            (size_t) (m_peakUsedPages * 0.9),
            m_pages.size()
        );

        // Remove discardable pages from the info list and add their pgnos to
        // the free list.
        auto lastLsn = m_pages.back().firstLsn;
        auto before = m_pages.size();
        for (;;) {
            auto && pi = m_pages.front();
            if (pi.firstLsn == lastLsn)
                break;
            if (pi.firstLsn + pi.cleanRecs > m_checkpointLsn)
                break;
            if (lastDurable)
                m_freePages.insert(lastDurable);
            lastDurable = pi.pgno;
            m_pages.pop_front();
        }
        s_perfFreePages +=
            (unsigned) (before - m_pages.size() - (bool) lastDurable);

        m_phase = Checkpoint::kReportComplete;

        // Shrink the WAL file if it is still less than 70% full right before
        // pages are freed by checkpoint.
        if (m_peakUsedPages < m_numPages * 0.7) {
            // Look for free pages at the end of the file, and if there are any
            // resize the file to get rid of them. But only up to 10% of the
            // total pages.
            auto lastUsed = (pgno_t) m_numPages - 1;
            if (auto i = m_freePages.find(lastUsed)) {
                i = i.firstContiguous();
                m_numPages = max((size_t) *i, (size_t) (m_numPages * 0.9));
                auto count = lastUsed - (pgno_t) m_numPages + 1;
                m_freePages.erase((pgno_t) m_numPages, count);
                s_perfFreePages -= count;
                s_perfPages -= count;
                fileResize(m_fwal, m_numPages * m_pageSize);
            }
            if (lastDurable >= m_numPages) {
                // The last durable page is no longer part of the newly shrunk
                // WAL file, so we don't want to rewrite it as a free page.
                lastDurable = {};
            }
        }
    }

    if (!lastDurable) {
        // No pages freed, nothing to truncate, immediately report that the
        // "truncation" is complete.
        checkpointComplete();
        return;
    }

    // Mark truncation in WAL file by explicitly setting the most recently
    // discardable page to free. This is not required for correctness, but
    // can be useful for debugging.
    //
    // The call to checkpointComplete() is made by the onFileWrite()
    // callback after the write.
    auto vptr = mallocAligned(m_pageSize, m_pageSize);
    auto mp = new(vptr) MinimumPage {
        .type = WalPageType::kFree,
        .pgno = lastDurable
    };
    fileWrite(
        this,
        m_fwal,
        lastDurable * m_pageSize,
        mp,
        m_pageSize,
        walQueue()
    );
}

//===========================================================================
void DbWal::checkpointComplete() {
    unique_lock lk(m_bufMut);
    assert(m_phase == Checkpoint::kReportComplete);
    // Set checkpoint status to complete, notify things that are waiting, and
    // maybe schedule the next checkpoint.
    if (m_openFlags.any(fDbOpenVerbose))
        logMsgInfo() << "Checkpoint completed";

    m_phase = Checkpoint::kComplete;
    s_perfCurCps -= 1;
    lk.unlock();

    unique_lock lkBlock(m_blockMut);
    if (m_checkpointBlockers.empty()) {
        lkBlock.unlock();
        checkpointQueueNext();
    } else {
        DbProgressInfo info = {};
        for (auto&& blocker : m_checkpointBlockers)
            blocker->onDbProgress(kRunStopped, info);
        lkBlock.unlock();
    }
    // Notify one
    m_bufCheckpointCv.notify_one();
}

//===========================================================================
void DbWal::checkpointQueueNext() {
    unique_lock lk{m_bufMut};
    if (m_closing)
        return;

    Duration wait = 0ms;
    if (m_checkpointData >= m_maxCheckpointData) {
        // Do it now.
    } else {
        auto elapsed = timeNow() - m_checkpointStart;
        if (elapsed >= m_maxCheckpointInterval) {
            // Do it now.
        } else {
            // Wait for interval to expire.
            wait = m_maxCheckpointInterval - elapsed;
        }
    }
    lk.unlock();
    timerUpdate(&m_checkpointTimer, wait, true);
}


/****************************************************************************
*
*   DbWal - write-ahead logging
*
***/

//===========================================================================
// Write transaction begin WAL record. The transaction id used is the lowest
// available value in the range of 1 to 65534 that isn't already assigned to
// another active transaction.
Lsx DbWal::beginTxn() {
    auto localTxn = LocalTxn(1);
    {
        scoped_lock lk{m_bufMut};
        if (!m_localTxns) {
            // There are no TXNs in progress, so go ahead and use 1.
        } else {
            auto first = m_localTxns.lowerBound(1);
            if (*first > 1) {
                // No TXN with id of 1, so go ahead and use it.
            } else {
                // Find the first available value greater than 1.
                localTxn = LocalTxn(*m_localTxns.lastContiguous(first) + 1);
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
// Write transaction committed record to WAL.
void DbWal::commit(Lsx txn) {
    walCommitTxn(txn);
}

//===========================================================================
// Write transaction committed record to WAL.
void DbWal::commit(const std::unordered_set<Lsx> & txns) {
    walCommitTxns(txns);
}

//===========================================================================
Lsn DbWal::wal(
    const Record & rec,
    size_t bytes,
    TxnMode txnMode,
    Lsx txn,
    const std::unordered_set<Lsx> * txns
) {
    assert(bytes < m_pageSize - kMaxHdrLen);
    assert(bytes == getSize(rec));

    // Wait for enough buffer space to be available.
    unique_lock lk{m_bufMut};
    while (m_bufPos + bytes > m_pageSize && !m_emptyBufs)
        m_bufAvailCv.wait(lk);

    m_lastLsn += 1;
    auto lsn = m_lastLsn;

    // Count transaction beginnings on the page their WAL record started. This
    // means the current page before logging (since logging can advance to the
    // next page), UNLESS it's exactly at the end of the page. In that case the
    // transaction actually starts on the next page which, since WAL records
    // must be less than a page in size, is where we'll be after logging.
    //
    // Transaction commits are counted after logging, so it's always on the
    // page where they finished.
    if (m_bufPos == m_pageSize) {
        prepareBuffer_LK(rec, 0, bytes);
        if (txnMode == TxnMode::kBegin) {
            // Transaction began on the newly prepared page.
            countBeginTxn_LK();
        } else if (txnMode == TxnMode::kCommit) {
            // Transaction committed on newly prepared page.
            countCommitTxns_LK(txn, txns);
        }
        return lsn;
    }

    if (txnMode == TxnMode::kBegin) {
        // Transaction began on current page.
        countBeginTxn_LK();
    }

    // Adjust bytes down to amount that fits on this page, and overflow to
    // the amount that doesn't.
    size_t overflow = 0;
    if (auto avail = m_pageSize - m_bufPos; bytes > avail) {
        overflow = bytes - avail;
        bytes = avail;
    }
    // Copy record (as much as fits) to current page.
    auto base = bufPtr(m_curBuf) + m_bufPos;
    memcpy(base, &rec, bytes);
    m_bufPos += bytes;

    if (m_bufPos != m_pageSize) {
        // The WAL record does not fill the current page. A full page write
        // is not yet needed.
        auto & state = m_bufStates[m_curBuf];
        if (state == Buffer::kPartialClean) {
            state = Buffer::kPartialDirty;
            timerUpdate(&m_flushTimer, kDirtyWriteBufferTimeout);
        } else {
            assert(state == Buffer::kPartialDirty
                || state == Buffer::kPartialWriting
            );
        }
        if (txnMode == TxnMode::kCommit) {
            // Transaction committed on current page.
            countCommitTxns_LK(txn, txns);
        }
        return lsn;
    }

    // WAL record fills the current page, requiring a full page write. If it
    // has overflow bytes it will also start a new page.

    bool writeInProgress = m_bufStates[m_curBuf] == Buffer::kPartialWriting;

    // Save pointer to the buffer that filled up, this is important because if
    // there's overflow the "current buffer" will be moved to reference the
    // newly prepared buffer.
    auto rawbuf = bufPtr(m_curBuf);

    // Prepare current buffer for full page write.
    m_bufStates[m_curBuf] = Buffer::kFullWriting;
    WalPage wp;
    unpack(&wp, rawbuf);
    wp.numRecs = (uint16_t) (m_lastLsn - wp.firstLsn + 1);
    wp.lastPos = (uint16_t) m_bufPos;
    if (overflow)
        wp.lastPos -= (uint16_t) bytes;
    pack(rawbuf, wp, 0);

    if (overflow) {
        // Initialize new buffer and make it the current buffer.
        prepareBuffer_LK(rec, bytes, overflow);
    }
    if (txnMode == TxnMode::kCommit) {
        // Transaction committed on current page or, if overflow, on the newly
        // prepared page.
        countCommitTxns_LK(txn, txns);
    }

    lk.unlock();
    if (writeInProgress) {
        // The buffer is already being written, when that write completes its
        // onFileWrite() callback will start the full page write. This
        // serialization prevents the partial from overwriting the full page.
    } else {
        pack(rawbuf, wp, hash_crc32c(rawbuf, m_pageSize));
        auto offset = wp.pgno * m_pageSize;
        fileWrite(this, m_fwal, offset, rawbuf, m_pageSize, walQueue());
    }
    return lsn;
}

//===========================================================================
void DbWal::prepareBuffer_LK(
    const Record & rec,
    size_t bytesOnOldPage,
    size_t bytesOnNewPage
) {
    // Find empty buffer to prepare.
    assert(m_emptyBufs);
    for (;;) {
        if (++m_curBuf == m_numBufs)
            m_curBuf = 0;
        if (m_bufStates[m_curBuf] == Buffer::kEmpty)
            break;
    }
    auto rawbuf = bufPtr(m_curBuf);
    m_emptyBufs -= 1;

    // Initialize buffer.
    WalPage wp;
    wp.type = WalPageType::kLog;
    wp.checksum = 0;
    if (m_freePages) {
        // Recycle free page.
        wp.pgno = (pgno_t) m_freePages.pop_front();
        s_perfFreePages -= 1;
    } else {
        // Extend WAL file and use page at its new end.
        wp.pgno = (pgno_t) m_numPages++;
        s_perfPages += 1;
    }
    auto hdrLen = walHdrLen(wp.type);
    if (bytesOnOldPage) {
        // Record started on previous page, so LSN and position of first record
        // on this page will be that of the next record.
        wp.firstLsn = m_lastLsn + 1;
        wp.firstPos = (uint16_t) (hdrLen + bytesOnNewPage);
    } else {
        // Starting this record right at the beginning of this page.
        wp.firstLsn = m_lastLsn;
        wp.firstPos = (uint16_t) hdrLen;
    }
    wp.numRecs = 0;
    wp.lastPos = 0;
    pack(rawbuf, wp, 0);

    // Add reference to page table.
    auto & pi = m_pages.emplace_back(PageInfo{});
    pi.pgno = wp.pgno;
    pi.firstLsn = wp.firstLsn;
    pi.cleanRecs = 0;

    // Set buffer insertion point and initial data.
    m_bufPos = hdrLen + bytesOnNewPage;
    memcpy(
        rawbuf + hdrLen,
        (const char *) &rec + bytesOnOldPage,
        bytesOnNewPage
    );

    m_bufStates[m_curBuf] = Buffer::kPartialDirty;
    timerUpdate(&m_flushTimer, kDirtyWriteBufferTimeout);
}

//===========================================================================
void DbWal::countBeginTxn_LK() {
    m_pages.back().activeTxns += 1;
}

//===========================================================================
void DbWal::countCommitTxns_LK(
    Lsx txn,
    const std::unordered_set<Lsx> * txns
) {
    if (txn) {
        assert(!txns);
        countCommitTxn_LK(txn);
    } else if (txns) {
        assert(!txn);
        for (auto&& txn : *txns)
            countCommitTxn_LK(txn);
    }
}

//===========================================================================
void DbWal::countCommitTxn_LK(Lsx txn) {
    s_perfCurTxns -= 1;
    auto localTxn = getLocalTxn(txn);
    [[maybe_unused]] auto found = m_localTxns.erase(localTxn);
    assert(found && "Commit of unknown transaction");

    auto lsn = getLsn(txn);
    auto & commits = m_pages.back().commits;

    // Find page where TXN began within list of transaction beginning pages
    // this page already has commits for.
    auto i = lower_bound(
        commits.begin(),
        commits.end(),
        lsn,
        [](auto & a, auto lsn) {
            return lsn >= a.firstLsn + a.numRecs - 1;
        }
    );
    if (i != commits.end() && i->firstLsn <= lsn) {
        // Found commits page entry with LSN range containing transaction.
        // Increment number of transactions committed for this page.
        i->commits += 1;
        return;
    }

    // No matching page entry for transaction's LSN already in commits,
    // search pages for containing page.
    auto j = upper_bound(m_pages.begin(), m_pages.end(), lsn) - 1;
    auto firstLsn = j->firstLsn;
    auto numRecs = next(j) == m_pages.end()
        ? (unsigned) m_pageSize
        : (unsigned) (next(j)->firstLsn - firstLsn);
    commits.emplace(i, firstLsn, numRecs, 1);
}

//===========================================================================
void DbWal::onFileWrite(const FileWriteData & data) {
    if (data.written != data.data.size()) {
        logMsgFatal() << "Write to .tsl failed, " << errno << ", "
            << _doserrno;
    }

    auto rawbuf = (char *) data.data.data();
    s_perfWrites += 1;
    WalPage wp;
    unpack(&wp, rawbuf);

    unique_lock lk{m_bufMut};

    if (wp.type == WalPageType::kFree) {
        // The most recently discardable WAL page is explicitly written as free
        // by checkpointing as the last step. Now that it is durable finish the
        // checkpointing. This is the only time a free page is written.
        m_freePages.insert(wp.pgno);
        s_perfFreePages += 1;
        lk.unlock();

        // Buffer was explicitly allocated for this write, free it.
        freeAligned(rawbuf);

        checkpointComplete();
        return;
    }

    // If the data is within m_buffers it was a full page write.
    bool fullPageWrite = rawbuf >= m_buffers
        && rawbuf < m_buffers + m_numBufs * m_pageSize;

    updatePages_LK(wp.firstLsn, wp.numRecs, fullPageWrite);

    if (fullPageWrite) {
        // Full page was written.
        assert(data.data.size() == m_pageSize);
        // Set the buffer to empty so it can be reused.
        m_emptyBufs += 1;
        auto ibuf = (rawbuf - m_buffers) / m_pageSize;
        m_bufStates[ibuf] = Buffer::kEmpty;
        wp.type = WalPageType::kFree;
        pack(rawbuf, wp, wp.checksum);
        // Check if amount of data written should trigger a checkpoint.
        m_checkpointData += m_pageSize;
        bool needCheckpoint = m_checkpointData >= m_maxCheckpointData;
        lk.unlock();
        m_bufAvailCv.notify_one(); // After unlock() to avoid spurious wake-up.
        if (needCheckpoint)
            timerUpdate(&m_checkpointTimer, 0ms);
        return;
    }

    // Partial page was written.
    assert(rawbuf >= m_partialBuffers
        && rawbuf < m_partialBuffers + m_numBufs * m_pageSize
    );
    s_perfPartialWrites += 1;
    auto ibuf = (rawbuf - m_partialBuffers) / m_pageSize;
    // Inspect corresponding full page buffer.
    rawbuf = bufPtr(ibuf);
    WalPage owp;
    unpack(&owp, rawbuf);
    if (m_bufStates[ibuf] == Buffer::kPartialWriting) {
        if (owp.numRecs == wp.numRecs) {
            // Buffer has not changed since the partial write was initiated.
            m_bufStates[ibuf] = Buffer::kPartialClean;
            lk.unlock();
            m_bufAvailCv.notify_one();
        } else {
            // Data has been added to buffer, but it's still not full.
            m_bufStates[ibuf] = Buffer::kPartialDirty;
            bool closing = m_closing;
            lk.unlock();
            if (!closing) {
                // Start the flush timer.
                timerUpdate(&m_flushTimer, kDirtyWriteBufferTimeout);
            } else {
                // Since we're closing we don't want to wait for the flush
                // timer (and we've already closed it anyway). Immediately
                // queue the partial write.
                flushPartialBuffer();
            }
        }
    } else {
        assert(m_bufStates[ibuf] == Buffer::kFullWriting);
        // Buffer has become full since the partial write was initiated. Start
        // a full page write.
        lk.unlock();
        pack(rawbuf, owp, hash_crc32c(rawbuf, m_pageSize));
        fileWrite(this, m_fwal, data.offset, rawbuf, m_pageSize, walQueue());
    }
}

//===========================================================================
// Update WAL pages info to reflected completed page write and notify
// interested parties if durable LSN advanced. The durable LSN is the LSN at
// which all WAL records at or earlier than it can have their updated data
// pages written.
//
// A LSN becomes durable when all transactions that include WAL at or earlier
// than it have been either rolled back, or committed and had all of their
// WAL records (including ones after this LSN!) written to stable storage.
void DbWal::updatePages_LK(
    Lsn firstLsn,
    uint16_t cleanRecs,
    bool fullPageWrite
) {
    auto i = lower_bound(m_pages.begin(), m_pages.end(), firstLsn);
    assert(i != m_pages.end() && i->firstLsn == firstLsn);
    assert(cleanRecs >= i->cleanRecs);
    i->cleanRecs = cleanRecs;
    i->fullPageSaved = fullPageWrite;

    // Will point to oldest page with transaction committed by this update. It
    // is assumed to have committed transactions to itself.
    auto base = i;
    // Process commits in reverse order so, after the loop, base is left at the
    // oldest.
    for (auto&& pc : views::reverse(i->commits)) {
        assert(pc.commits);
        base = lower_bound(m_pages.begin(), m_pages.end(), pc.firstLsn);
        assert(base != m_pages.end() && base->firstLsn == pc.firstLsn);
        assert(base->activeTxns >= pc.commits);
        base->activeTxns -= pc.commits;
        s_perfVolatileTxns -= pc.commits;
    }
    i->commits.clear();

    if (base != m_pages.begin() && !prev(base)->commits.empty()) {
        // Previous page not yet written.
        s_perfReorderedWrites += 1;
    }
    if (base->firstLsn > m_durableLsn + 1) {
        // Oldest non-durable page not affected.
        return;
    }

    // Oldest dirty page may no longer have active transactions. Advance the
    // durable LSN through as many pages as this holds true.
    Lsn last = {};
    for (i = base; i != m_pages.end(); ++i) {
        auto & npi = *i;
        if (npi.activeTxns)
            break;
        if (!npi.cleanRecs) {
            // The only page that can have no records is a partial write of
            // what was the very last page with just the tail of the last WAL
            // record that was started on the previous page.
            assert(!npi.fullPageSaved);
            break;
        }
        last = npi.firstLsn + npi.cleanRecs - 1;
        if (!npi.fullPageSaved) {
            // The page was only written via a partial write, so when it is
            // saved again there will be an increase in cleanRecs. Therefore
            // the ultimate number of records is unknown, and we have to stop
            // counting them.
            break;
        }
        assert(npi.commits.empty());
    }
    if (!last) {
        // No eligible pages found, and hence no durable LSN advancement.
        return;
    }

    // Advance durable LSN and notify interested parties.
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
void DbWal::queueTask(
    ITaskNotify * task,
    Lsn waitLsn,
    TaskQueueHandle hq
) {
    if (!hq)
        hq = taskComputeQueue();
    unique_lock lk{m_bufMut};
    if (m_durableLsn >= waitLsn) {
        // Required LSN is already durable, run task immediately.
        taskPush(hq, task);
    } else {
        // Add task to priority queue that is ordered by LSN. It will wait
        // there until the required LSN becomes durable.
        auto ti = LsnTaskInfo{task, waitLsn, hq};
        m_lsnTasks.push(ti);
    }
}

//===========================================================================
void DbWal::flushPartialBuffer() {
    unique_lock lk{m_bufMut};
    if (m_bufStates[m_curBuf] != Buffer::kPartialDirty)
        return;

    // Update buffer state and header.
    auto rawbuf = bufPtr(m_curBuf);
    m_bufStates[m_curBuf] = Buffer::kPartialWriting;
    WalPage wp;
    unpack(&wp, rawbuf);
    wp.numRecs = (uint16_t) (m_lastLsn - wp.firstLsn + 1);
    wp.lastPos = (uint16_t) m_bufPos;
    pack(rawbuf, wp, 0);

    // Copy entire page to be written, not just the changed part, otherwise the
    // resulting page might not match the checksum.
    auto nraw = partialPtr(m_curBuf);
    memcpy(nraw, rawbuf, m_pageSize);

    lk.unlock();
    if (wp.type != WalPageType::kFree) {
        assert(wp.type == WalPageType::kLog || wp.type == WalPageType::kLogV1);
        pack(nraw, wp, hash_crc32c(nraw, m_pageSize));
    }
    auto offset = wp.pgno * m_pageSize;
    fileWrite(this, m_fwal, offset, nraw, m_pageSize, walQueue());
}


/****************************************************************************
*
*   DbTxn::PinScope
*
***/

//===========================================================================
DbTxn::PinScope::PinScope(DbTxn & txn)
    : m_txn(txn)
    , m_prevPins(m_txn.m_pinnedPages)
    , m_active(true)
{}

//===========================================================================
DbTxn::PinScope::~PinScope() {
    if (m_active)
        close();
}

//===========================================================================
void DbTxn::PinScope::close() {
    assert(m_active);
    m_txn.m_pinnedPages.erase(m_prevPins);
    m_txn.unpinAll();
    swap(m_prevPins, m_txn.m_pinnedPages);
    m_active = false;
}

//===========================================================================
void DbTxn::PinScope::release() {
    assert(m_active);
    swap(m_prevPins, m_txn.m_pinnedPages);
    m_prevPins.clear();
    m_active = false;
}

//===========================================================================
void DbTxn::PinScope::keep(pgno_t pgno) {
    assert(m_active);
    assert(m_txn.m_pinnedPages.contains(pgno));
    [[maybe_unused]] auto found = m_prevPins.insert(pgno);
    assert(found);
}


/****************************************************************************
*
*   DbTxn
*
***/

//===========================================================================
DbTxn::DbTxn(DbWal & wal, DbPage & work, shared_ptr<DbRootSet> roots)
    : m_wal{wal}
    , m_page{work}
    , m_roots{roots}
{}

//===========================================================================
DbTxn::~DbTxn() {
    commit();
}

//===========================================================================
DbTxn DbTxn::makeTxn() const {
    DbTxn out(m_wal, m_page, m_roots);
    return out;
}

//===========================================================================
Lsx DbTxn::getLsx() const {
    return m_txn;
}

//===========================================================================
UnsignedSet DbTxn::commit() {
    UnsignedSet out;
    if (m_txn) {
        shared_ptr<DbRootSet> roots;
        if (m_roots)
            roots = m_roots->lockForCommit(m_txn);
        if (!roots) {
            m_wal.commit(m_txn);
        } else if (auto txns = roots->commit(m_txn); !txns.empty()) {
            assert(txns.contains(m_txn));
            m_wal.commit(txns);

            // Create new index version
            roots = roots->publishNextSet(txns);
            roots->unlock();
        }
        m_txn = {};
    }
    unpinAll();

    swap(out, m_freePages);
    return out;
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
void DbTxn::unpinAll() {
    m_page.unpin(m_pinnedPages);
    m_pinnedPages.clear();
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
    lr->localTxn = {};
    return {m_buffer.data(), bytes};
}
