// Copyright Glen Knowles 2017 - 2022.
// Distributed under the Boost Software License, Version 1.0.
//
// dbwal.h - tismet db
#pragma once

#include "core/core.h"
#include "file/file.h"

#include <condition_variable>
#include <cstdint>
#include <deque>
#include <mutex>
#include <queue>
#include <string_view>
#include <vector>


/****************************************************************************
*
*   Tuning parameters
*
***/

constexpr unsigned kDefaultMaxCheckpointData = 1'048'576; // 1MiB
constexpr Dim::Duration kDefaultMaxCheckpointInterval = (std::chrono::hours) 1;


/****************************************************************************
*
*   DbWal
*
***/

enum DbWalRecType : int8_t;
class DbData;

class DbWal : Dim::IFileWriteNotify {
public:
    enum class Buffer : int;
    enum class Checkpoint : int;
    class IPageNotify;
    class IApplyNotify;

    struct Record;
    static uint16_t getSize(const Record & rec);
    static pgno_t getPgno(const Record & rec);
    static uint16_t getLocalTxn(const Record & rec);
    static uint64_t getStartLsn(const Record & rec);
    static void setLocalTxn(Record * rec, uint16_t localTxn);

    static uint64_t getLsn(uint64_t walPos);
    static uint16_t getLocalTxn(uint64_t walPos);
    static uint64_t getTxn(uint64_t lsn, uint16_t localTxn);

    struct PageInfo {
        pgno_t pgno;
        uint64_t firstLsn;
        uint16_t cleanRecs; // WAL records on page that have been saved.

        // Page has been filled up and fully saved.
        bool fullPageSaved;

        // Count of transactions begun on this page that have not yet been
        // committed and fully written to WAL.
        unsigned activeTxns;

        // Counts of transactions committed on this page grouped by their
        // beginning page. The vector is in order of newest to oldest page,
        // starting with this page, including only those pages that began a
        // transaction that was committed on this page.
        struct Commits {
            uint64_t firstLsn;  // First LSN of page with transaction begins.
            unsigned numRecs;   // Number of LSNs on the page.
            unsigned commits;   // Commits for the page.
        };
        std::vector<Commits> commits;

        auto operator<=>(const uint64_t & other) const {
            return firstLsn <=> other;
        }
    };

public:
    DbWal(IApplyNotify * data, IPageNotify * page);
    ~DbWal();

    // pageSize is only applied if new files are being created, 0 defaults to
    // the same size as memory pages.
    bool open(
        std::string_view file,
        Dim::EnumFlags<DbOpenFlags> flags,
        size_t pageSize = 0
    );

    enum RecoverFlags : unsigned {
        // Redo incomplete transactions during recovery, since they are
        // incomplete this would normally leave the database in a corrupt
        // state. Used by WAL dump tool, which completely replaces the normal
        // database apply logic.
        fRecoverIncompleteTxns = 0x01,

        // Include wal records from before the last checkpoint, also only for
        // WAL dump tool.
        fRecoverBeforeCheckpoint = 0x02,
    };
    bool recover(Dim::EnumFlags<RecoverFlags> flags = {});

    void close();
    DbConfig configure(const DbConfig & conf);

    // Returns transaction id (localTxn + LSN)
    uint64_t beginTxn();
    void commit(uint64_t txn);
    void checkpoint();
    void blockCheckpoint(IDbProgressNotify * notify, bool enable);

    void walAndApply(uint64_t txn, Record * rec, size_t bytes);

    // Queue task to be run after the indicated LSN becomes durable (is
    // committed to stable storage).
    void queueTask(
        Dim::ITaskNotify * task,
        uint64_t waitLsn,
        Dim::TaskQueueHandle hq = {} // defaults to compute queue
    );

    size_t dataPageSize() const { return m_dataPageSize; }
    size_t walPageSize() const { return m_pageSize; }

    Dim::FileHandle walFile() const { return m_fwal; }
    bool newFiles() const { return m_newFiles; }

private:
    char * bufPtr(size_t ibuf);
    char * partialPtr(size_t ibuf);

    void onFileWrite(const Dim::FileWriteData & data) override;

    bool loadPages(Dim::FileHandle fwal);

    uint64_t walCheckpoint(uint64_t startLsn);  // returns LSN
    uint64_t walBeginTxn(uint16_t localTxn);    // returns localTxn + LSN
    uint64_t walCommitTxn(uint64_t txn);        // returns localTxn + LSN

    // Returns LSN.
    enum class TxnMode { kBegin, kContinue, kCommit };
    uint64_t wal(
        const Record & rec,
        size_t bytes,
        TxnMode txnMode,
        uint64_t txn = 0
    );

    void prepareBuffer_LK(
        const Record & rec,
        size_t bytesOnOldPage,
        size_t bytesOnNewPage
    );
    void countBeginTxn_LK();
    void countCommitTxn_LK(uint64_t txn);
    void updatePages_LK(
        uint64_t firstLsn,
        uint16_t cleanRecs,
        bool fullPageWrite
    );
    void checkpointPages();
    void checkpointDurable();
    void checkpointComplete();
    void checkpointQueueNext();
    void flushPartialBuffer();

    struct AnalyzeData;
    void applyAll(AnalyzeData * data, Dim::FileHandle fwal);
    void apply(AnalyzeData * data, uint64_t lsn, const Record & rec);
    void applyCheckpoint(
        AnalyzeData * data,
        uint64_t lsn,
        uint64_t startLsn
    );
    void applyBeginTxn(AnalyzeData * data, uint64_t lsn, uint16_t txn);
    void applyCommitTxn(AnalyzeData * data, uint64_t lsn, uint16_t txn);
    void applyUpdate(AnalyzeData * data, uint64_t lsn, const Record & rec);

    void applyUpdate(void * page, uint64_t lsn, const Record & rec);

    IApplyNotify * m_data;
    IPageNotify * m_page;
    Dim::FileHandle m_fwal;
    bool m_closing = false;
    bool m_newFiles = false; // Did the open create new data files?
    Dim::EnumFlags<DbOpenFlags> m_openFlags{};

    // Last Assigned
    Dim::UnsignedSet m_localTxns; // Ids of active transactions.
    uint64_t m_lastLsn = 0; // LSN assigned to most recently added record.

    Dim::UnsignedSet m_freePages;
    size_t m_numPages = 0;
    size_t m_peakUsedPages = 0;

    // Information about all active pages. A page is active if it has not been
    // filled, has not been saved, or has active transactions. A transaction
    // is active if it hasn't been committed or that commit has not been saved.
    std::deque<PageInfo> m_pages;

    size_t m_pageSize = 0;
    size_t m_dataPageSize = 0;

    size_t m_maxCheckpointData = kDefaultMaxCheckpointData;
    size_t m_checkpointData = 0;
    Dim::Duration m_maxCheckpointInterval = kDefaultMaxCheckpointInterval;
    Dim::TimerProxy m_checkpointTimer;
    Dim::TaskProxy m_checkpointPagesTask;
    Dim::TaskProxy m_checkpointDurableTask;
    Checkpoint m_phase = {};
    std::condition_variable m_bufCheckpointCv;

    // Checkpoint blocks prevent checkpoints from occurring so that backups can
    // be done safely.
    std::mutex m_blockMut;
    std::vector<IDbProgressNotify *> m_checkpointBlockers;

    // Last started (perhaps unfinished) checkpoint.
    Dim::TimePoint m_checkpointStart;
    uint64_t m_checkpointLsn = 0;

    // Last known LSN durably saved.
    uint64_t m_durableLsn = 0;

    struct LsnTaskInfo {
        Dim::ITaskNotify * notify;
        uint64_t waitLsn;
        Dim::TaskQueueHandle hq;

        auto operator<=>(const LsnTaskInfo & other) const {
            return waitLsn <=> other.waitLsn;
        }
    };
    std::priority_queue<
        LsnTaskInfo,
        std::vector<LsnTaskInfo>,
        std::greater<LsnTaskInfo>
    > m_lsnTasks;

    Dim::TimerProxy m_flushTimer;
    std::mutex m_bufMut;
    std::condition_variable m_bufAvailCv;
    std::vector<Buffer> m_bufStates;

    // Page Aligned Buffers
    char * m_buffers = {};
    char * m_partialBuffers = {};

    unsigned m_numBufs = 0;
    unsigned m_emptyBufs = 0;
    unsigned m_curBuf = 0;      // Buffer currently receiving WAL
    size_t m_bufPos = 0;        // Write position within current buffer
};


/****************************************************************************
*
*   DbWal::IPageNotify
*
***/

class DbWal::IPageNotify {
public:
    virtual ~IPageNotify() = default;

    // Returns content of page that will be updated in place by applying the
    // action already recorded at the specified LSN. The returned buffer has
    // it's pgno and lsn fields set. Page is locked and must be unlocked via
    // subsequent call to onWalUnlockPtr().
    virtual void * onWalGetPtrForUpdate(
        pgno_t pgno,
        uint64_t lsn,
        uint16_t localTxn
    ) = 0;
    // Called to release lock on ptr returned by onWalGetPtrForUpdate().
    virtual void onWalUnlockPtr(pgno_t pgno) = 0;

    // Similar to onWalGetPtrForUpdate, except that if the page has already
    // been updated no action is taken and null is returned. A page is
    // considered to have been updated if the on page LSN is greater or equal
    // to the LSN of the update. Does not lock page, recovery is assumed to be
    // single threaded.
    virtual void * onWalGetPtrForRedo(
        pgno_t pgno,
        uint64_t lsn,
        uint16_t localTxn
    ) = 0;

    // Reports the durable LSN and the additional bytes of WAL that were
    // written to get there. The durable LSN is the point at which all WAL
    // records less or equal to it can have their updated data pages written.
    // The additional bytes is always a multiple of WAL page size.
    //
    // The byte count combined with max checkpoint bytes provides a target for
    // the page eviction algorithm.
    virtual void onWalDurable(uint64_t lsn, size_t bytes) {}

    // The first durable LSN is passed in, and the first durable LSN that still
    // has dirty (not yet persisted to stable storage) data pages associated
    // with it is returned.
    //
    // Upon return, all WAL prior to the returned LSN may be discarded. And, as
    // discarded pages aren't durable, this causes the value for first durable
    // LSN to be advanced. Since prior WAL is immediately discardable, this
    // function may need to make the OS flush it's cache to meet this
    // guarantee.
    virtual uint64_t onWalCheckpointPages(uint64_t lsn) { return lsn; }
};


/****************************************************************************
*
*   DbWal::IApplyNotify
*
***/

class DbWal::IApplyNotify {
public:
    virtual ~IApplyNotify() = default;

    virtual void onWalApplyCheckpoint(
        uint64_t lsn,
        uint64_t startLsn
    ) = 0;
    virtual void onWalApplyBeginTxn(uint64_t lsn, uint16_t localTxn) = 0;
    virtual void onWalApplyCommitTxn(uint64_t lsn, uint16_t localTxn) = 0;

    virtual void onWalApplyZeroInit(void * ptr) = 0;
    virtual void onWalApplyTagRootUpdate(void * ptr, pgno_t rootPage) = 0;
    virtual void onWalApplyPageFree(void * ptr) = 0;
    virtual void onWalApplyFullPageInit(
        void * ptr,
        DbPageType type,
        uint32_t id,
        std::span<const uint8_t> data
    ) = 0;

    virtual void onWalApplyRadixInit(
        void * ptr,
        uint32_t id,
        uint16_t height,
        const pgno_t * firstPgno,
        const pgno_t * lastPgno
    ) = 0;
    virtual void onWalApplyRadixErase(
        void * ptr,
        size_t firstPos,
        size_t lastPos
    ) = 0;
    virtual void onWalApplyRadixPromote(void * ptr, pgno_t refPage) = 0;
    virtual void onWalApplyRadixUpdate(
        void * ptr,
        size_t pos,
        pgno_t refPage
    ) = 0;

    virtual void onWalApplyBitInit(
        void * ptr,
        uint32_t id,
        uint32_t base,
        bool fill,
        uint32_t pos
    ) = 0;
    virtual void onWalApplyBitUpdate(
        void * ptr,
        uint32_t firstPos,
        uint32_t lastPos,
        bool value
    ) = 0;

    virtual void onWalApplyMetricInit(
        void * ptr,
        uint32_t id,
        std::string_view name,
        Dim::TimePoint creation,
        DbSampleType sampleType,
        Dim::Duration retention,
        Dim::Duration interval
    ) = 0;
    virtual void onWalApplyMetricUpdate(
        void * ptr,
        Dim::TimePoint creation,
        DbSampleType sampleType,
        Dim::Duration retention,
        Dim::Duration interval
    ) = 0;
    virtual void onWalApplyMetricClearSamples(void * ptr) = 0;
    virtual void onWalApplyMetricUpdateSamples(
        void * ptr,
        size_t pos,
        Dim::TimePoint refTime,
        size_t refSample,
        pgno_t refPage
    ) = 0;
    virtual void onWalApplySampleInit(
        void * ptr,
        uint32_t id,
        DbSampleType sampleType,
        Dim::TimePoint pageTime,
        size_t lastSample,
        double fill
    ) = 0;
    virtual void onWalApplySampleUpdate(
        void * ptr,
        size_t firstPos,
        size_t lastPos,
        double value,
        bool updateLast
    ) = 0;
    virtual void onWalApplySampleUpdateTime(
        void * ptr,
        Dim::TimePoint pageTime
    ) = 0;
};
