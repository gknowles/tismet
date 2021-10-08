// Copyright Glen Knowles 2017 - 2018.
// Distributed under the Boost Software License, Version 1.0.
//
// dblog.h - tismet db
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
*   DbLog
*
***/

enum DbLogRecType : int8_t;
class DbData;

class DbLog : Dim::IFileWriteNotify {
public:
    enum class Buffer : int;
    enum class Checkpoint : int;
    class IPageNotify;
    class IApplyNotify;

    struct Record;
    static uint16_t getSize(const Record & log);
    static pgno_t getPgno(const Record & log);
    static uint16_t getLocalTxn(const Record & log);
    static void setLocalTxn(Record * log, uint16_t localTxn);

    static uint64_t getLsn(uint64_t logPos);
    static uint16_t getLocalTxn(uint64_t logPos);
    static uint64_t getTxn(uint64_t lsn, uint16_t localTxn);

    struct PageInfo {
        pgno_t pgno;
        uint64_t firstLsn;
        uint16_t numLogs;

        unsigned activeTxns;
        std::vector<std::pair<
            uint64_t,   // firstLsn of page
            unsigned    // number of txns from that page committed
        >> commitTxns;
    };

public:
    DbLog(IApplyNotify * data, IPageNotify * page);
    ~DbLog();

    // pageSize must match the size saved in the data file or be zero. If it is
    // zero fDbOpenCreat must not be specified.
    bool open(std::string_view file, size_t pageSize, DbOpenFlags flags);

    enum RecoverFlags : unsigned {
        // Redo incomplete transactions during recovery, since they are
        // incomplete this would normally the database in a corrupt state. Used
        // by WAL dump tool, which completely replaces the normal database
        // apply logic.
        fRecoverIncompleteTxns = 0x01,

        // Include log records from before the last checkpoint, also only for
        // WAL dump tool.
        fRecoverBeforeCheckpoint = 0x02,
    };
    bool recover(RecoverFlags flags = {});

    void close();
    DbConfig configure(const DbConfig & conf);

    // Returns transaction id (localTxn + LSN)
    uint64_t beginTxn();
    void commit(uint64_t txn);
    void checkpoint();
    void blockCheckpoint(IDbProgressNotify * notify, bool enable);

    void logAndApply(uint64_t txn, Record * log, size_t bytes);

    // Queues task to be run after the indicated LSN is committed to stable
    // storage.
    void queueTask(
        Dim::ITaskNotify * task,
        uint64_t waitLsn,
        Dim::TaskQueueHandle hq = {} // defaults to compute queue
    );

    size_t dataPageSize() const { return m_pageSize / 2; }
    size_t logPageSize() const { return m_pageSize; }

    Dim::FileHandle logFile() const { return m_flog; }
    bool newFiles() const { return m_newFiles; }

private:
    char * bufPtr(size_t ibuf);
    char * partialPtr(size_t ibuf);

    void onFileWrite(
        int written,
        std::string_view data,
        int64_t offset,
        Dim::FileHandle f
    ) override;

    bool loadPages(Dim::FileHandle flog);

    void logCommitCheckpoint(uint64_t startLsn);
    uint64_t logBeginTxn(uint16_t localTxn);
    void logCommit(uint64_t txn);

    // returns LSN
    enum class TxnMode { kBegin, kContinue, kCommit };
    uint64_t log(
        const Record & log,
        size_t bytes,
        TxnMode txnMode,
        uint64_t txn = 0
    );

    void prepareBuffer_LK(
        const Record & log,
        size_t bytesOnOldPage,
        size_t bytesOnNewPage
    );
    void countBeginTxn_LK();
    void countCommitTxn_LK(uint64_t txn);
    void updatePages_LK(const PageInfo & pi, bool fullPageWrite);
    void checkpointPages();
    void checkpointStableCommit();
    void checkpointTruncateCommit();
    void checkpointWaitForNext();
    void flushWriteBuffer();

    struct AnalyzeData;
    void applyAll(AnalyzeData * data, Dim::FileHandle flog);
    void apply(AnalyzeData * data, uint64_t lsn, const Record & log);
    void applyCommitCheckpoint(
        AnalyzeData * data,
        uint64_t lsn,
        uint64_t startLsn
    );
    void applyBeginTxn(AnalyzeData * data, uint64_t lsn, uint16_t txn);
    void applyCommitTxn(AnalyzeData * data, uint64_t lsn, uint16_t txn);
    void applyUpdate(AnalyzeData * data, uint64_t lsn, const Record & log);

    void apply(uint64_t lsn, const Record & log);
    void applyUpdate(void * page, const Record & log);

    IApplyNotify * m_data;
    IPageNotify * m_page;
    Dim::FileHandle m_flog;
    bool m_closing{false};
    bool m_newFiles{false}; // did the open create new data files?
    DbOpenFlags m_openFlags{};

    // last assigned
    Dim::UnsignedSet m_localTxns;
    uint64_t m_lastLsn{0};

    Dim::UnsignedSet m_freePages;
    std::deque<PageInfo> m_pages;
    size_t m_numPages{0};
    size_t m_pageSize{0};

    size_t m_maxCheckpointData = kDefaultMaxCheckpointData;
    size_t m_checkpointData{0};
    Dim::Duration m_maxCheckpointInterval = kDefaultMaxCheckpointInterval;
    Dim::TimerProxy m_checkpointTimer;
    Dim::TaskProxy m_checkpointPagesTask;
    Dim::TaskProxy m_checkpointStableCommitTask;
    Checkpoint m_phase{};

    // Checkpoint blocks prevent checkpoints from occurring so that backups
    // can be done safely.
    std::vector<IDbProgressNotify *> m_checkpointBlocks;

    // last started (perhaps unfinished) checkpoint
    Dim::TimePoint m_checkpointStart;
    uint64_t m_checkpointLsn{0};

    // last known durably saved
    uint64_t m_stableLsn{0};

    struct LsnTaskInfo {
        Dim::ITaskNotify * notify;
        uint64_t waitLsn;
        Dim::TaskQueueHandle hq;

        bool operator<(const LsnTaskInfo & right) const;
        bool operator>(const LsnTaskInfo & right) const;
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

    // page aligned buffers
    char * m_buffers{};
    char * m_partialBuffers{};

    unsigned m_numBufs{};
    unsigned m_emptyBufs{};
    unsigned m_curBuf{};
    size_t m_bufPos{};
};

//===========================================================================
inline bool operator<(const DbLog::PageInfo & a, const DbLog::PageInfo & b) {
    return a.firstLsn < b.firstLsn;
}


/****************************************************************************
*
*   DbLog::IPageNotify
*
***/

class DbLog::IPageNotify {
public:
    virtual ~IPageNotify() = default;

    // Returns content of page that will be updated in place by applying the
    // action already recorded at the specified LSN. The pgno and lsn fields of
    // the buffer must be set before returning.
    virtual void * onLogGetUpdatePtr(
        pgno_t pgno,
        uint64_t lsn,
        uint16_t localTxn
    ) = 0;

    // Similar to onLogGetUpdatePtr, except that if the page has already been
    // updated no action is taken and null is returned. A page is considered
    // to have been updated if the on page LSN is greater or equal to the LSN
    // of the update.
    virtual void * onLogGetRedoPtr(
        pgno_t pgno,
        uint64_t lsn,
        uint16_t localTxn
    ) = 0;

    // Reports the stable LSN and the additional bytes of WAL that were written
    // to get there. A LSN is stable when all transactions that include logs at
    // or earlier than it have been either rolled back or been committed and
    // have had all of their logs (including any after this LSN!) written to
    // stable storage.
    //
    // The byte count combined with max checkpoint bytes provides a target for
    // the page eviction algorithm.
    virtual void onLogStable(uint64_t lsn, size_t bytes) {}

    // The stable LSN is passed in, and the first stable LSN that still has
    // volatile (not yet persisted to stable storage) data pages associated
    // with it is returned.
    //
    // Upon return, all WAL prior to the returned LSN may be purged.
    virtual uint64_t onLogCheckpointPages(uint64_t lsn) { return lsn; }
};


/****************************************************************************
*
*   DbLog::IApplyNotify
*
***/

class DbLog::IApplyNotify {
public:
    virtual ~IApplyNotify() = default;

    virtual void onLogApplyCommitCheckpoint(
        uint64_t lsn,
        uint64_t startLsn
    ) = 0;
    virtual void onLogApplyBeginTxn(uint64_t lsn, uint16_t localTxn) = 0;
    virtual void onLogApplyCommitTxn(uint64_t lsn, uint16_t localTxn) = 0;

    virtual void onLogApplyZeroInit(void * ptr) = 0;
    virtual void onLogApplyPageFree(void * ptr) = 0;
    virtual void onLogApplySegmentUpdate(
        void * ptr,
        pgno_t refPage,
        bool free
    ) = 0;

    virtual void onLogApplyRadixInit(
        void * ptr,
        uint32_t id,
        uint16_t height,
        const pgno_t * firstPgno,
        const pgno_t * lastPgno
    ) = 0;
    virtual void onLogApplyRadixErase(
        void * ptr,
        size_t firstPos,
        size_t lastPos
    ) = 0;
    virtual void onLogApplyRadixPromote(void * ptr, pgno_t refPage) = 0;
    virtual void onLogApplyRadixUpdate(
        void * ptr,
        size_t pos,
        pgno_t refPage
    ) = 0;

    virtual void onLogApplyMetricInit(
        void * ptr,
        uint32_t id,
        std::string_view name,
        Dim::TimePoint creation,
        DbSampleType sampleType,
        Dim::Duration retention,
        Dim::Duration interval
    ) = 0;
    virtual void onLogApplyMetricUpdate(
        void * ptr,
        Dim::TimePoint creation,
        DbSampleType sampleType,
        Dim::Duration retention,
        Dim::Duration interval
    ) = 0;
    virtual void onLogApplyMetricClearSamples(void * ptr) = 0;
    virtual void onLogApplyMetricUpdateSamples(
        void * ptr,
        size_t pos,
        Dim::TimePoint refTime,
        size_t refSample,
        pgno_t refPage
    ) = 0;
    virtual void onLogApplySampleInit(
        void * ptr,
        uint32_t id,
        DbSampleType sampleType,
        Dim::TimePoint pageTime,
        size_t lastSample,
        double fill
    ) = 0;
    virtual void onLogApplySampleUpdate(
        void * ptr,
        size_t firstPos,
        size_t lastPos,
        double value,
        bool updateLast
    ) = 0;
    virtual void onLogApplySampleUpdateTime(
        void * ptr,
        Dim::TimePoint pageTime
    ) = 0;
};
