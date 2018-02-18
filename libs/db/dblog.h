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
    static uint16_t size(const Record * log);
    static uint32_t getPgno(const Record * log);
    static uint16_t getLocalTxn(const Record * log);
    static void setLocalTxn(Record * log, uint16_t localTxn);

    static uint64_t getLsn(uint64_t logPos);
    static uint16_t getLocalTxn(uint64_t logPos);
    static uint64_t getTxn(uint64_t lsn, uint16_t localTxn);

    struct PageInfo {
        uint32_t pgno;
        uint64_t firstLsn;
        uint16_t numLogs;

        unsigned beginTxns;
        std::vector<std::pair<
            uint64_t,   // firstLsn of page
            unsigned    // number of txns from that page committed
        >> commitTxns;
    };

public:
    DbLog(IApplyNotify * data, IPageNotify * page);
    ~DbLog();

    // pageSize must match the size saved in the file or be zero. If it is
    // zero fDbOpenCreat must not be specified.
    bool open(std::string_view file, size_t pageSize, DbOpenFlags flags);

    void close();
    void configure(const DbConfig & conf);

    // Returns transaction id (localTxn + LSN)
    uint64_t beginTxn();
    void commit(uint64_t txn);
    void checkpoint();
    void blockCheckpoint(IDbProgressNotify * notify, bool enable);

    void logAndApply(uint64_t txn, Record * log, size_t bytes);
    void queueTask(
        Dim::ITaskNotify * task,
        uint64_t waitTxn,
        Dim::TaskQueueHandle hq = {} // defaults to compute queue
    );

    Dim::FileHandle logFile() const { return m_flog; }

private:
    char * bufPtr(size_t ibuf);

    void onFileWrite(
        int written,
        std::string_view data,
        int64_t offset,
        Dim::FileHandle f
    ) override;

    bool loadPages();
    bool recover();

    void logCommitCheckpoint(uint64_t startLsn);
    uint64_t logBeginTxn(uint16_t localTxn);
    void logCommit(uint64_t txn);

    // returns LSN
    uint64_t log(Record * log, size_t bytes, int txnType, uint64_t txn = 0);

    void prepareBuffer_LK(
        const Record * log,
        size_t bytesOnOldPage,
        size_t bytesOnNewPage
    );
    void countBeginTxn_LK();
    void countCommitTxn_LK(uint64_t txn);
    void updatePages_LK(const PageInfo & pi, bool partialWrite);
    void checkpointPages();
    void checkpointStablePages();
    void checkpointStableCommit();
    void checkpointTruncateCommit();
    void checkpointWaitForNext();
    void flushWriteBuffer();

    struct AnalyzeData;
    void applyAll(AnalyzeData & data);
    void apply(uint64_t lsn, const Record * log, AnalyzeData * data = nullptr);
    void applyUpdate(uint64_t lsn, const Record * log);
    void applyUpdate(void * page, const Record * log);
    void applyRedo(AnalyzeData & data, uint64_t lsn, const Record * log);
    void applyCommitCheckpoint(
        AnalyzeData & data,
        uint64_t lsn,
        uint64_t startLsn
    );
    void applyBeginTxn(AnalyzeData & data, uint64_t lsn, uint16_t txn);
    void applyCommit(AnalyzeData & data, uint64_t lsn, uint16_t txn);

    IApplyNotify * m_data;
    IPageNotify * m_page;
    Dim::FileHandle m_flog;
    bool m_closing{false};
    DbOpenFlags m_openFlags{};

    // last assigned
    uint16_t m_lastLocalTxn{0};
    uint64_t m_lastLsn{0};

    Dim::UnsignedSet m_freePages;
    std::deque<PageInfo> m_pages;
    size_t m_numPages{0};
    size_t m_pageSize{0};

    size_t m_maxCheckpointData{0};
    size_t m_checkpointData{0};
    Dim::Duration m_maxCheckpointInterval;
    Dim::TimerProxy m_checkpointTimer;
    Dim::TaskProxy m_checkpointPagesTask;
    Dim::TaskProxy m_checkpointStablePagesTask;
    Dim::TaskProxy m_checkpointStableCommitTask;
    Checkpoint m_phase{};
    std::vector<IDbProgressNotify *> m_checkpointBlocks;

    // last started (perhaps unfinished) checkpoint
    Dim::TimePoint m_checkpointStart;
    uint64_t m_checkpointLsn{0};

    // last known durably saved
    uint64_t m_stableTxn{0};

    struct TaskInfo {
        Dim::ITaskNotify * notify;
        uint64_t waitTxn;
        Dim::TaskQueueHandle hq;

        bool operator<(const TaskInfo & right) const;
        bool operator>(const TaskInfo & right) const;
    };
    std::priority_queue<
        TaskInfo,
        std::vector<TaskInfo>,
        std::greater<TaskInfo>
    > m_tasks;

    Dim::TimerProxy m_flushTimer;
    std::mutex m_bufMut;
    std::condition_variable m_bufAvailCv;
    std::vector<Buffer> m_bufStates;
    std::unique_ptr<char[]> m_buffers;
    unsigned m_numBufs{0};
    unsigned m_emptyBufs{0};
    unsigned m_curBuf{0};
    size_t m_bufPos{0};
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

    virtual void * onLogGetUpdatePtr(uint64_t lsn, uint32_t pgno) = 0;
    virtual void * onLogGetRedoPtr(uint64_t lsn, uint32_t pgno) = 0;

    virtual void onLogStable(uint64_t lsn) {}
    virtual void onLogCheckpointPages() {}
    virtual void onLogCheckpointStablePages() {}
};


/****************************************************************************
*
*   DbLog::IApplyNotify
*
***/

class DbLog::IApplyNotify {
public:
    virtual ~IApplyNotify() = default;

    virtual void onLogApplyZeroInit(void * ptr) = 0;
    virtual void onLogApplyPageFree(void * ptr) = 0;
    virtual void onLogApplySegmentUpdate(
        void * ptr,
        uint32_t refPage,
        bool free
    ) = 0;
    virtual void onLogApplyRadixInit(
        void * ptr,
        uint32_t id,
        uint16_t height,
        const uint32_t * firstPgno,
        const uint32_t * lastPgno
    ) = 0;
    virtual void onLogApplyRadixErase(
        void * ptr,
        size_t firstPos,
        size_t lastPos
    ) = 0;
    virtual void onLogApplyRadixPromote(void * ptr, uint32_t refPage) = 0;
    virtual void onLogApplyRadixUpdate(
        void * ptr,
        size_t pos,
        uint32_t refPage
    ) = 0;
    virtual void onLogApplyMetricInit(
        void * ptr,
        uint32_t id,
        std::string_view name,
        DbSampleType sampleType,
        Dim::Duration retention,
        Dim::Duration interval
    ) = 0;
    virtual void onLogApplyMetricUpdate(
        void * ptr,
        DbSampleType sampleType,
        Dim::Duration retention,
        Dim::Duration interval
    ) = 0;
    virtual void onLogApplyMetricClearSamples(void * ptr) = 0;
    virtual void onLogApplyMetricUpdateSamples(
        void * ptr,
        size_t pos,
        uint32_t refPage,
        Dim::TimePoint refTime,
        bool updateIndex
    ) = 0;
    virtual void onLogApplySampleInit(
        void * ptr,
        uint32_t id,
        DbSampleType sampleType,
        Dim::TimePoint pageTime,
        size_t lastSample
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
