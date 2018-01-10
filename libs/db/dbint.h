// Copyright Glen Knowles 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// dbint.h - tismet db
#pragma once


/****************************************************************************
*
*   Declarations
*
***/

const unsigned kMinPageSize = 128;


/****************************************************************************
*
*   DbView
*
***/

template<bool Writable>
class DbFileView {
public:
    ~DbFileView();

    bool open(Dim::FileHandle file, size_t viewSize, size_t pageSize);
    void close();
    void growToFit(uint32_t pgno);

    const void * rptr(uint32_t pgno) const;
    size_t pageSize() const { return m_pageSize; }
    size_t viewSize() const { return m_viewSize; }

    uint32_t pgno(const void * ptr) const;

protected:
    using Pointer = std::conditional_t<Writable, char *, const char *>;
    static constexpr Dim::File::ViewMode kMode = Writable
        ? Dim::File::kViewReadWrite
        : Dim::File::kViewReadOnly;
    size_t minFirstSize() const;
    Pointer ptr(uint32_t pgno) const;

private:
    Dim::FileHandle m_file;
    size_t m_firstViewSize = 0;
    Pointer m_view = nullptr;
    std::vector<Pointer> m_views;
    size_t m_viewSize = 0;
    size_t m_pageSize = 0;
};

class DbReadView : public DbFileView<false>
{};

class DbWriteView : public DbFileView<true> {
public:
    void * wptr(uint32_t pgno) const;
};


/****************************************************************************
*
*   DbPage
*
***/

enum DbPageType : int32_t;

enum DbPageFlags : uint32_t {
    fDbPageDirty = 1,
};

struct DbPageHeader {
    DbPageType type;
    uint32_t pgno;
    uint32_t id;
    union {
        uint32_t checksum;
        uint32_t flags;
    };
    uint64_t lsn;
};

class DbPage : Dim::ITimerNotify {
public:
    DbPage();
    ~DbPage();

    bool open(
        std::string_view datafile,
        std::string_view workfile,
        size_t pageSize
    );
    void close();
    void configure(const DbConfig & conf);
    void growToFit(uint32_t pgno);

    const void * rptr(uint64_t txn, uint32_t pgno) const;
    void * wptr(uint64_t lsn, uint32_t pgno, void ** newPage);
    size_t pageSize() const { return m_pageSize; }
    size_t viewSize() const { return m_vwork.viewSize(); }
    size_t size() const { return m_pages.size(); }

    void checkpointPages();
    void checkpointStablePages();
    void stable(uint64_t lsn);
    void * wptrRedo(uint64_t lsn, uint32_t pgno);

    // true if page scan previously enabled
    bool enablePageScan(bool enable);

private:
    bool openWork(std::string_view workfile, size_t pageSize);
    bool openData(std::string_view datafile);
    void writePageWait(DbPageHeader * hdr);
    DbPageHeader * dupPage_LK(const DbPageHeader * hdr);

    void queuePageScan();
    Dim::Duration onTimer(Dim::TimePoint now) override;
    void flushStalePages();

    mutable std::mutex m_workMut;

    // One entry for every data page, may point to either a data or work view
    std::vector<DbPageHeader *> m_pages;
    std::unordered_map<uint32_t, DbPageHeader *> m_oldPages;

    size_t m_pageSize{0};

    bool m_pageScanEnabled{true};
    Dim::Duration m_pageMaxAge;
    Dim::Duration m_pageScanInterval;
    std::deque<uint64_t> m_stableLsns;
    uint64_t m_stableLsn{0};
    uint64_t m_flushLsn{0};
    Dim::TaskProxy m_flushTask;

    DbWriteView m_vdata;
    Dim::FileHandle m_fdata;
    DbWriteView m_vwork;
    Dim::FileHandle m_fwork;
    size_t m_workPages{0};
    Dim::UnsignedSet m_freeWorkPages;
};


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

    struct Record;
    static uint16_t size(const Record * log);
    static bool interleaveSafe(const Record * log);
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
    DbLog(DbData & data, DbPage & page);
    ~DbLog();

    bool open(std::string_view file);
    void close();
    void configure(const DbConfig & conf);

    // Returns transaction id (localTxn + LSN)
    uint64_t beginTxn();
    void commit(uint64_t txn);
    void checkpoint();

    void logAndApply(uint64_t txn, Record * log, size_t bytes);
    void queueTask(
        Dim::ITaskNotify * task,
        uint64_t waitTxn,
        Dim::TaskQueueHandle hq = {} // defaults to compute queue
    );

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
    uint64_t log(Record * log, size_t bytes);

    void prepareBuffer_LK(
        const Record * log,
        size_t bytesOnOldPage,
        size_t bytesOnNewPage
    );
    void updatePages_LK(const PageInfo & pi, bool partialWrite);
    void checkpointPages();
    void checkpointStablePages();
    void checkpointStableCommit();
    void checkpointTruncateCommit();
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

    DbData & m_data;
    DbPage & m_page;
    Dim::FileHandle m_flog;
    bool m_closing{false};

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

class DbTxn {
public:
    DbTxn(DbLog & log, DbPage & page);
    ~DbTxn();

    template<typename T> const T * viewPage(uint32_t pgno) const;
    size_t pageSize() const { return m_page.pageSize(); }
    size_t numPages() const { return m_page.size(); }
    void growToFit(uint32_t pgno) { m_page.growToFit(pgno); }

    void logZeroInit(uint32_t pgno);
    void logPageFree(uint32_t pgno);
    void logSegmentUpdate(uint32_t pgno, uint32_t refPage, bool free);
    void logRadixInit(
        uint32_t pgno,
        uint32_t id,
        uint16_t height,
        const uint32_t * firstPage,
        const uint32_t * lastPage
    );
    void logRadixErase(uint32_t pgno, size_t firstPos, size_t lastPos);
    void logRadixPromote(uint32_t pgno, uint32_t refPage);
    void logRadixUpdate(uint32_t pgno, size_t pos, uint32_t refPage);
    void logMetricInit(
        uint32_t pgno,
        uint32_t id,
        std::string_view name,
        Dim::Duration retention,
        Dim::Duration interval
    );
    void logMetricUpdate(
        uint32_t pgno,
        Dim::Duration retention,
        Dim::Duration interval
    );
    void logMetricClearSamples(uint32_t pgno);
    void logMetricUpdateSamples(
        uint32_t pgno,
        size_t refPos,
        uint32_t refPage,
        bool updateIndex
    );
    void logSampleInit(
        uint32_t pgno,
        uint32_t id,
        Dim::TimePoint pageTime,
        size_t lastSample
    );
    void logSampleUpdateTxn(
        uint32_t pgno,
        size_t pos,
        double value,
        bool updateLast
    );
    void logSampleUpdate(
        uint32_t pgno,
        size_t firstSample,
        size_t lastSample,
        double value,
        bool updateLast
    );
    void logSampleUpdateTime(uint32_t pgno, Dim::TimePoint pageTime);

private:
    template<typename T>
    std::pair<T *, size_t> alloc(
        DbLogRecType type,
        uint32_t pgno,
        size_t bytes = sizeof(T)
    );
    void log(DbLog::Record * rec, size_t bytes);

    DbLog & m_log;
    DbPage & m_page;
    uint64_t m_txn{0};
    std::string m_buffer;
};

//===========================================================================
template<typename T>
const T * DbTxn::viewPage(uint32_t pgno) const {
    auto lsn = DbLog::getLsn(m_txn);
    auto ptr = static_cast<const T *>(m_page.rptr(lsn, pgno));
    if constexpr (!std::is_same_v<T, DbPageHeader>) {
        // Must start with and be layout compatible with DbPageHeader
        assert((std::is_same_v<decltype(ptr->hdr), DbPageHeader>));
        assert(intptr_t(ptr) == intptr_t(&ptr->hdr));
        assert(ptr->hdr.type == ptr->type);
    }
    return ptr;
}


/****************************************************************************
*
*   DbData
*
***/

class DbData : public Dim::HandleContent {
public:
    struct SegmentPage;
    struct ZeroPage;
    struct FreePage;
    struct RadixPage;
    struct MetricPage;
    struct SamplePage;

    struct RadixData;

    struct MetricPosition {
        Dim::Duration interval;
        uint32_t infoPage;
        uint32_t lastPage; // page with most recent samples
        Dim::TimePoint pageFirstTime; // time of first sample on last page
        uint16_t pageLastSample; // position of last sample on last page
    };

public:
    ~DbData();

    // Allows updates from DbLog to be applied
    void openForApply(size_t pageSize);

    // After open metrics and samples can be updated and queried
    bool openForUpdate(
        DbTxn & txn,
        IDbEnumNotify * notify,
        std::string_view name
    );
    DbStats queryStats();

    void insertMetric(DbTxn & txn, uint32_t id, const std::string & name);
    bool eraseMetric(DbTxn & txn, std::string & name, uint32_t id);
    void updateMetric(
        DbTxn & txn,
        uint32_t id,
        const MetricInfo & info
    );
    bool getMetricInfo(const DbTxn & txn, MetricInfo & info, uint32_t id) const;

    void updateSample(
        DbTxn & txn,
        uint32_t id,
        Dim::TimePoint time,
        double value
    );
    size_t enumSamples(
        DbTxn & txn,
        IDbEnumNotify * notify,
        uint32_t id,
        Dim::TimePoint first,
        Dim::TimePoint last
    );

    void applyZeroInit(void * ptr);
    void applyPageFree(void * ptr);
    void applySegmentUpdate(void * ptr, uint32_t refPage, bool free);
    void applyRadixInit(
        void * ptr,
        uint32_t id,
        uint16_t height,
        const uint32_t * firstPgno,
        const uint32_t * lastPgno
    );
    void applyRadixErase(void * ptr, size_t firstPos, size_t lastPos);
    void applyRadixPromote(void * ptr, uint32_t refPage);
    void applyRadixUpdate(void * ptr, size_t pos, uint32_t refPage);
    void applyMetricInit(
        void * ptr,
        uint32_t id,
        std::string_view name,
        Dim::Duration retention,
        Dim::Duration interval
    );
    void applyMetricUpdate(
        void * ptr,
        Dim::Duration retention,
        Dim::Duration interval
    );
    void applyMetricClearSamples(void * ptr);
    void applyMetricUpdateSamples(
        void * ptr,
        size_t pos,
        uint32_t refPage,
        bool updateIndex
    );
    void applySampleInit(
        void * ptr,
        uint32_t id,
        Dim::TimePoint pageTime,
        size_t lastSample
    );
    void applySampleUpdate(
        void * ptr,
        size_t firstPos,
        size_t lastPos,
        double value,
        bool updateLast
    );
    void applySampleUpdateTime(void * ptr, Dim::TimePoint pageTime);

private:
    bool loadMetrics(
        DbTxn & txn,
        IDbEnumNotify * notify,
        uint32_t pgno
    );
    void metricDestructPage(DbTxn & txn, uint32_t pgno);

    bool loadFreePages(DbTxn & txn);
    uint32_t allocPgno(DbTxn & txn);
    void freePage(DbTxn & txn, uint32_t pgno);

    template<typename T> uint16_t radixEntriesPerPage() const;
    size_t radixPageEntries(
        int * ents,
        size_t maxEnts,
        DbPageType rootType,
        uint16_t height,
        size_t pos
    );
    void radixDestruct(DbTxn & txn, const DbPageHeader & hdr);
    void radixErase(
        DbTxn & txn,
        const DbPageHeader & hdr,
        size_t firstPos,
        size_t lastPos
    );
    void radixDestructPage(DbTxn & txn, uint32_t pgno);
    bool radixInsert(DbTxn & txn, uint32_t root, size_t pos, uint32_t value);

    // Returns false if pos is past the end of the index.
    bool radixFind(
        DbTxn & txn,
        DbPageHeader const ** hdr,
        RadixData const ** rd,
        size_t * rpos,
        uint32_t root,
        size_t pos
    );
    // Returns false if no value was found at the position, including if it's
    // past the end of the index.
    bool radixFind(DbTxn & txn, uint32_t * out, uint32_t root, size_t pos);

    size_t samplesPerPage() const;
    MetricPosition & loadMetricPos(DbTxn & txn, uint32_t id);
    MetricPosition & loadMetricPos(
        DbTxn & txn,
        uint32_t id,
        Dim::TimePoint time
    );
    bool findSamplePage(
        DbTxn & txn,
        uint32_t * pgno,
        unsigned * pagePos,
        uint32_t id,
        Dim::TimePoint time
    );

    std::vector<MetricPosition> m_metricPos;

    size_t m_segmentSize = 0;
    size_t m_pageSize = 0;
    size_t m_numPages = 0;
    Dim::UnsignedSet m_freePages;
    unsigned m_numMetrics = 0;
};
