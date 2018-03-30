// Copyright Glen Knowles 2017 - 2018.
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

class DbPage : public DbLog::IPageNotify, Dim::ITimerNotify {
public:
    DbPage();
    ~DbPage();

    bool open(
        std::string_view datafile,
        std::string_view workfile,
        size_t pageSize,
        DbOpenFlags flags
    );
    void close();
    void configure(const DbConfig & conf);
    void growToFit(uint32_t pgno);

    const void * rptr(uint64_t lsn, uint32_t pgno) const;
    size_t pageSize() const { return m_pageSize; }
    size_t viewSize() const { return m_vwork.viewSize(); }
    size_t size() const { return m_pages.size(); }

    // true if page scan previously enabled
    bool enablePageScan(bool enable);

    Dim::FileHandle dataFile() const { return m_fdata; }
    bool newFiles() const { return m_newFiles; }

private:
    bool openData(std::string_view datafile, size_t pageSize);
    bool openWork(std::string_view workfile);
    void writePageWait(DbPageHeader * hdr);
    DbPageHeader * dupPage_LK(const DbPageHeader * hdr);

    void * onLogGetUpdatePtr(uint32_t pgno, uint64_t lsn, uint16_t txn) override;
    void * onLogGetRedoPtr(uint32_t pgno, uint64_t lsn, uint16_t txn) override;
    void onLogStable(uint64_t lsn) override;
    void onLogCheckpointPages() override;
    void onLogCheckpointStablePages() override;

    Dim::Duration onTimer(Dim::TimePoint now) override;
    void queuePageScan();
    void flushStalePages();

    mutable std::mutex m_workMut;

    // One entry for every data page, may point to either a data or work view
    std::vector<DbPageHeader *> m_pages;
    std::unordered_map<uint32_t, DbPageHeader *> m_oldPages;

    size_t m_pageSize{0};
    DbOpenFlags m_flags{};
    bool m_newFiles{false};

    bool m_pageScanEnabled{true};
    Dim::Duration m_pageMaxAge{};
    Dim::Duration m_pageScanInterval{};
    std::deque<uint64_t> m_stableLsns;
    uint64_t m_stableLsn{0};
    uint64_t m_flushLsn{0};
    Dim::TaskProxy m_flushTask;

    DbReadView m_vdata;
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
        Dim::TimePoint creation,
        DbSampleType sampleType,
        Dim::Duration retention,
        Dim::Duration interval
    );
    void logMetricUpdate(
        uint32_t pgno,
        Dim::TimePoint creation,
        DbSampleType sampleType,
        Dim::Duration retention,
        Dim::Duration interval
    );
    void logMetricClearSamples(uint32_t pgno);
    void logMetricUpdateSamples(
        uint32_t pgno,
        size_t refPos,
        uint32_t refPage,
        Dim::TimePoint refTime,
        bool updateIndex
    );
    void logSampleInit(
        uint32_t pgno,
        uint32_t id,
        DbSampleType sampleType,
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
        assert(ptr->hdr.type == ptr->s_pageType);
    }
    return ptr;
}


/****************************************************************************
*
*   DbData
*
***/

class DbData : public DbLog::IApplyNotify, public Dim::HandleContent {
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
        Dim::TimePoint pageFirstTime; // time of first sample on last page
        uint32_t infoPage;
        uint32_t lastPage; // page with most recent samples
        uint16_t pageLastSample; // position of last sample on last page
        DbSampleType sampleType;
    };

public:
    // Reads the file header to determine the page size, returns 0 if the
    // file doesn't exist, access is denied, or has an invalid header.
    static size_t queryPageSize(Dim::FileHandle f);

public:
    ~DbData();

    // Allows updates from DbLog to be applied
    void openForApply(size_t pageSize, DbOpenFlags flags);

    // After open metrics and samples can be updated and queried
    bool openForUpdate(
        DbTxn & txn,
        IDbDataNotify * notify,
        std::string_view name,
        DbOpenFlags flags
    );
    DbStats queryStats();

    void insertMetric(DbTxn & txn, uint32_t id, std::string_view name);
    bool eraseMetric(std::string * outName, DbTxn & txn, uint32_t id);
    void updateMetric(
        DbTxn & txn,
        uint32_t id,
        const DbMetricInfo & info
    );
    void getMetricInfo(
        IDbDataNotify * notify,
        const DbTxn & txn,
        uint32_t id
    );

    void updateSample(
        DbTxn & txn,
        uint32_t id,
        Dim::TimePoint time,
        double value
    );
    void getSamples(
        DbTxn & txn,
        IDbDataNotify * notify,
        uint32_t id,
        Dim::TimePoint first,
        Dim::TimePoint last,
        unsigned presamples
    );

    // Inherited via IApplyNotify
    void onLogApplyCommitCheckpoint(uint64_t lsn, uint64_t startLsn) override;
    void onLogApplyBeginTxn(uint64_t lsn, uint16_t localTxn) override;
    void onLogApplyCommitTxn(uint64_t lsn, uint16_t localTxn) override;

    void onLogApplyZeroInit(void * ptr) override;
    void onLogApplyPageFree(void * ptr) override;
    void onLogApplySegmentUpdate(
        void * ptr,
        uint32_t refPage,
        bool free
    ) override;
    void onLogApplyRadixInit(
        void * ptr,
        uint32_t id,
        uint16_t height,
        const uint32_t * firstPgno,
        const uint32_t * lastPgno
    ) override;
    void onLogApplyRadixErase(
        void * ptr,
        size_t firstPos,
        size_t lastPos
    ) override;
    void onLogApplyRadixPromote(void * ptr, uint32_t refPage) override;
    void onLogApplyRadixUpdate(
        void * ptr,
        size_t pos,
        uint32_t refPage
    ) override;
    void onLogApplyMetricInit(
        void * ptr,
        uint32_t id,
        std::string_view name,
        Dim::TimePoint creation,
        DbSampleType sampleType,
        Dim::Duration retention,
        Dim::Duration interval
    ) override;
    void onLogApplyMetricUpdate(
        void * ptr,
        Dim::TimePoint creation,
        DbSampleType sampleType,
        Dim::Duration retention,
        Dim::Duration interval
    ) override;
    void onLogApplyMetricClearSamples(void * ptr);
    void onLogApplyMetricUpdateSamples(
        void * ptr,
        size_t pos,
        uint32_t refPage,
        Dim::TimePoint refTime,
        bool updateIndex
    ) override;
    void onLogApplySampleInit(
        void * ptr,
        uint32_t id,
        DbSampleType sampleType,
        Dim::TimePoint pageTime,
        size_t lastSample
    ) override;
    void onLogApplySampleUpdate(
        void * ptr,
        size_t firstPos,
        size_t lastPos,
        double value,
        bool updateLast
    ) override;
    void onLogApplySampleUpdateTime(
        void * ptr,
        Dim::TimePoint pageTime
    ) override;

private:
    RadixData * radixData(DbPageHeader * hdr) const;
    RadixData * radixData(MetricPage * mp) const;
    const RadixData * radixData(const DbPageHeader * hdr) const;

    bool loadMetrics(
        DbTxn & txn,
        IDbDataNotify * notify,
        uint32_t pgno
    );
    void metricDestructPage(DbTxn & txn, uint32_t pgno);

    bool loadFreePages(DbTxn & txn);
    uint32_t allocPgno(DbTxn & txn);
    void freePage(DbTxn & txn, uint32_t pgno);

    uint16_t entriesPerMetricPage() const;
    uint16_t entriesPerRadixPage() const;
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

    size_t samplesPerPage(DbSampleType type) const;
    MetricPosition getMetricPos(uint32_t id) const;
    void setMetricPos(uint32_t id, const MetricPosition & mi);
    MetricPosition loadMetricPos(const DbTxn & txn, uint32_t id);
    MetricPosition loadMetricPos(
        DbTxn & txn,
        uint32_t id,
        Dim::TimePoint time
    );

    bool m_verbose{false};
    size_t m_segmentSize = 0;
    size_t m_pageSize = 0;

    mutable std::shared_mutex m_mposMut;
    std::vector<MetricPosition> m_metricPos;
    unsigned m_numMetrics = 0;

    std::recursive_mutex m_pageMut;
    size_t m_numPages = 0;
    Dim::UnsignedSet m_freePages;
    size_t m_numFreed = 0;

    // used to manage the index at kMetricIndexPageNum
    std::mutex m_mndxMut;
};
