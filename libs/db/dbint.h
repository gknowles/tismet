// Copyright Glen Knowles 2017 - 2022.
// Distributed under the Boost Software License, Version 1.0.
//
// dbint.h - tismet db
#pragma once


/****************************************************************************
*
*   Declarations
*
***/

constexpr unsigned kDefaultPageSize = 4096;
static_assert(kDefaultPageSize == std::bit_ceil(kDefaultPageSize));

constexpr unsigned kMinPageSize = 128;
static_assert(kDefaultPageSize % kMinPageSize == 0);

static_assert(std::is_same_v<std::underlying_type_t<pgno_t>, uint32_t>);
constexpr auto kMaxPageNum = (pgno_t) 0x7fff'ffff;
constexpr auto kFreePageMark = (pgno_t) 0xffff'ffff;

constexpr int kMaxVirtualSample = 0x3fff'ffff;
constexpr int kMinVirtualSample = -kMaxVirtualSample;


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
    void growToFit(pgno_t pgno);

    const void * rptr(pgno_t pgno) const;
    size_t pageSize() const { return m_pageSize; }
    size_t viewSize() const { return m_viewSize; }

    pgno_t pgno(const void * ptr) const;

protected:
    using Pointer = std::conditional_t<Writable, char *, const char *>;
    static constexpr Dim::File::View kMode = Writable
        ? Dim::File::View::kReadWrite
        : Dim::File::View::kReadOnly;
    size_t minFirstSize() const;
    Pointer ptr(pgno_t pgno) const;

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
    Pointer wptr(pgno_t pgno) const;
};


/****************************************************************************
*
*   DbPage
*
***/

class DbPage : public DbLog::IPageNotify {
public:
    DbPage();
    ~DbPage();

    bool open(
        std::string_view datafile,
        std::string_view workfile,
        size_t pageSize,
        Dim::EnumFlags<DbOpenFlags> flags
    );
    void close();
    DbConfig configure(const DbConfig & conf);
    void growToFit(pgno_t pgno);

    const void * rptr(uint64_t lsn, pgno_t pgno) const;
    size_t pageSize() const { return m_pageSize; }
    size_t viewSize() const { return m_vwork.viewSize(); }
    size_t size() const { return m_pages.size(); }

    Dim::FileHandle dataFile() const { return m_fdata; }
    bool newFiles() const { return m_newFiles; }

private:
    struct WorkPageInfo;

    bool openData(std::string_view datafile);
    bool openWork(std::string_view workfile);
    void writePageWait(DbPageHeader * hdr);
    void freePage_LK(DbPageHeader * hdr);
    DbPageHeader * dupPage_LK(const DbPageHeader * hdr);
    void * dirtyPage_LK(pgno_t pgno, uint64_t lsn);
    WorkPageInfo * allocWorkInfo_LK();
    void freeWorkInfo_LK(WorkPageInfo * pi);

    // Inherited by DbLog::IPageNotify
    void * onLogGetUpdatePtr(
        pgno_t pgno,
        uint64_t lsn,
        uint16_t txn
    ) override;
    void * onLogGetRedoPtr(pgno_t pgno, uint64_t lsn, uint16_t txn) override;
    void onLogStable(uint64_t lsn, size_t bytes) override;
    uint64_t onLogCheckpointPages(uint64_t lsn) override;

    Dim::Duration untilNextSave_LK();
    void queueSaveWork_LK();
    Dim::Duration onSaveTimer(Dim::TimePoint now);
    void removeWalPages_LK(uint64_t saveLsn);
    void saveOldPages_LK();
    void saveWork();

    // Variables determined at open
    size_t m_pageSize{0};
    Dim::EnumFlags<DbOpenFlags> m_flags{};
    bool m_newFiles{false}; // did the open create new data files?

    // Configuration settings
    Dim::Duration m_maxDirtyAge{};
    size_t m_maxDirtyData{};


    mutable std::mutex m_workMut;

    bool m_saveInProgress{false}; // is saveWork() task running?

    // Info about work pages that have been modified in memory but not yet
    // written to disk.
    struct WorkPageInfo : Dim::ListLink<> {
        DbPageHeader * hdr;
        Dim::TimePoint firstTime; // time page became dirty
        uint64_t firstLsn; // LSN at which page became dirty
        pgno_t pgno;
        Dim::EnumFlags<DbPageFlags> flags;
    };
    // List of all dirty pages in order of when they became dirty as measured
    // by LSN (and therefore also time).
    Dim::List<WorkPageInfo> m_dirtyPages;
    // Static copies of old versions of dirty pages, that aren't yet stable,
    // waiting to be written.
    Dim::List<WorkPageInfo> m_oldPages;
    // Clean pages that were recently dirty in the order they became clean.
    Dim::List<WorkPageInfo> m_cleanPages;
    // Number of pages, dirty or clean, that haven't had their cleaning cost
    // fully repaid.
    size_t m_pageDebt{0};
    Dim::List<WorkPageInfo> m_freeInfos;

    // One entry for every data page, null for untracked pages (which must
    // therefore also be unmodified pages).
    std::vector<WorkPageInfo *> m_pages;

    // The LSN up to which all data can be safely recovered. All WAL for any
    // transaction, that has not been rolled back and includes logs from this
    // or any previous LSN, has been persisted to stable storage.
    uint64_t m_stableLsn{0};

    // Info about WAL pages that have been persisted but with some or all of
    // their corresponding data pages still dirty. Used to pace the speed at
    // which dirty pages are written.
    struct WalPageInfo {
        uint64_t lsn; // first LSN on the page
        Dim::TimePoint time; // time page became stable
        size_t bytes; // bytes on the page
    };
    // Stable WAL pages that are within the "checkpoint bytes" threshold
    std::deque<WalPageInfo> m_currentWal;
    // Stable WAL pages older than the "checkpoint bytes" threshold
    std::deque<WalPageInfo> m_overflowWal;
    // Sum of bytes in overflow WAL pages
    size_t m_overflowBytes{0};
    // Sum of bytes in all stable WAL pages (both current and overflow)
    size_t m_stableBytes{0};

    DbReadView m_vdata;
    Dim::FileHandle m_fdata;
    DbWriteView m_vwork;
    Dim::FileHandle m_fwork;
    size_t m_workPages{0};
    Dim::UnsignedSet m_freeWorkPages;

    Dim::TimerProxy m_saveTimer;
    // Last time at which the save timer ran.
    Dim::TimePoint m_lastSaveTime;
};


/****************************************************************************
*
*   DbTxn
*
***/

enum DbLogRecType : int8_t;

class DbTxn {
public:
    DbTxn(DbLog & log, DbPage & page);
    ~DbTxn();

    template<typename T> const T * viewPage(pgno_t pgno) const;
    size_t pageSize() const { return m_page.pageSize(); }
    size_t numPages() const { return m_page.size(); }
    void growToFit(pgno_t pgno) { m_page.growToFit(pgno); }

    void logZeroInit(pgno_t pgno);
    void logTagRootUpdate(pgno_t pgno, pgno_t rootPage);
    void logPageFree(pgno_t pgno);

    std::pair<void *, size_t> allocFullPage(pgno_t pgno, size_t bytes);
    // "bytes" must be less or equal to amount passed in to preceding 
    // allocFullPage() call.
    void logFullPageInit(DbPageType type, uint32_t id, size_t bytes);

    void logFullPageInit(
        pgno_t pgno, 
        DbPageType type, 
        uint32_t id, 
        std::span<uint8_t> data
    );

    void logRadixInit(
        pgno_t pgno,
        uint32_t id,
        uint16_t height,
        const pgno_t * firstPage,
        const pgno_t * lastPage
    );
    void logRadixErase(pgno_t pgno, size_t firstPos, size_t lastPos);
    void logRadixPromote(pgno_t pgno, pgno_t refPage);
    void logRadixUpdate(pgno_t pgno, size_t pos, pgno_t refPage);
    void logBitInit(
        pgno_t, 
        uint32_t id, 
        uint32_t base, 
        bool fill, 
        size_t pos = -1
    );
    void logBitUpdate(pgno_t, size_t firstPos, size_t lastPos, bool value);
    void logMetricInit(
        pgno_t pgno,
        uint32_t id,
        std::string_view name,
        Dim::TimePoint creation,
        DbSampleType sampleType,
        Dim::Duration retention,
        Dim::Duration interval
    );
    void logMetricUpdate(
        pgno_t pgno,
        Dim::TimePoint creation,
        DbSampleType sampleType,
        Dim::Duration retention,
        Dim::Duration interval
    );
    void logMetricClearSamples(pgno_t pgno);
    void logMetricUpdateSamplesTxn(pgno_t pgno, size_t refSample);
    void logMetricUpdateSamples(
        pgno_t pgno,
        size_t refPos,
        Dim::TimePoint refTime,
        size_t refSample,
        pgno_t refPage
    );
    void logSampleInit(
        pgno_t pgno,
        uint32_t id,
        DbSampleType sampleType,
        Dim::TimePoint pageTime,
        size_t lastSample
    );
    void logSampleInit(
        pgno_t pgno,
        uint32_t id,
        DbSampleType sampleType,
        Dim::TimePoint pageTime,
        size_t lastSample,
        double fill
    );
    void logSampleUpdateTxn(
        pgno_t pgno,
        size_t pos,
        double value,
        bool updateLast
    );
    void logSampleUpdate(
        pgno_t pgno,
        size_t firstSample,
        size_t lastSample,
        double value,
        bool updateLast
    );
    void logSampleUpdateTime(pgno_t pgno, Dim::TimePoint pageTime);

private:
    template<typename T>
    std::pair<T *, size_t> alloc(
        DbLogRecType type,
        pgno_t pgno,
        size_t bytes = sizeof(T)
    );
    std::pair<void *, size_t> alloc(
        DbLogRecType type,
        pgno_t pgno,
        size_t bytes
    );
    void log(DbLog::Record * rec, size_t bytes);

    DbLog & m_log;
    DbPage & m_page;
    uint64_t m_txn{0};
    std::string m_buffer;
};

//===========================================================================
template<typename T>
const T * DbTxn::viewPage(pgno_t pgno) const {
    auto lsn = DbLog::getLsn(m_txn);
    auto ptr = static_cast<const T *>(m_page.rptr(lsn, pgno));
    if constexpr (!std::is_same_v<T, DbPageHeader>) {
        // Must start with and be layout compatible with DbPageHeader
        assert((std::is_same_v<decltype(ptr->hdr), DbPageHeader>));
        assert(intptr_t(ptr) == intptr_t(&ptr->hdr));
        assert(ptr->hdr.type == ptr->kPageType);
    }
    return ptr;
}

//===========================================================================
template<typename T>
std::pair<T *, size_t> DbTxn::alloc(
    DbLogRecType type,
    pgno_t pgno,
    size_t bytes
) {
    assert(bytes >= sizeof(T));
    auto&& [ptr, count] = alloc(type, pgno, bytes);
    return {(T *) ptr, count};
}


/****************************************************************************
*
*   DbPageHeap
*
***/

class DbPageHeap : public Dim::IPageHeap {
public:
    DbPageHeap(DbTxn & txn, DbData & data);

    // Inherited via IPageHeap
    size_t create() override;
    void destroy(size_t pgno) override;
    void setRoot(size_t pgno) override;
    size_t root() const override;
    size_t pageSize() const override;
    bool empty() const override;
    bool empty(size_t pgno) const override;
    uint8_t * wptr(size_t pgno) override;
    const uint8_t * ptr(size_t pgno) const override;

private:
    void commitIfPending() const;

    DbData & m_data;
    DbTxn & m_txn;
    pgno_t m_updatePgno = pgno_t::npos;
    mutable uint8_t * m_updatePtr = {};
};


/****************************************************************************
*
*   DbData
*
***/

class DbData : public DbLog::IApplyNotify, public Dim::HandleContent {
public:
    struct ZeroPage;
    struct FreePage;
    struct RadixPage;
    struct BitmapPage;
    struct MetricPage;
    struct SamplePage;
    struct TriePage;

    struct RadixData {
        // Distance from leaf radix pages. Therefore initialized to 0 when
        // the root page is created, and increased by 1 each time the root 
        // page is promoted.
        uint16_t height;    

        uint16_t numPages;

        // EXTENDS BEYOND END OF STRUCT
        pgno_t pages[3];

        const pgno_t * begin() const { return pages; }
        const pgno_t * end() const { return pages + numPages; }
    };

    struct MetricPosition {
        Dim::Duration interval;
        Dim::TimePoint pageFirstTime; // time of first sample on last page
        pgno_t infoPage;
        pgno_t lastPage; // page with most recent samples
        uint16_t pageLastSample; // position of last sample on last page
        DbSampleType sampleType;
    };

public:
    static uint16_t entriesPerMetricPage(size_t pageSize);
    static size_t metricNameSize(size_t pageSize);
    static RadixData * radixData(MetricPage * mp, size_t pageSize);

    static uint16_t entriesPerRadixPage(size_t pageSize);

    static RadixData * radixData(DbPageHeader * hdr, size_t pageSize);
    static const RadixData * radixData(
        const DbPageHeader * hdr, 
        size_t pageSize
    );

public:
    ~DbData();

    // Allows updates from DbLog to be applied, pageSize *MUST* match page
    // size of existing data file.
    void openForApply(size_t pageSize, Dim::EnumFlags<DbOpenFlags> flags);

    // After open metrics and samples can be updated and queried
    bool openForUpdate(
        DbTxn & txn,
        IDbDataNotify * notify,
        std::string_view name,
        Dim::EnumFlags<DbOpenFlags> flags
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
    void onLogApplyTagRootUpdate(void * ptr, pgno_t rootPage) override;
    void onLogApplyPageFree(void * ptr) override;
    void onLogApplyFullPageInit(
        void * ptr,
        DbPageType type,
        uint32_t id,
        std::span<const uint8_t> data
    ) override;
    void onLogApplyRadixInit(
        void * ptr,
        uint32_t id,
        uint16_t height,
        const pgno_t * firstPgno,
        const pgno_t * lastPgno
    ) override;
    void onLogApplyRadixErase(
        void * ptr,
        size_t firstPos,
        size_t lastPos
    ) override;
    void onLogApplyRadixPromote(void * ptr, pgno_t refPage) override;
    void onLogApplyRadixUpdate(
        void * ptr,
        size_t pos,
        pgno_t refPage
    ) override;
    void onLogApplyBitInit(
        void * ptr,
        uint32_t id,
        uint32_t base,
        bool fill,
        uint32_t pos
    ) override;
    void onLogApplyBitUpdate(
        void * ptr,
        uint32_t firstPos,
        uint32_t lastPos,
        bool value
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
        Dim::TimePoint refTime,
        size_t refSample,
        pgno_t refPage
    ) override;
    void onLogApplySampleInit(
        void * ptr,
        uint32_t id,
        DbSampleType sampleType,
        Dim::TimePoint pageTime,
        size_t lastSample,
        double fill
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
    friend DbPageHeap;

    bool loadMetric(
        DbTxn & txn,
        IDbDataNotify * notify,
        const DbPageHeader & hdr
    );
    bool loadMetrics(DbTxn & txn, IDbDataNotify * notify);
    void metricDestructPage(DbTxn & txn, pgno_t pgno);
    void metricClearCounters();

    bool loadFreePages(DbTxn & txn);
    bool loadDeprecatedPages(DbTxn & txn);
    pgno_t allocPgno(DbTxn & txn);
    void freePage(DbTxn & txn, pgno_t pgno);
    void deprecatePage(DbTxn & txn, pgno_t pgno);
    void freeDeprecatedPage(DbTxn & txn, pgno_t pgno);

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
        pgno_t root,
        size_t firstPos,
        size_t lastPos
    );
    void radixDestructPage(DbTxn & txn, pgno_t pgno);
    bool radixInsertOrAssign(
        DbTxn & txn,
        pgno_t root,
        size_t pos,
        pgno_t value
    );

    // Returns false if pos is past the end of the index.
    bool radixFind(
        DbTxn & txn,
        const DbPageHeader ** hdr,
        const RadixData ** rd,
        size_t * rpos,
        pgno_t root,
        size_t pos
    );
    // Returns false if no value was found at the position, including if it's
    // past the end of the index.
    bool radixFind(DbTxn & txn, pgno_t * out, pgno_t root, size_t pos);

    // Calls fn for each page in index, exits immediately if an fn() call 
    // returns false. Returns true if no fn() calls returned false.
    bool radixVisit(
        DbTxn & txn,
        pgno_t root,
        const std::function<bool(DbTxn&, uint32_t, const DbPageHeader&)> & fn
    );

    bool bitUpsert(
        DbTxn & txn, 
        pgno_t root, 
        uint32_t id, 
        size_t firstPos, 
        size_t lastPos,
        bool value
    );
    bool bitLoad(DbTxn & txn, Dim::UnsignedSet * out, pgno_t root);
    size_t bitsPerPage() const;

    pgno_t sampleMakePhysical(
        DbTxn & txn,
        uint32_t id,
        DbData::MetricPosition & mi,
        size_t sppos,
        Dim::TimePoint pageTime,
        size_t lastSample,
        pgno_t vpage = {}
    );
    bool sampleTryMakeVirtual(DbTxn & txn, MetricPosition & mi, pgno_t spno);
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
    size_t m_pageSize = 0;
    pgno_t m_freeStoreRoot = {};
    pgno_t m_deprecatedStoreRoot = {};
    pgno_t m_metricStoreRoot = {};
    pgno_t m_metricTagStoreRoot = {};

    mutable std::shared_mutex m_mposMut;
    std::vector<MetricPosition> m_metricPos;
    unsigned m_numMetrics = 0;

    std::recursive_mutex m_pageMut;
    size_t m_numPages = 0;
    Dim::UnsignedSet m_freePages;
    size_t m_numFreed = 0;
    Dim::UnsignedSet m_deprecatedPages;

    // used to manage the index at kMetricIndexPageNum
    std::mutex m_mndxMut;
};
