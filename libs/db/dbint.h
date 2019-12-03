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

const unsigned kDefaultPageSize = 4096;
static_assert(kDefaultPageSize == Dim::pow2Ceil(kDefaultPageSize));

const unsigned kMinPageSize = 128;
static_assert(kDefaultPageSize % kMinPageSize == 0);

static_assert(std::is_same_v<std::underlying_type_t<pgno_t>, uint32_t>);
const auto kMaxPageNum = (pgno_t) 0x7fff'ffff;
const auto kFreePageMark = (pgno_t) 0xffff'ffff;

const int kMaxVirtualSample = 0x3fff'ffff;
const int kMinVirtualSample = -kMaxVirtualSample;


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
    static constexpr Dim::File::ViewMode kMode = Writable
        ? Dim::File::kViewReadWrite
        : Dim::File::kViewReadOnly;
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
    void * wptr(pgno_t pgno) const;
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
        DbOpenFlags flags
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
    DbOpenFlags m_flags{};
    bool m_newFiles{false}; // did the open create new data files?

    // Configuration settings
    Dim::Duration m_maxDirtyAge{};
    size_t m_maxDirtyData{};


    mutable std::mutex m_workMut;

    bool m_saveInProgress{false}; // is saveWork() task running?

    // Info about work pages that have been modified in memory but not yet
    // written to disk.
    struct WorkPageInfo : Dim::ListBaseLink<> {
        DbPageHeader * hdr;
        Dim::TimePoint firstTime; // time page became dirty
        uint64_t firstLsn; // LSN at which page became dirty
        pgno_t pgno;
        DbPageFlags flags;
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
    void logZeroUpdateRoots(
        pgno_t pgno,
        pgno_t infoRootPage,
        pgno_t nameRootPage,
        pgno_t idRootPage
    );
    void logPageFree(pgno_t pgno);
    void logSegmentUpdate(pgno_t pgno, pgno_t refPage, bool free);
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

    void logIndexLeafInit(pgno_t pgno, uint32_t id);

    void logMetricInit(
        pgno_t pgno,
        uint32_t id,
        Dim::TimePoint creation,
        Dim::Duration retention
    );
    void logMetricUpdate(
        pgno_t pgno,
        Dim::TimePoint creation,
        Dim::Duration retention
    );
    void logMetricEraseSamples(
        pgno_t pgno,
        size_t count,
        Dim::TimePoint lastIndexTime = {}
    );
    void logMetricUpdateSample(
        pgno_t pgno,
        size_t pos,
        double value,
        double oldValue
    );
    void logMetricInsertSample(
        pgno_t pgno,
        size_t pos,
        Dim::Duration dt,
        double value,
        double oldValue
    );
    void logMetricInsertSampleTxn(
        pgno_t pgno,
        size_t pos,
        Dim::Duration dt,
        double value,
        double oldValue
    );
    void logSampleInit(pgno_t pgno, uint32_t id);
    void logSampleUpdate(
        pgno_t pgno,
        size_t offset,
        std::string_view data,
        size_t unusedBits = -1
    );

private:
    template<typename T>
    std::pair<T *, size_t> alloc(
        DbLogRecType type,
        pgno_t pgno,
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
    struct IndexPage;
    struct MetricPage;
    struct SamplePage;

    struct RadixData {
        uint16_t height;
        uint16_t numPages;

        // EXTENDS BEYOND END OF STRUCT
        pgno_t pages[3];

        const pgno_t * begin() const { return pages; }
        const pgno_t * end() const { return pages + numPages; }
    };
    struct RadixPage {
        static const auto kPageType = DbPageType::kRadix;
        DbPageHeader hdr;

        // EXTENDS BEYOND END OF STRUCT
        RadixData rd;
    };
    struct IndexPage {
        DbPageHeader hdr;
        uint16_t ndxHeight;
        uint16_t ndxUsed;
        uint16_t ndxAvail;
    };

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
        Dim::TimePoint last
    );

    // Inherited via IApplyNotify
    void onLogApplyCommitCheckpoint(uint64_t lsn, uint64_t startLsn) override;
    void onLogApplyBeginTxn(uint64_t lsn, uint16_t localTxn) override;
    void onLogApplyCommitTxn(uint64_t lsn, uint16_t localTxn) override;

    void onLogApplyZeroInit(void * ptr) override;
    void onLogApplyZeroUpdateRoots(
        void * ptr,
        pgno_t infoRoot,
        pgno_t nameRoot,
        pgno_t idRoot
    ) override;
    void onLogApplyPageFree(void * ptr) override;
    void onLogApplySegmentUpdate(
        void * ptr,
        pgno_t refPage,
        bool free
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
    void onLogApplyIndexLeafInit(void * ptr, uint32_t id) override;
    void onLogApplyMetricInit(
        void * ptr,
        uint32_t id,
        Dim::TimePoint creation,
        Dim::Duration retention
    ) override;
    void onLogApplyMetricUpdate(
        void * ptr,
        Dim::TimePoint creation,
        Dim::Duration retention
    ) override;
    void onLogApplyMetricEraseSamples(
        void * ptr,
        size_t count,
        Dim::TimePoint lastIndexTime
    ) override;
    void onLogApplyMetricUpdateSample(
        void * ptr,
        size_t pos,
        double value,
        double dv
    ) override;
    void onLogApplyMetricInsertSample(
        void * ptr,
        size_t pos,
        Dim::Duration dt,
        double value,
        double dv
    ) override;
    void onLogApplySampleInit(void * ptr, uint32_t id) override;
    void onLogApplySampleUpdate(
        void * ptr,
        size_t offset,
        std::string_view data,
        size_t unusedBits
    ) override;

private:
    RadixData * radixData(DbPageHeader * hdr) const;
    RadixData * radixData(MetricPage * mp) const;
    const RadixData * radixData(const DbPageHeader * hdr) const;

    bool findMetricInfoPage(const DbTxn & txn, pgno_t * out, uint32_t id) const;
    bool findMetricName(
        const DbTxn & txn,
        std::string * out,
        uint32_t id
    ) const;
    bool findMetricId(
        const DbTxn & txn,
        uint32_t * id,
        std::string_view name
    ) const;
    bool loadMetrics(
        DbTxn & txn,
        IDbDataNotify * notify,
        pgno_t infoRootPage
    );
    void metricDestructPage(DbTxn & txn, pgno_t pgno);
    size_t metricNameSize() const;
    void metricClearCounters();
    size_t maxSamples(const MetricPage * mp) const;
    size_t maxData(const SamplePage * sp) const;

    void sampleIndexErase(
        DbTxn & txn,
        pgno_t mpno,
        Dim::TimePoint first,
        Dim::TimePoint last
    );
    void sampleIndexMerge(
        DbTxn & txn,
        pgno_t mpno,
        const DbSample samples[],
        size_t count
    );
    void sampleIndexSplit(
        DbTxn & txn,
        pgno_t mpno,
        const SamplePage * sp
    );

    bool loadFreePages(DbTxn & txn);
    pgno_t allocPgno(DbTxn & txn);
    void freePage(DbTxn & txn, pgno_t pgno);

    static std::string toKey(uint32_t id);
    static std::string toKey(Dim::TimePoint time);
    static bool fromKey(uint32_t * out, std::string_view src);
    static bool fromKey(pgno_t * out, std::string_view src);

    void indexDestructPage(DbTxn & txn, pgno_t pgno);
    void indexDestruct(DbTxn & txn, const DbPageHeader & hdr);
    void indexErase(DbTxn & txn, pgno_t root, std::string_view key);
    bool indexUpdate(
        DbTxn & txn,
        pgno_t root,
        std::string_view key,
        std::string_view data
    );

    bool indexFindLeafPgno(
        const DbTxn & txn,
        pgno_t * out,
        pgno_t root,
        std::string_view key,
        bool after = false
    );
    bool indexInsertLeafPgno(
        DbTxn & txn,
        pgno_t root,
        std::string_view key,
        pgno_t pgno
    );
    bool indexEraseLeafPgno(
        DbTxn & txn,
        pgno_t root,
        std::string_view key,
        pgno_t pgno
    );

    bool indexFind(
        const DbTxn & txn,
        uint32_t * out,
        pgno_t root,
        std::string_view key
    ) const;
    bool indexFind(
        const DbTxn & txn,
        std::string * out,
        pgno_t root,
        std::string_view key
    ) const;

    uint16_t entriesPerRadixPage() const;
    size_t radixPageEntries(
        int * ents,
        size_t maxEnts,
        DbPageType rootType,
        uint16_t height,
        size_t pos
    ) const;
    void radixDestruct(DbTxn & txn, const DbPageHeader & hdr);
    void radixErase(
        DbTxn & txn,
        const DbPageHeader & hdr,
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
        const DbTxn & txn,
        const DbPageHeader ** hdr,
        const RadixData ** rd,
        size_t * rpos,
        pgno_t root,
        size_t pos
    ) const;
    // Returns false if no value was found at the position, including if it's
    // past the end of the index.
    bool radixFind(
        const DbTxn & txn,
        pgno_t * out,
        pgno_t root,
        size_t pos
    ) const;

    bool m_verbose{false};
    size_t m_segmentSize = 0;
    size_t m_pageSize = 0;

    std::recursive_mutex m_pageMut;
    size_t m_numPages = 0;
    Dim::UnsignedSet m_freePages;
    size_t m_numFreed = 0;

    // used to manage the page, id, and name indexes
    mutable std::shared_mutex m_mndxMut;
    pgno_t m_infoIndexRoot{};
    pgno_t m_idIndexRoot{};
    pgno_t m_nameIndexRoot{};
    unsigned m_numMetrics = 0;
};
