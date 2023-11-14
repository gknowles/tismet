// Copyright Glen Knowles 2017 - 2023.
// Distributed under the Boost Software License, Version 1.0.
//
// dbint.h - tismet db
#pragma once


/****************************************************************************
*
*   Tuning parameters
*
***/

constexpr unsigned kDefaultPageSize = 4096;
static_assert(kDefaultPageSize == std::bit_ceil(kDefaultPageSize));

constexpr unsigned kMinPageSize = 128;
static_assert(kDefaultPageSize % kMinPageSize == 0);

constexpr unsigned kMaxActiveRootUpdates = 4;


/****************************************************************************
*
*   Declarations
*
***/

static_assert(std::is_same_v<std::underlying_type_t<pgno_t>, uint32_t>);
constexpr auto kMaxPageNum = (pgno_t) 0x7fff'ffff;
constexpr auto kFreePageMark = (pgno_t) 0xffff'ffff;

constexpr int kMaxVirtualSample = 0x3fff'ffff;
constexpr int kMinVirtualSample = -kMaxVirtualSample;

// Forward declarations
class DbData;
class DbRootSet;


/****************************************************************************
*
*   DbFileView
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

class DbPage final : public DbWal::IPageNotify {
public:
    DbPage();
    ~DbPage();

    bool open(
        std::string_view datafile,
        std::string_view workfile,
        size_t pageSize,
        size_t walPageSize,
        Dim::EnumFlags<DbOpenFlags> flags
    );
    void close();
    DbConfig configure(const DbConfig & conf);
    void growToFit(pgno_t pgno);

    // Pins page in cache (if it was already cached) with a read pin, and
    // returns a pointer to it. Read pins prevent cached pages from being freed
    // by saveWork().
    const void * rptr(Lsn lsn, pgno_t pgno, bool withPin);
    void unpin(const Dim::UnsignedSet & pages);

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
    WorkPageInfo * dirtyPage_LK(pgno_t pgno, Lsn lsn);
    WorkPageInfo * allocWorkInfo_LK();
    void freeWorkInfo_LK(WorkPageInfo * pi);

    // Inherited by DbWal::IPageNotify
    void * onWalGetPtrForUpdate(
        pgno_t pgno,
        Lsn lsn,
        LocalTxn txn
    ) override;
    void onWalUnlockPtr(pgno_t pgno) override;
    void * onWalGetPtrForRedo(
        pgno_t pgno,
        Lsn lsn,
        LocalTxn txn
    ) override;
    void onWalDurable(Lsn lsn, size_t bytes) override;
    Lsn onWalCheckpointPages(Lsn lsn) override;

    Dim::Duration untilNextSave_LK();
    void queueSaveWork_LK();
    Dim::Duration onSaveTimer(Dim::TimePoint now);
    void saveWork();
    void saveOverduePages_LK();
    Lsn saveDirtyPages_LK(Dim::TimePoint lastSave);
    void removeWalPages_LK(Lsn saveLsn);
    void removeCleanPages_LK();

    // Variables determined at open
    size_t m_pageSize = 0;
    size_t m_walPageSize = 0;
    Dim::EnumFlags<DbOpenFlags> m_flags;
    bool m_newFiles = false; // did the open create new data files?

    // Configuration settings, these provide a soft cap that triggers the
    // process (i.e. check pointing) of making WAL discardable and then
    // discarding it.
    Dim::Duration m_maxWalAge = {};
    size_t m_maxWalBytes = 0;


    mutable std::mutex m_workMut;
    std::condition_variable m_workCv;

    bool m_saveInProgress = false; // is saveWork() task running?

    struct WorkPageInfoBase {
        DbPageHeader * hdr;
        Dim::TimePoint firstTime; // time page became dirty
        Lsn firstLsn; // LSN at which page became dirty
        pgno_t pgno;
        Dim::EnumFlags<DbPageFlags> flags;

        // Pins tell the page save algorithm when it is unsafe to save or free
        // work pages.
        //
        // Page is being accessed, must not be freed, but may be saved.
        unsigned readPins;
        // Request to update page has been made (and granted if readPins == 1).
        // If granted, may be internally inconsistent, and must not be saved.
        bool writePin;
    };
    // Info about work pages that have been modified in memory but not yet
    // written to disk.
    struct WorkPageInfo : Dim::ListLink<>, WorkPageInfoBase
    {};
    // One entry for every data page, null for untracked pages (which must
    // therefore also be unmodified pages).
    std::vector<WorkPageInfo *> m_pages;

    // Unused page info structs waiting to be recycled.
    Dim::List<WorkPageInfo> m_freeInfos;
    // List of all dirty pages in order of when they became dirty as measured
    // by LSN (and therefore also time).
    Dim::List<WorkPageInfo> m_dirtyPages;
    // Static copies of old versions of overdue dirty pages waiting for their
    // modifying LSNs to become durable so that they can be saved. These copies
    // are made so pages that are updated faster than LSNs are saved can
    // eventually be saved.
    Dim::List<WorkPageInfo> m_overduePages;
    // Pages that were recently dirty but might not yet be discardable, in the
    // order they became clean. Kept either to shadow overdue pages that can't
    // yet be saved, or because an active reader prevented it from being freed.
    Dim::List<WorkPageInfo> m_cleanPages;
    // Number of pages, dirty or clean, that first became dirty within the last
    // max WAL age. Which means that their repayment term hasn't fully matured.
    size_t m_pageBonds = 0;
    // Info about pages from the data file that are unchanged and have not been
    // copied to work pages. Used to track pins (that block modification) on
    // pages that are being referenced.
    Dim::List<WorkPageInfo> m_referencePages;

    // The LSN up to which all data can be safely recovered. All WAL for any
    // transaction, that has not been rolled back and includes logs from this or
    // any previous LSN, has been persisted to stable storage.
    Lsn m_durableLsn = {};

    // Info about WAL pages that have been persisted but with some or all of
    // their corresponding data pages still dirty. Used to pace the speed at
    // which dirty pages are written.
    struct WalPageInfo {
        Lsn lsn; // first LSN on the page
        Dim::TimePoint time; // time page became durable
        size_t bytes; // bytes on the page
    };
    // Durable WAL pages that are within the "checkpoint bytes" threshold.
    std::deque<WalPageInfo> m_currentWal;
    // Durable WAL pages older than the "checkpoint bytes" threshold, will be
    // freed as soon as all data pages still relying on them for indirect
    // durability can be written to stable storage.
    std::deque<WalPageInfo> m_overflowWal;
    // Sum of bytes in overflow WAL pages.
    size_t m_overflowWalBytes = 0;
    // Sum of bytes in all durable WAL pages (both current and overflow).
    size_t m_durableWalBytes = 0;

    DbReadView m_vdata;
    Dim::FileHandle m_fdata;
    DbWriteView m_vwork;
    Dim::FileHandle m_fwork;
    size_t m_workPages = 0;
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

enum DbWalRecType : int8_t;

class DbTxn {
public:
    // Used to track and then release a set of buffer pins without requiring
    // the transaction to end.
    class PinScope {
    public:
        PinScope(DbTxn & txn);
        ~PinScope();

        void close();
        void release();
        void keep(pgno_t pgno);

    private:
        DbTxn & m_txn;
        Dim::UnsignedSet m_prevPins;
        bool m_active = true;
    };

public:
    DbTxn(DbWal & wal, DbPage & page, std::shared_ptr<DbRootSet> roots);
    ~DbTxn();

    // Creates new DbTxn with same wal and page.
    DbTxn makeTxn() const;

    DbRootSet & roots() const { return *m_roots; }
    Lsx getLsx() const;

    // Returns pages that have been freed.
    Dim::UnsignedSet commit();

    size_t pageSize() const { return m_page.pageSize(); }
    size_t numPages() const { return m_page.size(); }
    template<typename T> const T * pin(pgno_t pgno);
    void growToFit(pgno_t pgno) { m_page.growToFit(pgno); }
    const Dim::UnsignedSet & freePages() const { return m_freePages; }

    void walZeroInit(pgno_t pgno);
    void walTagRootUpdate(pgno_t pgno, pgno_t rootPage);
    void walPageFree(pgno_t pgno);

    std::pair<void *, size_t> allocFullPage(pgno_t pgno, size_t bytes);
    // "bytes" must be less or equal to amount passed in to preceding
    // allocFullPage() call.
    void walFullPageInit(DbPageType type, uint32_t id, size_t bytes);

    void walFullPageInit(
        pgno_t pgno,
        DbPageType type,
        uint32_t id,
        std::span<uint8_t> data
    );

    void walRadixInit(
        pgno_t pgno,
        uint32_t id,
        uint16_t height,
        const pgno_t * firstPage,
        const pgno_t * lastPage
    );
    void walRadixErase(pgno_t pgno, size_t firstPos, size_t lastPos);
    void walRadixPromote(pgno_t pgno, pgno_t refPage);
    void walRadixUpdate(pgno_t pgno, size_t pos, pgno_t refPage);
    void walBitInit(
        pgno_t pgno,
        uint32_t id,
        uint32_t base,
        bool fill,
        size_t pos = -1
    );
    void walBitUpdate(
        pgno_t pgno,
        size_t firstPos,
        size_t lastPos,
        bool value
    );
    void walMetricInit(
        pgno_t pgno,
        uint32_t id,
        std::string_view name,
        Dim::TimePoint creation,
        DbSampleType sampleType,
        Dim::Duration retention,
        Dim::Duration interval
    );
    void walMetricUpdate(
        pgno_t pgno,
        Dim::TimePoint creation,
        DbSampleType sampleType,
        Dim::Duration retention,
        Dim::Duration interval
    );
    void walMetricClearSamples(pgno_t pgno);
    void walMetricUpdateSamplesTxn(pgno_t pgno, size_t refSample);
    void walMetricUpdateSamples(
        pgno_t pgno,
        size_t refPos,
        Dim::TimePoint refTime,
        size_t refSample,
        pgno_t refPage
    );
    void walSampleInit(
        pgno_t pgno,
        uint32_t id,
        DbSampleType sampleType,
        Dim::TimePoint pageTime,
        size_t lastSample
    );
    void walSampleInit(
        pgno_t pgno,
        uint32_t id,
        DbSampleType sampleType,
        Dim::TimePoint pageTime,
        size_t lastSample,
        double fill
    );
    void walSampleUpdateTxn(
        pgno_t pgno,
        size_t pos,
        double value,
        bool updateLast
    );
    void walSampleUpdate(
        pgno_t pgno,
        size_t firstSample,
        size_t lastSample,
        double value,
        bool updateLast
    );
    void walSampleUpdateTime(pgno_t pgno, Dim::TimePoint pageTime);

private:
    template<typename T>
    std::pair<T *, size_t> alloc(
        DbWalRecType type,
        pgno_t pgno,
        size_t bytes = sizeof(T)
    );
    std::pair<void *, size_t> alloc(
        DbWalRecType type,
        pgno_t pgno,
        size_t bytes
    );
    void wal(DbWal::Record * rec, size_t bytes);
    void unpinAll();

    DbWal & m_wal;
    DbPage & m_page;
    Lsx m_txn = {};
    std::string m_buffer;
    mutable Dim::UnsignedSet m_pinnedPages;
    Dim::UnsignedSet m_freePages;
    std::shared_ptr<DbRootSet> m_roots;
};

//===========================================================================
template<typename T>
const T * DbTxn::pin(pgno_t pgno) {
    auto lsn = DbWal::getLsn(m_txn);
    auto withPin = m_pinnedPages.insert(pgno);
    auto ptr = static_cast<const T *>(m_page.rptr(lsn, pgno, withPin));
    if constexpr (!std::is_same_v<T, DbPageHeader>) {
        // Must start with and be layout compatible with DbPageHeader.
        assert((std::is_same_v<decltype(ptr->hdr), DbPageHeader>));
        assert(intptr_t(ptr) == intptr_t(&ptr->hdr));
        assert(ptr->hdr.type == ptr->kPageType
            || ptr->hdr.type == DbPageType::kInvalid);
    }
    return ptr;
}

//===========================================================================
template<typename T>
std::pair<T *, size_t> DbTxn::alloc(
    DbWalRecType type,
    pgno_t pgno,
    size_t bytes
) {
    assert(bytes >= sizeof(T));
    auto&& [ptr, count] = alloc(type, pgno, bytes);
    return {(T *) ptr, count};
}


/****************************************************************************
*
*   DbRootVersion
*
*   Lifecycle
*   - initial committed root loaded from database
*   - readers access index via committed root
*   - first updater locks mutex
*   - last reference removed, free deprecated pages
*
***/

enum class IndexCode : int {
    kMetricName,
    kMetricNameSegs,
    kMetricTags,
    kNumIndexCodes,
};

struct DbRootVersion {
    // Root page of this version of the index.
    //  npos - there is no root
    //  0 - new root being created
    //  other - the root page
    pgno_t root = pgno_t::npos;

    // Next version of this index.
    std::shared_ptr<DbRootVersion> next;

    // Transaction that owns this version.
    Lsx lsx = {};

    // Pages used by this version that have subsequently been deprecated and
    // will be freed when all references to this version are gone.
    Dim::UnsignedSet deprecatedPages;

    // Transaction and data to be used when freeing the deprecated pages.
    DbTxn txn;
    DbData & data;

    DbRootVersion(DbTxn * txn, DbData * data);
    ~DbRootVersion();

    std::shared_ptr<DbRootVersion> addNextVer(Lsx txnId);
    bool complete() const { return root; }
};


/****************************************************************************
*
*   DbRootSet
*
***/

class DbRootSet : public std::enable_shared_from_this<DbRootSet> {
public:
    std::shared_ptr<DbRootVersion> name;

public:
    DbRootSet(
        DbData * data,
        std::shared_ptr<std::mutex> mut,
        std::shared_ptr<std::condition_variable> cv
    );

    std::vector<std::shared_ptr<DbRootVersion> *> firstRoots();

    // Returns position in vector of the one root ready to be updated. Updates
    // vector entry to point that current version of root and adds new
    // incomplete root as its next.
    std::pair<std::shared_ptr<DbRootVersion>, size_t> beginUpdate(
        Lsx txnId,
        const std::vector<std::shared_ptr<DbRootVersion>> & roots
    );
    void rollbackUpdate(std::shared_ptr<DbRootVersion> root);
    void commitUpdate(std::shared_ptr<DbRootVersion> root, pgno_t pgno);

    std::shared_ptr<DbRootSet> lockForCommit(Lsx txnId);

    // Returns set of transactions to commit as a group.
    std::unordered_set<Lsx> commit(Lsx txnId);

    std::shared_ptr<DbRootSet> publishNextSet(
        const std::unordered_set<Lsx> & txns
    );

    void unlock();

private:
    DbData & m_data;
    std::shared_ptr<std::mutex> m_mut;
    std::shared_ptr<std::condition_variable> m_cv;

    bool m_commitInProgress = false;
    std::shared_ptr<DbRootSet> m_next;

    // Ids of transactions that have, or are waiting to, make an update.
    std::unordered_set<Lsx> m_writeTxns;

    // Ids of the active transactions that aren't committed.
    std::unordered_set<Lsx> m_completeTxns;
};


/****************************************************************************
*
*   DbPageHeap
*
***/

class DbPageHeap final : public Dim::IPageHeap {
public:
    DbPageHeap(
        DbTxn * txn,
        DbData * data,
        pgno_t root,
        bool forUpdate
    );
    const Dim::UnsignedSet & destroyed() const { return m_destroyed; }

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
    bool releasePending(size_t pgno);

    DbTxn & m_txn;
    DbData & m_data;
    pgno_t m_root;
    Dim::UnsignedSet m_destroyed;
    pgno_t m_updatePgno = pgno_t::npos;
    mutable uint8_t * m_updatePtr = {};
};


/****************************************************************************
*
*   DbData
*
***/

class DbData final : public DbWal::IApplyNotify, public Dim::HandleContent {
public:
    struct ZeroPage;
    struct FreePage;
    struct RadixPage;
    struct BitmapPage;
    struct MetricPage;
    struct SamplePage;
    struct TriePage;

    struct RadixData {
        // Distance from leaf radix pages. Therefore initialized to 0 when the
        // root page is created, and increased by 1 each time the root page is
        // promoted.
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

    // Allows updates from DbWal to be applied, pageSize *MUST* match page size
    // of existing data file.
    void openForApply(size_t pageSize, Dim::EnumFlags<DbOpenFlags> flags);

    // Allows metrics and samples to be updated and queried. Must already be
    // open for apply.
    bool openForUpdate(
        DbTxn & txn,
        IDbDataNotify * notify,
        std::string_view name,
        Dim::EnumFlags<DbOpenFlags> flags
    );
    DbStats queryStats() const;
    void publishFreePages(const Dim::UnsignedSet & freePages);

    std::shared_ptr<DbRootSet> metricRootsInstance();

    void insertMetric(DbTxn & txn, uint32_t id, std::string_view name);
    bool eraseMetric(std::string * outName, DbTxn & txn, uint32_t id);
    void updateMetric(
        DbTxn & txn,
        uint32_t id,
        const DbMetricInfo & info
    );
    void getMetricInfo(IDbDataNotify * notify, DbTxn & txn, uint32_t id);

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
    void onWalApplyCheckpoint(Lsn lsn, Lsn startLsn) override;
    void onWalApplyBeginTxn(Lsn lsn, LocalTxn localTxn) override;
    void onWalApplyCommitTxn(Lsn lsn, LocalTxn localTxn) override;
    void onWalApplyGroupCommitTxn(
        Lsn lsn,
        const std::vector<LocalTxn> & localTxns
    ) override;

    void onWalApplyZeroInit(void * ptr) override;
    void onWalApplyTagRootUpdate(void * ptr, pgno_t rootPage) override;
    void onWalApplyPageFree(void * ptr) override;
    void onWalApplyFullPageInit(
        void * ptr,
        DbPageType type,
        uint32_t id,
        std::span<const uint8_t> data
    ) override;
    void onWalApplyRadixInit(
        void * ptr,
        uint32_t id,
        uint16_t height,
        const pgno_t * firstPgno,
        const pgno_t * lastPgno
    ) override;
    void onWalApplyRadixErase(
        void * ptr,
        size_t firstPos,
        size_t lastPos
    ) override;
    void onWalApplyRadixPromote(void * ptr, pgno_t refPage) override;
    void onWalApplyRadixUpdate(
        void * ptr,
        size_t pos,
        pgno_t refPage
    ) override;
    void onWalApplyBitInit(
        void * ptr,
        uint32_t id,
        uint32_t base,
        bool fill,
        uint32_t pos
    ) override;
    void onWalApplyBitUpdate(
        void * ptr,
        uint32_t firstPos,
        uint32_t lastPos,
        bool value
    ) override;
    void onWalApplyMetricInit(
        void * ptr,
        uint32_t id,
        std::string_view name,
        Dim::TimePoint creation,
        DbSampleType sampleType,
        Dim::Duration retention,
        Dim::Duration interval
    ) override;
    void onWalApplyMetricUpdate(
        void * ptr,
        Dim::TimePoint creation,
        DbSampleType sampleType,
        Dim::Duration retention,
        Dim::Duration interval
    ) override;
    void onWalApplyMetricClearSamples(void * ptr);
    void onWalApplyMetricUpdateSamples(
        void * ptr,
        size_t pos,
        Dim::TimePoint refTime,
        size_t refSample,
        pgno_t refPage
    ) override;
    void onWalApplySampleInit(
        void * ptr,
        uint32_t id,
        DbSampleType sampleType,
        Dim::TimePoint pageTime,
        size_t lastSample,
        double fill
    ) override;
    void onWalApplySampleUpdate(
        void * ptr,
        size_t firstPos,
        size_t lastPos,
        double value,
        bool updateLast
    ) override;
    void onWalApplySampleUpdateTime(
        void * ptr,
        Dim::TimePoint pageTime
    ) override;

private:
    friend DbPageHeap;
    friend DbRootSet;
    friend DbRootVersion;

    bool loadMetric(
        DbTxn & txn,
        IDbDataNotify * notify,
        pgno_t pgno
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

    // Calls the function for each page in index, exits immediately if the
    // function returns false. Returns true if no function returned false.
    bool radixVisit(
        DbTxn & txn,
        pgno_t root,
        const std::function<bool(DbTxn&, uint32_t, pgno_t)> & fn
    );

    bool bitAssign(
        DbTxn & txn,
        pgno_t root,
        uint32_t id,
        size_t firstPos,
        size_t lastPos,
        bool value
    );
    bool bitLoad(DbTxn & txn, Dim::UnsignedSet * out, pgno_t root);
    size_t bitsPerPage() const;

    void trieApply(
        DbTxn & txn,
        const std::vector<std::shared_ptr<DbRootVersion>> & roots,
        const std::vector<std::string> & keys,
        std::function<bool(Dim::StrTrieBase * index, const std::string & key)>
    );
    void trieInsert(
        DbTxn & txn,
        const std::vector<std::shared_ptr<DbRootVersion>> & roots,
        const std::vector<std::string> & keys
    );
    void trieErase(
        DbTxn & txn,
        const std::vector<std::shared_ptr<DbRootVersion>> & roots,
        const std::vector<std::string> & keys
    );

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
    MetricPosition loadMetricPos(DbTxn & txn, uint32_t id);
    MetricPosition loadMetricPos(
        DbTxn & txn,
        uint32_t id,
        Dim::TimePoint time
    );

    bool m_verbose{false};
    size_t m_pageSize = 0;
    pgno_t m_freeStoreRoot = pgno_t::npos;
    pgno_t m_deprecatedStoreRoot = pgno_t::npos;
    pgno_t m_metricStoreRoot = pgno_t::npos;

    std::atomic<std::shared_ptr<DbRootSet>> m_metricRoots;

    mutable std::shared_mutex m_mposMut;
    std::vector<MetricPosition> m_metricPos;
    unsigned m_numMetrics = 0;

    mutable std::recursive_mutex m_pageMut;
    size_t m_numPages = 0;
    Dim::UnsignedSet m_freePages;
    size_t m_numFree = 0;
    Dim::UnsignedSet m_deprecatedPages;

    // Used to manage the index at m_metricStoreRoot.
    mutable std::mutex m_mndxMut;
};
