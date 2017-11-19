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
*   DbWork
*
***/

enum DbPageType : uint32_t;

struct DbPageHeader {
    DbPageType type;
    uint32_t pgno;
    uint32_t id;
    uint32_t checksum;
    uint64_t lsn;
};

class DbWork {
public:
    ~DbWork();

    bool open(
        std::string_view datafile,
        std::string_view workfile,
        size_t pageSize
    );
    void close();
    void flush();
    void growToFit(uint32_t pgno);

    const void * rptr(uint64_t txn, uint32_t pgno) const;
    void * wptr(uint64_t txn, uint32_t pgno);
    size_t pageSize() const { return m_pageSize; }
    size_t viewSize() const { return m_vwork.viewSize(); }
    size_t size() const { return m_pages.size(); }

    void lsnCommitted(uint64_t lsn);

    // TODO: remove!
    void writePage(const DbPageHeader * hdr);

private:
    bool openWork(std::string_view workfile, size_t pageSize);
    bool openData(std::string_view datafile);

    // One entry for every data page, may point to either a data or work view
    std::vector<void *> m_pages;
    size_t m_pageSize{0};

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

enum DbLogRecType : uint8_t;
class DbData;

class DbLog {
public:
    struct Record;

public:
    DbLog(DbData & data, DbWork & work);

    bool open(std::string_view file);

    // Returns transaction id.
    unsigned beginTxn();
    void commit(unsigned txn);

    Record * alloc(
        unsigned txn,
        DbLogRecType type,
        uint32_t pgno,
        size_t bytes
    );
    void log(const Record * log, size_t bytes);

private:
    void * alloc(size_t bytes);

    void apply(const Record * log);
    void apply(void * ptr, const Record * log);
    void applyBeginTxn(uint16_t txn);
    void applyCommit(uint16_t txn);

    DbData & m_data;
    DbWork & m_work;
    Dim::FileHandle m_file;
    uint16_t m_lastTxn = 0;
    uint64_t m_lastLsn = 0;
};

class DbTxn {
public:
    DbTxn(DbLog & log, DbWork & work);
    ~DbTxn();

    template<typename T> const T * viewPage(uint32_t pgno) const;
    template<typename T> const T * editPage(uint32_t pgno);
    size_t pageSize() const { return m_work.pageSize(); }
    size_t numPages() const { return m_work.size(); }
    void growToFit(uint32_t pgno) { m_work.growToFit(pgno); }

    void logZeroInit(uint32_t pgno);
    void logPageFree(uint32_t pgno);
    void logSegmentInit(uint32_t pgno);
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
    void logSampleUpdate(
        uint32_t pgno,
        size_t firstSample,
        size_t lastSample,
        float value,
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

    DbLog & m_log;
    DbWork & m_work;
    unsigned m_txn{0};
};

//===========================================================================
template<typename T>
const T * DbTxn::viewPage(uint32_t pgno) const {
    auto ptr = static_cast<const T *>(m_work.rptr(m_txn, pgno));
    if constexpr (!std::is_same_v<T, DbPageHeader>) {
        // Must start with and be layout compatible with DbPageHeader
        assert((std::is_same_v<decltype(ptr->hdr), DbPageHeader>));
        assert(intptr_t(ptr) == intptr_t(&ptr->hdr));
        assert(ptr->hdr.type == ptr->type);
    }
    return ptr;
}

//===========================================================================
template<typename T>
const T * DbTxn::editPage(uint32_t pgno) {
    auto ptr = static_cast<const T *>(m_work.wptr(m_txn, pgno));
    if constexpr (!std::is_same_v<T, DbPageHeader>) {
        assert((std::is_same_v<decltype(ptr->hdr), DbPageHeader>));
        assert(offsetof(T, hdr) == 0);
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

    bool open(
        DbTxn & txn,
        IDbEnumNotify * notify,
        std::string_view name
    );
    DbStats queryStats();

    void insertMetric(DbTxn & txn, uint32_t id, const std::string & name);
    bool eraseMetric(DbTxn & txn, std::string & name, uint32_t id);
    bool getMetricInfo(DbTxn & txn, MetricInfo & info, uint32_t id);
    void updateMetric(
        DbTxn & txn,
        uint32_t id,
        const MetricInfo & info
    );

    void updateSample(
        DbTxn & txn,
        uint32_t id,
        Dim::TimePoint time,
        float value
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
    void applySegmentInit(void * ptr);
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
        float value,
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
    std::unique_ptr<SamplePage> allocSamplePage(
        DbTxn & txn,
        uint32_t id,
        Dim::TimePoint time,
        uint16_t lastSample
    );
    MetricPosition & loadMetricPos(DbTxn & txn, uint32_t id);
    MetricPosition & loadMetricPos(DbTxn & txn, uint32_t id, Dim::TimePoint time);
    bool findSamplePage(
        DbTxn & txn,
        uint32_t * pgno,
        unsigned * pagePos,
        uint32_t id,
        Dim::TimePoint time
    );

    std::vector<MetricPosition> m_metricPos;

    DbRadix m_rdIndex;
    DbRadix m_rdMetric;

    Dim::FileHandle m_fdata;
    DbReadView m_vdata;
    size_t m_segmentSize = 0;
    size_t m_pageSize = 0;
    size_t m_numPages = 0;
    Dim::UnsignedSet m_freePages;
    unsigned m_numMetrics = 0;
};
