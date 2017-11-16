// Copyright Glen Knowles 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// dbint.h - tismet db
#pragma once


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
*   DbLog
*
***/

enum DbLogRecType : uint8_t {
    kRecTypePageFree,           // [any]
    kRecTypeSegmentInit,        // [segment]
    kRecTypeSegmentAlloc,       // [master/segment] pgno
    kRecTypeSegmentFree,        // [master/segment] pgno
    kRecTypeRadixInit,          // [radix] height
    kRecTypeRadixInitList,      // [radix] height, page list
    kRecTypeRadixUpdate,        // [radix] pos, pgno
    kRecTypeRadixErase,         // [metric/radix] firstPos, lastPos
    kRecTypeMetricInit,         // [metric] name
    kRecTypeMetricUpdate,       // [metric] retention, interval
    kRecTypeMetricClearIndex,   // [metric] (clears index & last)
    kRecTypeMetricUpdateIndex,  // [metric] pos, pgno
    kRecTypeMetricUpdateLast,   // [metric] pos, pgno
    kRecTypeMetricUpdateIndexAndLast, // [metric] pos, pgno
    kRecTypeSampleInit,         // [sample] pageTime, lastPos
    kRecTypeSampleUpdate,       // [sample] first, last, value
                                //   [first, last) = NANs, last = value
    kRecTypeSampleUpdateLast,   // [sample] first, last, value
                                //   [first, last) = NANs, last = value
    kRecTypeSampleUpdateTime,   // [sample] pageTime (pos=0, samples[0]=NAN)
    kRecTypeTxnCommit,          // N/A
};

class DbData;

class DbLog {
public:
    struct Record;

public:
    DbLog(DbData & data);

    bool open(std::string_view file);

    // Returns initial LSN for transaction, each log event updates it, and
    // finally it is consumed and invalidated by the call to commit that
    // completes the transaction.
    uint16_t beginTrans();
    void commit(uint16_t txn);

    template<typename T>
    T * alloc(
        uint16_t txn,
        DbLogRecType type,
        uint32_t pgno,
        size_t bytes = sizeof(T)
    );

    Record * alloc(
        uint16_t txn,
        DbLogRecType type,
        uint32_t pgno,
        size_t bytes
    );
    void apply(Record * rec);

private:
    DbData & m_data;
    Dim::FileHandle m_file;
    uint16_t m_lastTxnId = 0;
    uint64_t m_lastLsn = 0;
};

class DbTxn {
public:
    DbTxn(DbLog & log);
    ~DbTxn();

    void logPageFree(uint32_t pgno);
    void logSegmentInit(uint32_t pgno);
    void logSegmentUpdate(
        uint32_t pgno,
        uint32_t refPage,
        bool free
    );
    void logRadixInit(
        uint32_t pgno,
        uint16_t height,
        uint32_t * firstPage,
        uint32_t * lastPage
    );
    void logRadixUpdate(
        uint32_t pgno,
        size_t pos,
        uint32_t refPage
    );
    void logRadixErase(
        uint32_t pgno,
        size_t firstPos,
        size_t lastPos
    );
    void logMetricInit(
        uint32_t pgno,
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
        size_t pos,
        uint32_t refPage,
        bool updateLast,
        bool updateIndex
    );
    void logSampleInit(
        uint32_t pgno,
        Dim::TimePoint pageTime,
        size_t lastPos
    );
    void logSampleUpdate(
        uint32_t pgno,
        size_t firstPos,
        size_t lastPos,
        float value,
        bool updateLast
    );
    void logSampleUpdateTime(
        uint32_t pgno,
        Dim::TimePoint pageTime
    );

private:
    DbLog & m_log;
    uint16_t m_txn;
};


/****************************************************************************
*
*   DbData
*
***/

class DbData : public Dim::HandleContent {
public:
    struct SegmentPage;
    struct MasterPage;
    struct FreePage;
    struct RadixPage;
    struct MetricPage;
    struct SamplePage;

    struct RadixData;

    struct MetricInfo {
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
        std::unordered_map<std::string, uint32_t> & metricIds,
        std::string_view name,
        size_t pageSize
    );
    DbStats queryStats();

    void insertMetric(DbTxn & txn, uint32_t id, const std::string & name);
    bool eraseMetric(DbTxn & txn, std::string & name, uint32_t id);
    void updateMetric(
        DbTxn & txn,
        uint32_t id,
        Dim::Duration retention,
        Dim::Duration interval
    );

    void updateSample(
        DbTxn & txn,
        uint32_t id,
        Dim::TimePoint time,
        float value
    );
    size_t enumSamples(
        IDbEnumNotify * notify,
        uint32_t id,
        Dim::TimePoint first,
        Dim::TimePoint last
    );

    void applyPageFree(uint32_t pgno);
    void applySegmentInit(uint32_t pgno);
    void applySegmentUpdate(
        uint32_t pgno,
        uint32_t refPage,
        bool free
    );

private:
    bool loadMetrics(
        std::unordered_map<std::string, uint32_t> & metricIds,
        uint32_t pgno
    );
    void metricFreePage(DbTxn & txn, uint32_t pgno);

    bool loadFreePages();
    uint32_t allocPgno(DbTxn & txn);
    template<typename T>
    std::unique_ptr<T> allocPage(DbTxn & txn, uint32_t id);
    template<typename T>
    std::unique_ptr<T> allocPage(uint32_t id, uint32_t pgno);
    void freePage(DbTxn & txn, uint32_t pgno);

    const void * viewPageRaw(uint32_t pgno) const;
    template<typename T> const T * viewPage(uint32_t pgno) const;

    // get copy of page to update and then write
    template<typename T> std::unique_ptr<T> editPage(uint32_t pgno) const;
    template<typename T> std::unique_ptr<T> editPage(const T & data) const;

    template<typename T>
    void writePage(T & data) const;
    void writePagePrefix(
        uint32_t pgno,
        const void * ptr,
        size_t count
    ) const;

    void radixClear(DbTxn & txn, DbPageHeader & hdr);
    void radixErase(
        DbTxn & txn,
        DbPageHeader & hdr,
        size_t firstPos,
        size_t lastPos
    );
    void radixFreePage(DbTxn & txn, uint32_t pgno);
    bool radixInsert(DbTxn & txn, uint32_t root, size_t pos, uint32_t value);
    bool radixFind(
        DbPageHeader const ** hdr,
        RadixData const ** rd,
        size_t * rpos,
        uint32_t root,
        size_t pos
    );
    bool radixFind(uint32_t * out, uint32_t root, size_t pos);

    size_t samplesPerPage() const;
    std::unique_ptr<SamplePage> allocSamplePage(
        DbTxn & txn,
        uint32_t id,
        Dim::TimePoint time,
        uint16_t lastSample
    );
    MetricInfo & loadMetricInfo(uint32_t id);
    MetricInfo & loadMetricInfo(DbTxn & txn, uint32_t id, Dim::TimePoint time);
    bool findSamplePage(
        uint32_t * pgno,
        unsigned * pagePos,
        uint32_t id,
        Dim::TimePoint time
    );

    std::vector<MetricInfo> m_metricInfo;

    DbRadix m_rdIndex;
    DbRadix m_rdMetric;

    Dim::FileHandle m_fdata;
    DbReadView m_vdata;
    const MasterPage * m_hdr = nullptr;
    size_t m_pageSize = 0;
    size_t m_numPages = 0;
    Dim::UnsignedSet m_freePages;
    unsigned m_numMetrics = 0;
};
