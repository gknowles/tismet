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
    kRecTypeFree,               // [any]
    kRecTypeInitSegment,        // [segment]
    kRecTypeUpdateSegment,      // [master/segment] pgno, isFree
    kRecTypeInitRadix,          // [radix] height, isWithList, page list
    kRecTypeUpdateRadix,        // [radix] pos, pgno
    kRecTypeCopyRadix,          // [radix] height, page list
    kRecTypeEraseRadix,         // [metric/radix] firstPos, lastPos
    kRecTypePromoteRadix,       // [metric/radix] height
    kRecTypeInitMetric,         // [metric] name
    kRecTypeUpdateMetric,       // [metric] retention, interval
    kRecTypeClearSampleIndex,   // [metric]
    kRecTypeUpdateSampleIndex,  // [metric] pos, pgno, isNewLast, isRadix
    kRecTypeInitSample,         // [sample] pageTime, lastPos
    kRecTypeUpdateSample,       // [sample] first, last, value, isNewLastFlag
                                //   [first, last) = NANs, last = value
    kRecTypeUpdateSampleTime,   // [sample] pageTime (pos=0, samples[0]=NAN)
    kRecTypeCommit,             // N/A
};

struct DbLogRec {
    uint64_t seq : 16;
    uint64_t txn : 48;
    uint32_t pgno;
    DbLogRecType type;
};

class DbLog {
public:
    bool open(std::string_view file);

    // Returns initial LSN for transaction, each log event updates it, and
    // finally it is consumed and invalidated by the call to commit that
    // completes the transaction.
    uint64_t beginTrans();
    void commit(uint64_t & lsn);

private:
    DbLogRec * alloc(size_t bytes);

    Dim::FileHandle m_file;
    uint64_t m_lastTxnId = 0;
};

class DbTxn {
public:
    DbTxn(DbLog & log);
    ~DbTxn();

    void logFree(uint32_t pgno);
    void logInitSegment(uint32_t pgno);
    void logUpdateSegment(
        uint32_t pgno,
        uint32_t refPage,
        bool free
    );
    void logInitRadix(
        uint32_t pgno,
        uint16_t height,
        uint32_t * firstPage,
        uint32_t * lastPage
    );
    void logUpdateRadix(
        uint32_t pgno,
        size_t pos,
        uint32_t refPage
    );
    void logCopyRadix(
        uint32_t pgno,
        uint16_t height,
        uint32_t * firstPage,
        uint32_t * lastPage
    );
    void logEraseRadix(
        uint32_t pgno,
        size_t firstPos,
        size_t lastPos
    );
    void logPromoteRadix(uint32_t pgno, uint16_t height);
    void logInitMetric(
        uint32_t pgno,
        std::string_view name,
        Dim::Duration retention,
        Dim::Duration interval
    );
    void logUpdateMetric(
        uint32_t pgno,
        Dim::Duration retention,
        Dim::Duration interval
    );
    void logClearSampleIndex(uint32_t pgno);
    void logUpdateSampleIndex(
        uint32_t pgno,
        size_t pos,
        uint32_t refPage,
        bool updateLast,
        bool updateIndex
    );
    void logInitSample(
        uint32_t pgno,
        Dim::TimePoint pageTime,
        size_t lastPos
    );
    void logUpdateSample(
        uint32_t pgno,
        size_t firstPos,
        size_t lastPos,
        float value,
        bool updateLast
    );
    void logUpdateSampleTime(
        uint32_t pgno,
        Dim::TimePoint pageTime
    );

private:
    DbLog & m_log;
    uint64_t m_lsn;
};
