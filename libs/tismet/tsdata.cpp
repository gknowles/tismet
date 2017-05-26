// Copyright Glen Knowles 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// tsdata.cpp - tismet
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace Dim;


/****************************************************************************
*
*   Private
*
***/

const unsigned kMaxMetricNameLen = 64;
static_assert(kMaxMetricNameLen <= numeric_limits<unsigned char>::max());

const unsigned kDefaultPageSize = 4096;
static_assert(kDefaultPageSize == pow2Ceil(kDefaultPageSize));

const unsigned kDataFileSig[] = { 
    0x39515728, 
    0x4873456d, 
    0xf6bfd8a1, 
    0xa33f3ba2 
};

namespace {
enum PageType {
    kPageTypeFree = 'F',
    kPageTypeMaster = 'M',
    kPageTypeMetric = 'm',
    kPageTypeRadix = 'r',
    kPageTypeData = 'd',
    kPageTypeBranch = 'b',
    kPageTypeLeaf = 'l',
};

struct PageHeader {
    unsigned type;
    uint32_t pgno;
    uint32_t checksum;
    uint64_t lsn;
};

struct MasterPage {
    static const PageType type = kPageTypeMaster;
    PageHeader hdr;
    char signature[sizeof(kDataFileSig)];
    unsigned pageSize;
    unsigned numPages;
    unsigned freePageRoot;
    unsigned metricInfoRoot;
};
static_assert(is_standard_layout<MasterPage>::value);

struct FreePage {
    static const PageType type = kPageTypeFree;
    PageHeader hdr;
    unsigned nextPage;
};

struct LeafPage {
    static const PageType type = kPageTypeLeaf;
    PageHeader hdr;
    char entries[1];
};

struct RadixData {
    uint16_t height;
    uint16_t numPages;
    uint32_t pages[1];
};

struct RadixPage {
    static const PageType type = kPageTypeRadix;
    PageHeader hdr;

    // MUST BE LAST DATA MEMBER
    RadixData rd; 
};

struct MetricPage {
    static const PageType type = kPageTypeMetric;
    PageHeader hdr;
    char name[kMaxMetricNameLen];
    uint32_t id;
    Duration interval;
    Duration retention;
    uint32_t lastPage;
    unsigned lastPagePos;

    // MUST BE LAST DATA MEMBER
    RadixData rd; 
};

struct DataPage {
    static const PageType type = kPageTypeData;
    PageHeader hdr;
    uint32_t id;

    // time of first value on page
    TimePoint firstPageTime; 
    
    // Position of last value, values that come after this on the page are 
    // either in the not yet populated future or (because it's a giant 
    // discontinuous ring buffer) in the distant past.
    uint16_t lastPageValue; 
    
    float values[1];
};

struct MetricInfo {
    Duration interval;
    uint32_t infoPage;
    uint32_t lastPage; // page with most recent data values
    TimePoint firstPageTime; // time of first value on last page
    uint16_t lastPageValue; // position of last value on last page
};

class TsdFile {
public:
    ~TsdFile();

    bool open(string_view name, size_t pageSize);
    bool insertMetric(uint32_t & out, const string & name);
    void eraseMetric(uint32_t id);
    void updateMetric(uint32_t id, Duration retention, Duration interval);
    
    void writeData(uint32_t id, TimePoint time, float value);

    bool findMetric(uint32_t & out, const string & name) const;
    void dump(ostream & os) const;

private:
    void dump(ostream & os, const MetricPage & mp, uint32_t pgno) const;

    bool loadMetricInfo (uint32_t pgno);
    void metricFreePage(uint32_t pgno);

    bool loadFreePages ();
    uint32_t allocPgno();
    template<typename T> unique_ptr<T> allocPage();
    void freePage(uint32_t pgno);
    template<typename T> unique_ptr<T> allocPage(uint32_t pgno) const;

    template<typename T> const T * addr(uint32_t pgno) const;
    template<> const PageHeader * addr<PageHeader>(uint32_t pgno) const;

    // get copy of page to update and then write
    template<typename T> unique_ptr<T> editPage(uint32_t pgno) const;
    template<typename T> unique_ptr<T> editPage(const T & data) const;

    // allocate new page (with new id) that is a copy of an existing page
    template<typename T> unique_ptr<T> dupPage(uint32_t pgno);
    template<typename T> unique_ptr<T> dupPage(const T & data);

    template<typename T> 
    void writePage(T & data, size_t count = sizeof(T)) const;
    template<> 
    void writePage<PageHeader>(PageHeader & hdr, size_t count) const;
    void writePage(uint32_t pgno, const void * ptr, size_t count) const;

    void radixClear(PageHeader & hdr);
    void radixErase(PageHeader & hdr, size_t firstPos, size_t lastPos);
    void radixFreePage(uint32_t pgno);
    bool radixFind(uint32_t * out, uint32_t root, size_t pos);
    bool radixInsert(uint32_t root, size_t pos, uint32_t value);
    bool radixFind(
        PageHeader const ** hdr, 
        RadixData const ** rd, 
        size_t * rpos,
        uint32_t root, 
        size_t pos
    );

    unique_ptr<DataPage> allocDataPage(uint32_t id, TimePoint time);
    size_t valuesPerPage() const;

    unordered_map<string, uint32_t> m_metricIds;
    vector<MetricInfo> m_metricInfo;
    priority_queue<uint32_t, vector<uint32_t>, greater<uint32_t>> m_freeIds;
    RadixDigits m_rdIndex;
    RadixDigits m_rdMetric;

    const MasterPage * m_hdr{nullptr};
    FileHandle m_data;
    FileHandle m_log;
};
} // namespace


/****************************************************************************
*
*   Variables
*
***/

static HandleMap<TsdFileHandle, TsdFile> s_files;

static auto & s_perfCount = uperf("perfs (total)");
static auto & s_perfCreated = uperf("perfs created");
static auto & s_perfDeleted = uperf("perfs deleted");

static auto & s_perfOld = uperf("perf data ignored (old)");
static auto & s_perfDup = uperf("perf data duplicate");
static auto & s_perfUpdate = uperf("perf data added");


/****************************************************************************
*
*   TsdFile
*
***/

//===========================================================================
TsdFile::~TsdFile () {
    s_perfCount -= (unsigned) m_metricInfo.size();
    fileClose(m_data);
    fileClose(m_log);
}

//===========================================================================
bool TsdFile::open(string_view name, size_t pageSize) {
    assert(pageSize == pow2Ceil(pageSize));
    if (!pageSize)
        pageSize = kDefaultPageSize;

    m_data = fileOpen(name, File::fCreat | File::fReadWrite);
    if (!m_data)
        return false;
    if (!fileSize(m_data)) {
        MasterPage tmp = {};
        tmp.hdr.type = kPageTypeMaster;
        memcpy(tmp.signature, kDataFileSig, sizeof(tmp.signature));
        tmp.pageSize = (unsigned) pageSize;
        tmp.numPages = 1;
        fileWriteWait(m_data, 0, &tmp, sizeof(tmp));
    }
    const char * base;
    if (!fileOpenView(base, m_data, 1024 * filePageSize())) 
        return false;
    m_hdr = (const MasterPage *)base;
    if (memcmp(
        m_hdr->signature, 
        kDataFileSig, 
        sizeof(m_hdr->signature)
    ) != 0) {
        logMsgError() << "Bad signature in " << name;
        return false;
    }

    auto ipOff = offsetof(RadixPage, rd) + offsetof(RadixData, pages);
    m_rdIndex.init(m_hdr->pageSize, ipOff, ipOff);
    auto mpOff = offsetof(MetricPage, rd) + offsetof(RadixData, pages);
    m_rdMetric.init(m_hdr->pageSize, mpOff, ipOff);

    if (!loadMetricInfo(m_hdr->metricInfoRoot))
        return false;
    if (!loadFreePages())
        return false;

    s_perfCount += (unsigned) m_metricInfo.size();
    return true;
}

//===========================================================================
void TsdFile::dump(ostream & os, const MetricPage & mp, uint32_t pgno) const {
    if (!pgno)
        return;

    auto p = addr<PageHeader>(pgno);
    if (p->type == kPageTypeRadix) {
        auto rp = reinterpret_cast<const RadixPage*>(p);
        for (int i = 0; i < rp->rd.numPages; ++i)
            dump(os, mp, rp->rd.pages[i]);
        return;
    }

    assert(p->type == kPageTypeData);
    auto pageValues = valuesPerPage();
    auto dp = reinterpret_cast<const DataPage*>(p);
    auto time = dp->firstPageTime;
    auto pageInterval = pageValues * mp.interval;
    auto lastValueTime = time + dp->lastPageValue * mp.interval;
    auto endPageTime = time + pageInterval;
    if (lastValueTime == endPageTime)
        lastValueTime -= mp.interval;
    int i = 0;
    for (; time <= lastValueTime; ++i, time += mp.interval) {
        if (!isnan(dp->values[i])) {
            os << mp.name << ' ' 
                << dp->values[i] << ' ' 
                << Clock::to_time_t(time) << '\n';
        }
    }
    if (time == endPageTime)
        return;
    time = lastValueTime - mp.retention + mp.interval;
    auto numValues = mp.retention / mp.interval;
    auto numPages = (numValues - 1) / pageValues + 1;
    auto gap = numPages * pageValues - numValues;
    i += (int) gap;
    for (; i < pageValues; ++i, time += mp.interval) {
        if (!isnan(dp->values[i])) {
            os << mp.name << ' ' 
                << dp->values[i] << ' ' 
                << Clock::to_time_t(time) << '\n';
        }
    }
}

//===========================================================================
void TsdFile::dump(ostream & os) const {
    for (auto && mi : m_metricInfo) {
        auto mp = addr<MetricPage>(mi.infoPage);
        for (int i = 0; i < mp->rd.numPages; ++i)
            dump(os, *mp, mp->rd.pages[i]);
    }
}


/****************************************************************************
*
*   Metric index
*
***/

//===========================================================================
void TsdFile::metricFreePage (uint32_t pgno) {
    auto mp = addr<MetricPage>(pgno);
    for (int i = 0; i < mp->rd.numPages; ++i) {
        if (auto pn = mp->rd.pages[i])
            freePage(pn);
    }
    auto num = m_metricIds.erase(mp->name);
    assert(num == 1);
    m_metricInfo[mp->id] = {};
}

//===========================================================================
bool TsdFile::loadMetricInfo (uint32_t pgno) {
    if (!pgno)
        return true;

    auto p = addr<PageHeader>(pgno);
    if (!p)
        return false;

    if (p->type == kPageTypeRadix) {
        auto rp = reinterpret_cast<const RadixPage*>(p);
        for (int i = 0; i < rp->rd.numPages; ++i) {
            if (!loadMetricInfo(rp->rd.pages[i]))
                return false;
        }
        return true;
    }

    if (p->type == kPageTypeMetric) {
        auto mp = reinterpret_cast<const MetricPage*>(p);
        m_metricIds[mp->name] = mp->id;
        if (m_metricInfo.size() <= mp->id)
            m_metricInfo.resize(mp->id + 1);
        auto & mi = m_metricInfo[mp->id];
        mi.infoPage = mp->hdr.pgno;
        mi.interval = mp->interval;
        mi.lastPage = mp->lastPage;
        return true;
    }

    return false;
}

//===========================================================================
bool TsdFile::findMetric(uint32_t & out, const string & name) const {
    auto i = m_metricIds.find(name);
    if (i == m_metricIds.end())
        return false;
    out = i->second;
    return true;
}

//===========================================================================
bool TsdFile::insertMetric(uint32_t & out, const string & name) {
    assert(!name.empty());
    assert(name.size() < kMaxMetricNameLen);
    auto i = m_metricIds.find(name);
    if (i != m_metricIds.end()) {
        out = i->second;
        return false;
    }

    // get metric id
    uint32_t id;
    if (m_freeIds.empty()) {
        id = (uint32_t) m_metricInfo.size();
        m_metricInfo.push_back({});
    } else {
        id = m_freeIds.top();
        m_freeIds.pop();
    }
    out = id;
    m_metricIds[name] = id;

    // set info page 
    auto mp = allocPage<MetricPage>();
    auto count = name.copy(mp->name, size(mp->name) - 1);
    mp->name[count] = 0;
    mp->id = id;
    mp->interval = 1min;
    mp->retention = 30min;
    mp->rd.height = 0;
    mp->rd.numPages = (uint16_t) m_rdMetric.rootEntries();
    writePage(*mp);

    auto & mi = m_metricInfo[id];
    mi = {};
    mi.infoPage = mp->hdr.pgno;
    mi.interval = mp->interval;

    // update index
    if (!m_hdr->metricInfoRoot) {
        auto rp = allocPage<RadixPage>();
        rp->rd.height = 0;
        rp->rd.numPages = (uint16_t) m_rdIndex.rootEntries();
        writePage(*rp);
        auto masp = *m_hdr;
        masp.metricInfoRoot = rp->hdr.pgno;
        writePage(masp);
    }
    bool inserted = radixInsert(m_hdr->metricInfoRoot, id, mp->hdr.pgno);
    assert(inserted);
    s_perfCount += 1;
    return true;
}

//===========================================================================
void TsdFile::eraseMetric(uint32_t id) {
    if (uint32_t pgno = m_metricInfo[id].infoPage) 
        metricFreePage(pgno);
}

//===========================================================================
void TsdFile::updateMetric(
    uint32_t id, 
    Duration retention, 
    Duration interval
) {
    auto & mi = m_metricInfo[id];
    auto mp = addr<MetricPage>(mi.infoPage);
    if (mp->retention == retention && mp->interval == interval)
        return;

    auto nmp = editPage(*mp);
    radixClear(nmp->hdr);
    nmp->lastPage = 0;
    nmp->lastPagePos = 0;
    writePage(*nmp, m_hdr->pageSize);
    mi.lastPage = 0;
    mi.firstPageTime = {};
    mi.lastPageValue = 0;
}


/****************************************************************************
*
*   Metric data values
*
***/

//===========================================================================
size_t TsdFile::valuesPerPage() const {
    return (m_hdr->pageSize - offsetof(DataPage, values)) 
        / sizeof(float);
}

//===========================================================================
unique_ptr<DataPage> TsdFile::allocDataPage(uint32_t id, TimePoint time) {
    auto count = valuesPerPage();
    auto dp = allocPage<DataPage>();
    dp->id = id;
    dp->lastPageValue = 0;
    dp->firstPageTime = time;
    for (auto i = 0; i < count; ++i) 
        dp->values[i] = NAN;
    return dp;
}

//===========================================================================
void TsdFile::writeData(uint32_t id, TimePoint time, float value) {
    auto & mi = m_metricInfo[id];
    assert(mi.infoPage);

    // round time down to metric's sampling interval
    time -= time.time_since_epoch() % mi.interval;

    auto count = valuesPerPage();

    // ensure all info about the last page is loaded, the hope is that almost
    // all updates are to the last page.
    if (!mi.lastPage) {
        auto mp = editPage<MetricPage>(mi.infoPage);

        auto dp = allocDataPage(id, time);
        dp->lastPageValue = (uint16_t) (id % count);
        dp->firstPageTime = time - dp->lastPageValue * mi.interval;
        writePage(*dp, m_hdr->pageSize);

        mp->lastPage = dp->hdr.pgno;
        mp->rd.pages[0] = dp->hdr.pgno;
        writePage(*mp);

        mi.lastPage = mp->lastPage;
        mi.firstPageTime = dp->firstPageTime;
        mi.lastPageValue = dp->lastPageValue;
    }
    if (mi.firstPageTime == TimePoint{}) {
        auto dp = addr<DataPage>(mi.lastPage);
        mi.firstPageTime = dp->firstPageTime;
        mi.lastPageValue = dp->lastPageValue;
    }

    auto pageInterval = valuesPerPage() * mi.interval;
    auto lastValueTime = mi.firstPageTime + mi.lastPageValue * mi.interval;

    // one interval past last time on page (aka first time on next page)
    auto endPageTime = mi.firstPageTime + pageInterval; 

    // updating historical value?
    if (time <= lastValueTime) {
        auto dpno = mi.lastPage;
        if (time < mi.firstPageTime) {
            auto mp = addr<MetricPage>(mi.infoPage);
            auto firstValueTime = lastValueTime - mp->retention;
            if (time < firstValueTime) {
                s_perfOld += 1;
                return;
            }
            auto off = (mi.firstPageTime - time) / pageInterval + 1;
            auto dpages = (mp->retention + pageInterval - mi.interval) 
                / pageInterval;
            auto pagePos = 
                (uint32_t) (mp->lastPagePos + dpages - off) % dpages;
            if (!radixFind(&dpno, mi.infoPage, pagePos)) {
                auto pageTime = mi.firstPageTime - off * pageInterval;
                auto dp = allocDataPage(id, pageTime);
                dp->lastPageValue = (uint16_t) valuesPerPage() - 1;
                writePage(*dp, m_hdr->pageSize);
                dpno = dp->hdr.pgno;
                bool inserted = radixInsert(mi.infoPage, pagePos, dpno);
                assert(inserted);
            }
        }
        auto dp = editPage<DataPage>(dpno);
        assert(time >= dp->firstPageTime);
        auto ent = (time - dp->firstPageTime) / mi.interval;
        assert(ent < (unsigned) valuesPerPage());
        dp->values[ent] = value;
        writePage(*dp, m_hdr->pageSize);
        return;
    } 
    
    //-----------------------------------------------------------------------
    // after last known value

    // If past the end of the page, check if it's past the end of all pages
    if (time >= endPageTime) {
        auto mp = addr<MetricPage>(mi.infoPage);
        // further in the future than the retention period? remove all values
        // and add as new initial value.
        if (time >= lastValueTime + mp->retention) {
            auto nmp = editPage<MetricPage>(mi.infoPage);
            radixClear(nmp->hdr);
            nmp->lastPage = 0;
            nmp->lastPagePos = 0;
            writePage(*nmp, m_hdr->pageSize);
            mi.lastPage = 0;
            mi.firstPageTime = {};
            mi.lastPageValue = 0;
            writeData(id, time, value);
            return;
        }
    }

    // update last page
    auto dp = editPage<DataPage>(mi.lastPage);
    assert(mi.firstPageTime == dp->firstPageTime);
    assert(mi.lastPageValue == dp->lastPageValue);
    auto i = mi.lastPageValue;
    for (;;) {
        i += 1;
        lastValueTime += mi.interval;
        if (lastValueTime == endPageTime)
            break;
        if (lastValueTime == time) {
            dp->values[i] = value;
            mi.lastPageValue = dp->lastPageValue = i;
            writePage(*dp, m_hdr->pageSize);
            return;
        }
        dp->values[i] = NAN;
    }
    mi.lastPageValue = dp->lastPageValue = i;
    writePage(*dp, m_hdr->pageSize);

    //-----------------------------------------------------------------------
    // value is after last page

    // delete pages between last page and the one the value is on
    auto num = (time - endPageTime) / pageInterval;
    auto mp = editPage<MetricPage>(mi.infoPage);
    auto numValues = mp->retention / mp->interval;
    auto numPages = (numValues - 1) / valuesPerPage() + 1;
    auto first = (mp->lastPagePos + 1) % numPages;
    auto last = first + num;
    if (num) {
        if (last <= numPages) {
            radixErase(mp->hdr, first, last);
        } else {
            radixErase(mp->hdr, first, numPages);
            radixErase(mp->hdr, 0, last % numPages);
        }
    }

    // update last page references
    mp->lastPagePos = (unsigned) last;
    radixFind(&mp->lastPage, mi.infoPage, last);
    writePage(*mp, m_hdr->pageSize);

    dp = editPage<DataPage>(mp->lastPage);
    dp->firstPageTime = endPageTime;
    dp->lastPageValue = 0;
    writePage(*dp);

    mi.lastPage = mp->lastPage;
    mi.firstPageTime = dp->firstPageTime;
    mi.lastPageValue = dp->lastPageValue;

    // write value to new last page
    writeData(id, time, value);
}


/****************************************************************************
*
*   Radix index
*
***/

//===========================================================================
static RadixData * radixData(PageHeader * hdr) {
    if (hdr->type == kPageTypeMetric) {
        return &reinterpret_cast<MetricPage *>(hdr)->rd;
    } else {
        assert(hdr->type == kPageTypeRadix);
        return &reinterpret_cast<RadixPage *>(hdr)->rd;
    }
}

//===========================================================================
static const RadixData * radixData(const PageHeader * hdr) {
    return radixData(const_cast<PageHeader *>(hdr));
}

//===========================================================================
void TsdFile::radixFreePage(uint32_t pgno) {
    auto rp = addr<RadixPage>(pgno);
    for (int i = 0; i < rp->rd.numPages; ++i) {
        if (uint32_t p = rp->rd.pages[i]) 
            freePage(p);
    }
}

//===========================================================================
void TsdFile::radixClear(PageHeader & hdr) {
    auto rd = radixData(&hdr);
    for (int i = 0; i < rd->numPages; ++i) {
        if (uint32_t p = rd->pages[i]) {
            freePage(p);
            rd->pages[i] = 0;
        }
    }
}

//===========================================================================
void TsdFile::radixErase(
    PageHeader & rhdr, 
    size_t firstPos, 
    size_t lastPos
) {
    assert(firstPos <= lastPos);
    while (firstPos < lastPos) {
        const PageHeader * hdr;
        const RadixData * rd;
        size_t rpos;
        if (!radixFind(&hdr, &rd, &rpos, rhdr.pgno, firstPos))
            return;

        unique_ptr<PageHeader> nhdr;
        RadixData * nrd;
        if (hdr == &rhdr) {
            nrd = radixData(&rhdr);
        } else {
            nhdr = editPage(*hdr);
            nrd = radixData(nhdr.get());
        }
        auto lastPagePos = min(
            (size_t) nrd->numPages, 
            rpos + lastPos - firstPos
        );
        for (auto i = rpos; i < lastPagePos; ++i, ++firstPos) {
            if (auto p = nrd->pages[i]) {
                freePage(p);
                nrd->pages[i] = 0;
            }
        }
        if (nhdr)
            writePage(*nhdr, m_hdr->pageSize);
    }
}

//===========================================================================
bool TsdFile::radixFind(
    PageHeader const ** hdr, 
    RadixData const ** rd, 
    size_t * rpos,
    uint32_t root, 
    size_t pos
) {
    *hdr = addr<PageHeader>(root);
    *rd = radixData(*hdr);
    RadixDigits & cvt = ((*hdr)->type == kPageTypeMetric) 
        ? m_rdMetric 
        : m_rdIndex;

    int digits[10];
    size_t count = cvt.convert(digits, size(digits), pos);
    count -= 1;
    if ((*rd)->height < count)
        return false;
    int * d = digits;
    while (auto height = (*rd)->height) {
        int pos = (height > count) ? 0 : *d;
        if (!(*rd)->pages[pos])
            return false;
        *hdr = addr<PageHeader>((*rd)->pages[pos]);
        *rd = radixData(*hdr);
        assert((*rd)->height == height - 1);
        if (height == count) {
            d += 1;
            count -= 1;
        }
    }

    *rpos = *d;
    return true;
}

//===========================================================================
bool TsdFile::radixFind(uint32_t * out, uint32_t root, size_t pos) {
    const PageHeader * hdr;
    const RadixData * rd;
    size_t rpos;
    if (!radixFind(&hdr, &rd, &rpos, root, pos))
        return false;
    *out = rd->pages[rpos];
    return *out;
}

//===========================================================================
bool TsdFile::radixInsert(uint32_t root, size_t pos, uint32_t value) {
    auto hdr = addr<PageHeader>(root);
    auto rd = radixData(hdr);
    RadixDigits & cvt = (hdr->type == kPageTypeMetric) 
        ? m_rdMetric 
        : m_rdIndex;

    int digits[10];
    size_t count = cvt.convert(digits, size(digits), pos);
    count -= 1;
    while (rd->height < count) {
        auto mid = allocPage<RadixPage>();
        mid->rd.height = rd->height;
        mid->rd.numPages = (uint16_t) cvt.pageEntries();
        memcpy(mid->rd.pages, rd->pages, rd->numPages * sizeof(rd->pages[0]));
        writePage(*mid, m_hdr->pageSize);

        auto nhdr = editPage(*hdr);
        auto nrd = radixData(nhdr.get());
        nrd->height += 1;
        memset(nrd->pages, 0, nrd->numPages * sizeof(nrd->pages[0]));
        nrd->pages[0] = mid->hdr.pgno;
        writePage(*nhdr, m_hdr->pageSize);
    }
    int * d = digits;
    while (count) {
        int pos = (rd->height > count) ? 0 : *d;
        if (!rd->pages[pos]) {
            auto next = allocPage<RadixPage>();
            next->rd.height = rd->height - 1;
            next->rd.numPages = (uint16_t) cvt.pageEntries();
            writePage(*next);
            auto nhdr = editPage(*hdr);
            auto nrd = radixData(nhdr.get());
            nrd->pages[pos] = next->hdr.pgno;
            writePage(*nhdr, m_hdr->pageSize);
            assert(rd->pages[pos]);
        }
        hdr = addr<PageHeader>(rd->pages[pos]);
        rd = radixData(hdr);
        d += 1;
        count -= 1;
    }
    if (rd->pages[*d]) 
        return false;

    auto nhdr = editPage(*hdr);
    auto nrd = radixData(nhdr.get());
    nrd->pages[*d] = value;
    writePage(*nhdr, m_hdr->pageSize);
    return true;
}


/****************************************************************************
*
*   Page management
*
***/

//===========================================================================
uint32_t TsdFile::allocPgno () {
    auto pgno = m_hdr->freePageRoot;
    auto pageSize = m_hdr->pageSize;
    auto mp = *m_hdr;
    if (!pgno) {
        pgno = m_hdr->numPages;
        mp.numPages += 1;
        fileExtendView(m_data, (pgno + 1) * pageSize);
    } else {
        auto fp = addr<FreePage>(pgno);
        assert(fp->hdr.type == kPageTypeFree);
        mp.freePageRoot = fp->nextPage;
    }
    writePage(mp);
    return pgno;
}

//===========================================================================
template<typename T>
unique_ptr<T> TsdFile::allocPage() {
    auto pgno = allocPgno();
    return allocPage<T>(pgno);
}

//===========================================================================
template<typename T>
unique_ptr<T> TsdFile::allocPage(uint32_t pgno) const {
    auto pageSize = m_hdr->pageSize;
    void * vptr = new char[pageSize];
    memset(vptr, 0, pageSize);
    T * ptr = new(vptr) T;
    ptr->hdr.type = ptr->type;
    ptr->hdr.pgno = pgno;
    ptr->hdr.checksum = 0;
    ptr->hdr.lsn = 0;
    return unique_ptr<T>(ptr);
}

//===========================================================================
bool TsdFile::loadFreePages () {
    auto pgno = m_hdr->freePageRoot;
    while (pgno) {
        auto p = addr<PageHeader>(pgno);
        if (!p || p->type != kPageTypeFree)
            return false;
        if (m_metricInfo.size() <= pgno)
            m_metricInfo.resize(pgno + 1);
        if (m_metricInfo[pgno].lastPage)
            return false;
        m_metricInfo[pgno].lastPage = pgno;
        m_freeIds.push(pgno);
        auto fp = reinterpret_cast<const FreePage*>(p);
        pgno = fp->nextPage;
    }
    return true;
}

//===========================================================================
void TsdFile::freePage(uint32_t pgno) {
    assert(pgno < m_hdr->numPages);
    auto p = addr<PageHeader>(pgno);
    assert(p->type != kPageTypeFree);
    FreePage fp;
    fp.hdr = *p;
    switch (fp.hdr.type) {
    case kPageTypeMetric:
        metricFreePage(pgno);
        break;
    case kPageTypeRadix:
        radixFreePage(pgno);
        break;
    case kPageTypeData:
    case kPageTypeLeaf:
        break;
    case kPageTypeFree:
        logMsgCrash() << "freePage: page already free";
    default:
    case kPageTypeBranch:
        logMsgCrash() << "freePage(" << fp.hdr.type << "): invalid state";
    }
    fp.hdr.type = kPageTypeFree;
    fp.nextPage = m_hdr->freePageRoot;
    writePage(fp);
    auto mp = *m_hdr;
    mp.freePageRoot = pgno;
    writePage(mp);
}

//===========================================================================
template<typename T>
unique_ptr<T> TsdFile::editPage(uint32_t pgno) const {
    return editPage(*addr<T>(pgno));
}

//===========================================================================
template<typename T>
unique_ptr<T> TsdFile::editPage(const T & data) const {
    auto pageSize = m_hdr->pageSize;
    void * vptr = new char[pageSize];
    T * ptr = new(vptr) T;
    memcpy(ptr, &data, pageSize);
    return unique_ptr<T>(ptr);
}

//===========================================================================
template<typename T>
unique_ptr<T> TsdFile::dupPage(uint32_t pgno) {
    return dupPage(*addr<T>(pgno));
}

//===========================================================================
template<typename T>
unique_ptr<T> TsdFile::dupPage(const T & data) {
    auto ptr = editPage(data);
    ptr->hdr.pgno = allocPgno();
    return ptr;
}

//===========================================================================
template<typename T>
const T * TsdFile::addr(uint32_t pgno) const {
    assert(pgno < m_hdr->numPages);
    const void * vptr = (char *) m_hdr + m_hdr->pageSize * pgno;
    auto ptr = static_cast<const T*>(vptr);
    assert(ptr->hdr.type == ptr->type);
    return ptr;
}

//===========================================================================
template<>
const PageHeader * TsdFile::addr<PageHeader>(uint32_t pgno) const {
    if (pgno > m_hdr->numPages)
        return nullptr;
    const void * vptr = (char *) m_hdr + m_hdr->pageSize * pgno;
    auto ptr = static_cast<const PageHeader*>(vptr);
    return ptr;
}

//===========================================================================
template<typename T>
void TsdFile::writePage(T & data, size_t count) const {
    writePage(data.hdr.pgno, &data, count);
}

//===========================================================================
template<>
void TsdFile::writePage<PageHeader>(PageHeader & hdr, size_t count) const {
    writePage(hdr.pgno, &hdr, count);
}

//===========================================================================
void TsdFile::writePage(uint32_t pgno, const void * ptr, size_t count) const {
    assert(pgno < m_hdr->numPages);
    assert(count <= m_hdr->pageSize);
    fileWriteWait(m_data, pgno * m_hdr->pageSize, ptr, count);
}


/****************************************************************************
*
*   External
*
***/

//===========================================================================
TsdFileHandle tsdOpen(string_view name, size_t pageSize) {
    auto tsd = make_unique<TsdFile>();
    if (!tsd->open(name, pageSize))
        return TsdFileHandle{};

    auto h = s_files.insert(tsd.release());
    return h;
}

//===========================================================================
void tsdClose(TsdFileHandle h) {
    s_files.erase(h);
}

//===========================================================================
bool tsdFindMetric(uint32_t & out, TsdFileHandle h, string_view name) {
    auto * tsd = s_files.find(h);
    assert(tsd);
    return tsd->findMetric(out, string(name));
}

//===========================================================================
bool tsdInsertMetric(uint32_t & out, TsdFileHandle h, string_view name) {
    auto * tsd = s_files.find(h);
    assert(tsd);
    return tsd->insertMetric(out, string(name));
}

//===========================================================================
void tsdEraseMetric(TsdFileHandle h, uint32_t id) {
    auto * tsd = s_files.find(h);
    assert(tsd);
    return tsd->eraseMetric(id);
}

//===========================================================================
void tsdUpdateMetric(
    TsdFileHandle h,
    uint32_t id,
    Duration retention,
    Duration interval
) {
    auto * tsd = s_files.find(h);
    assert(tsd);
    return tsd->updateMetric(id, retention, interval);
}

//===========================================================================
void tsdWriteData(TsdFileHandle h, uint32_t id, TimePoint time, float value) {
    auto * tsd = s_files.find(h);
    assert(tsd);
    return tsd->writeData(id, time, value);
}

//===========================================================================
void tsdDump(std::ostream & os, TsdFileHandle h) {
    auto * tsd = s_files.find(h);
    assert(tsd);
    tsd->dump(os);
}
