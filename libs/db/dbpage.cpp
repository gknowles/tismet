// Copyright Glen Knowles 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// dbpage.cpp - tismet db
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace Dim;


/****************************************************************************
*
*   Tuning parameters
*
***/

const unsigned kDefaultPageSize = 4096;
static_assert(kDefaultPageSize == pow2Ceil(kDefaultPageSize));

// Must be a multiple of fileViewAlignment()
const size_t kViewSize = 0x100'0000; // 16MiB
const size_t kDefaultFirstViewSize = 2 * kViewSize;


/****************************************************************************
*
*   Declarations
*
***/

namespace {

const unsigned kWorkFileSig[] = {
    0xa6e6fd51,
    0x4a443864,
    0x8b4302ae,
    0x84b2074b,
};

const uint32_t kPageTypeZero = 'wZ';
const uint32_t kPageTypeFree = 'F';

struct ZeroPage {
    DbPageHeader hdr;
    char signature[sizeof(kWorkFileSig)];
    unsigned pageSize;
};

} // namespace


/****************************************************************************
*
*   DbPage
*
***/

//===========================================================================
DbPage::~DbPage() {
    close();
}

//===========================================================================
bool DbPage::open(
    string_view datafile,
    string_view workfile,
    size_t pageSize
) {
    if (openWork(workfile, pageSize) && openData(datafile))
        return true;

    close();
    return false;
}

//===========================================================================
bool DbPage::openWork(string_view workfile, size_t pageSize) {
    assert(pageSize == pow2Ceil(pageSize));
    if (!pageSize)
        pageSize = kDefaultPageSize;
    assert(kViewSize % fileViewAlignment() == 0);

    m_fwork = fileOpen(
        workfile,
        File::fCreat | File::fReadWrite | File::fDenyWrite | File::fBlocking
    );
    if (!m_fwork)
        return false;
    auto len = fileSize(m_fwork);
    ZeroPage zp{};
    if (!len) {
        zp.hdr.type = (DbPageType) kPageTypeZero;
        memcpy(zp.signature, kWorkFileSig, sizeof(zp.signature));
        zp.pageSize = (unsigned) pageSize;
        fileWriteWait(m_fwork, 0, &zp, sizeof(zp));
        len = pageSize;
    } else {
        fileReadWait(&zp, sizeof(zp), m_fwork, 0);
    }
    if (memcmp(zp.signature, kWorkFileSig, sizeof(zp.signature)) != 0) {
        logMsgError() << "Bad signature in " << workfile;
        return false;
    }
    m_pageSize = zp.pageSize;
    if (m_pageSize < kMinPageSize || kViewSize % m_pageSize != 0) {
        logMsgError() << "Invalid page size in " << workfile;
        return false;
    }
    m_workPages = len / pageSize;
    m_freeWorkPages.insert(1, (unsigned) m_workPages - 1);
    if (!m_vwork.open(m_fwork, kViewSize, m_pageSize)) {
        logMsgError() << "Open view failed for " << workfile;
        return false;
    }
    return true;
}

//===========================================================================
bool DbPage::openData(string_view datafile) {
    m_fdata = fileOpen(
        datafile,
        File::fCreat | File::fReadWrite | File::fDenyWrite | File::fBlocking
    );
    if (!m_fdata)
        return false;
    auto len = fileSize(m_fdata);
    if (!len) {
        DbPageHeader hdr = {};
        fileWriteWait(m_fdata, 0, &hdr, sizeof(hdr));
    }
    if (!m_vdata.open(m_fdata, kViewSize, m_pageSize)) {
        logMsgError() << "Open view failed for " << datafile;
        return false;
    }

    // Remove trailing blank pages from page count
    auto lastPage = (uint32_t) (len / m_pageSize);
    while (lastPage) {
        lastPage -= 1;
        auto p = static_cast<const DbPageHeader *>(m_vdata.rptr(lastPage));
        if (p->type)
            break;
    }
    m_pages.resize(lastPage + 1);

    return true;
}

//===========================================================================
void DbPage::close() {
    m_pages.clear();
    m_pageSize = 0;
    m_vdata.close();
    fileClose(m_fdata);
    m_vwork.close();
    fileClose(m_fwork);
    m_freeWorkPages.clear();
    m_workPages = 0;
}

//===========================================================================
void DbPage::flush() {
    uint32_t pgno = 1;
    unique_lock<mutex> lk{m_workMut};
    for (;; ++pgno) {
        if (auto i = m_freeWorkPages.find(pgno); i) {
            i = m_freeWorkPages.lastContiguous(i);
            pgno = *i + 1;
        }
        if (pgno >= m_workPages)
            break;
        lk.unlock();
        auto hdr = reinterpret_cast<DbPageHeader *>(m_vwork.wptr(pgno));
        if (m_checkpointLsn < DbLog::getLsn(hdr->lsn)) {
            lk.lock();
            continue;
        }
        writePage(hdr);
        lk.lock();
        hdr->type = (DbPageType) kPageTypeFree;
        hdr->pgno = numeric_limits<decltype(hdr->pgno)>::max();
        m_freeWorkPages.insert(pgno);
        if (m_pages[pgno] == hdr)
            m_pages[pgno] = nullptr;
    }
    m_oldPages.clear();
}

//===========================================================================
void DbPage::growToFit(uint32_t pgno) {
    unique_lock<mutex> lk{m_workMut};
    if (pgno < m_pages.size())
        return;
    assert(pgno == m_pages.size());
    m_vdata.growToFit(pgno);
    m_pages.resize(pgno + 1);
}

//===========================================================================
void DbPage::checkpoint(uint64_t lsn) {
    m_checkpointLsn = lsn;
}

//===========================================================================
const void * DbPage::rptr(uint64_t lsn, uint32_t pgno) const {
    unique_lock<mutex> lk{m_workMut};
    assert(pgno < m_pages.size());
    if (auto ptr = m_pages[pgno])
        return ptr;

    return m_vdata.rptr(pgno);
}

//===========================================================================
DbPageHeader * DbPage::dupPage_LK(const DbPageHeader * hdr) {
    auto wpno = (uint32_t) 0;
    if (m_freeWorkPages.empty()) {
        wpno = (uint32_t) m_workPages++;
        m_vwork.growToFit(wpno);
    } else {
        wpno = m_freeWorkPages.pop_front();
    }
    void * ptr = m_vwork.wptr(wpno);
    memcpy(ptr, hdr, m_pageSize);
    return (DbPageHeader *) ptr;
}

//===========================================================================
void * DbPage::wptr(uint64_t lsn, uint32_t pgno, void ** newPage) {
    if (newPage)
        *newPage = nullptr;
    unique_lock<mutex> lk{m_workMut};
    assert(pgno < m_pages.size());
    auto hdr = (DbPageHeader *) m_pages[pgno];
    if (!hdr) {
        // create new dirty page from clean page
        auto src = reinterpret_cast<const DbPageHeader *>(m_vdata.rptr(pgno));
        hdr = dupPage_LK(src);
        m_pages[pgno] = hdr;
    } else {
        if (DbLog::getLsn(lsn) >= m_checkpointLsn) {
            // for new epoch
            if (DbLog::getLsn(hdr->lsn) >= m_checkpointLsn) {
                // dirty page from current epoch
                //  - use it
            } else {
                // dirty page from old epoch
                //  - save reference to old dirty page
                //  - create new dirty page from old dirty page
                if (!m_oldPages.insert({pgno, hdr}).second) {
                    logMsgCrash() << "Multiple dirty replacements of page #"
                        << pgno;
                }
                hdr = dupPage_LK(hdr);
                m_pages[hdr->pgno] = hdr;
            }
        } else {
            // for old epoch
            if (DbLog::getLsn(hdr->lsn) >= m_checkpointLsn) {
                // dirty page from future epoch
                //  - interleaving updates allowed?
                //      - include in output, create and use old dirty page
                //  - interleaving not allowed?
                //      - crash
                if (!newPage) {
                    logMsgCrash()
                        << "Transactions with interleaving updates on page #"
                        << pgno;
                }
                *newPage = hdr;
                // find or create old dirty page
                if (auto i = m_oldPages.find(pgno); i != m_oldPages.end()) {
                    hdr = i->second;
                } else {
                    // no existing old dirty page?
                    //  - create old dirty page from clean page
                    auto src = (const DbPageHeader *) m_vdata.rptr(pgno);
                    hdr = dupPage_LK(src);
                    m_oldPages[pgno] = hdr;
                }
            } else {
                // dirty page from current epoch
                //  - use it
            }
        }
    }
    hdr->pgno = pgno;
    hdr->lsn = lsn;
    return hdr;
}

//===========================================================================
void DbPage::writePage(const DbPageHeader * hdr) {
    assert(hdr->pgno != (uint32_t) -1);
    fileWriteWait(m_fdata, hdr->pgno * m_pageSize, hdr, m_pageSize);
}
