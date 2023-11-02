// Copyright Glen Knowles 2017 - 2023.
// Distributed under the Boost Software License, Version 1.0.
//
// dbdata.cpp - tismet db
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace Dim;


/****************************************************************************
*
*   Private
*
***/

constexpr auto kZeroPageNum = (pgno_t) 0;
constexpr auto kDefaultFreeStoreRoot = (pgno_t) 1;
constexpr auto kDefaultDeprecatedStoreRoot = (pgno_t) 2;
constexpr auto kDefaultMetricStoreRoot = (pgno_t) 3;

const auto kDataFileSig = "66b1e542-541c-4c52-9f61-0cb805980075"_Guid;

#pragma pack(push, 1)

struct DbData::ZeroPage {
    static const auto kPageType = DbPageType::kZero;
    DbPageHeader hdr;
    Guid signature;
    unsigned pageSize;
    pgno_t freeStoreRoot;
    pgno_t deprecatedStoreRoot;
    pgno_t metricStoreRoot;
    pgno_t metricTagStoreRoot;
};
static_assert(is_standard_layout_v<DbData::ZeroPage>);
static_assert(2 * sizeof(DbData::ZeroPage) <= kMinPageSize);

struct DbData::FreePage {
    static const auto kPageType = DbPageType::kFree;
    DbPageHeader hdr;
};

#pragma pack(pop)


/****************************************************************************
*
*   Variables
*
***/

static auto & s_perfPages = uperf("db.data pages (total)");
static auto & s_perfFreePages = uperf("db.data pages (free)");


/****************************************************************************
*
*   DbRootVersion
*
***/

//===========================================================================
DbRootVersion::DbRootVersion(DbTxn * txn, DbData * data)
    : txn(txn->makeTxn())
    , data(*data)
{}

//===========================================================================
DbRootVersion::~DbRootVersion() {
    // Remove pages that were deprecated (via replacement) when building the
    // next version.
    if (next) {
        swap(next->deprecatedPages, deprecatedPages);
        for (auto&& pgno : deprecatedPages) {
            data.freeDeprecatedPage(txn, (pgno_t) pgno);
        }
    }
}

//===========================================================================
shared_ptr<DbRootVersion> DbRootVersion::addNextVer(Lsx id) {
    assert(!next);
    next = make_shared<DbRootVersion>(&txn, &data);
    next->lsx = id;
    return next;
}


/****************************************************************************
*
*   DbRootSet
*
***/

//===========================================================================
DbRootSet::DbRootSet(
    DbData * data,
    shared_ptr<mutex> mut,
    shared_ptr<condition_variable> cv
)
    : m_data(*data)
    , m_mut(mut)
    , m_cv(cv)
{}

//===========================================================================
vector<DbRootVersion *> DbRootSet::firstRoots() {
    return { name.get() };
}

//===========================================================================


//===========================================================================
size_t DbRootSet::startUpdate(
    Lsx id,
    vector<shared_ptr<DbRootVersion>> roots
) {
    unique_lock lk(*m_mut);

    // Wait for available update capacity
    for (;;) {
        if (m_writeTxns.size() == kMaxActiveRootUpdates) {
            if (m_writeTxns.contains(id))
                break;
        } else {
            m_writeTxns.insert(id);
            break;
        }
        m_cv->wait(lk);
    }

    // Wait for last update to this root to complete
    shared_ptr<DbRootVersion> root;
    size_t pos = 0;
    for (;;) {
        for (pos = 0; pos < roots.size(); ++pos) {
            root = roots[pos];
            while (root->next)
                root = root->next;
            if (root->complete())
                goto FOUND;
        }
        m_cv->wait(lk);
    }

FOUND:
    root->addNextVer(id);
    return pos;
}

//===========================================================================
static bool eligible(
    unordered_set<Lsx> * path,
    Lsx id,
    const unordered_map<Lsx, unordered_set<Lsx>> & ref,
    const unordered_set<Lsx> & completeTxns
) {
    if (path->contains(id)) {
        // Recursive references are not blocking.
        return true;
    }
    auto i = ref.find(id);
    if (i == ref.end()) {
        // Has no references, therefore no blocking references.
        return true;
    }
    for (auto&& id : i->second) {
        if (!completeTxns.contains(id)) {
            // References incomplete transaction.
            return false;
        }
    }
    for (auto&& refId : i->second) {
        path->insert(id);
        auto okay = eligible(path, refId, ref, completeTxns);
        path->erase(id);
        if (!okay)
            return false;
    }
    return true;
}

//===========================================================================
shared_ptr<DbRootSet> DbRootSet::lockForCommit(Lsx id) {
    shared_ptr<DbRootSet> roots;
    unique_lock lk(*m_mut);
    if (m_writeTxns.contains(id)) {
        roots = shared_from_this();
        for (;;) {
            while (roots->m_next)
                roots = roots->m_next;
            if (!roots->m_commitInProgress)
                break;
            m_cv->wait(lk);
        }
        m_commitInProgress = true;
    }
    return roots;
}

//===========================================================================
unordered_set<Lsx> DbRootSet::commit(Lsx txnId) {
    unique_lock lk(*m_mut);
    assert(m_commitInProgress);

    if (!m_writeTxns.contains(txnId))
        return {txnId};
    m_completeTxns.insert(txnId);

    unordered_map<Lsx, unordered_set<Lsx>> ref;
    auto roots = firstRoots();
    for (auto && root : roots) {
        unordered_set<Lsx> found;
        auto ptr = root;
        for (;;) {
            if (!ptr || !ptr->complete()) {
                assert(!ptr || !ptr->next);
                break;
            }
            auto id = ptr->lsx;
            ref[id].insert(found.begin(), found.end());
            found.insert(id);
            ptr = ptr->next.get();
        }
    }

    // Transactions always reference themselves.
    unordered_map<Lsx, unordered_set<Lsx>> refBy;
    for (auto&& id : m_writeTxns) {
        ref[id].insert(id);
        for (auto&& bid : ref[id])
            refBy[bid].insert(id);
    }

    unordered_set<Lsx> ready;
    unordered_set<Lsx> path;
    for (auto&& id : m_completeTxns) {
        if (eligible(&path, id, refBy, m_completeTxns))
            ready.insert(id);
    }
    return ready;
}

//===========================================================================
shared_ptr<DbRootSet> DbRootSet::publishNextSet(
    const unordered_set<Lsx> & txns
) {
    scoped_lock lk(*m_mut);
    auto out = make_shared<DbRootSet>(&m_data, m_mut, m_cv);
    out->m_commitInProgress = true;
    out->m_writeTxns = m_writeTxns;
    out->m_completeTxns = m_completeTxns;
    for (auto&& id : txns) {
        out->m_writeTxns.erase(id);
        out->m_completeTxns.erase(id);
    }

    auto roots = firstRoots();
    auto nexts = out->firstRoots();
    assert(roots.size() == nexts.size());
    auto nroot = nexts.begin();
    for (auto i = roots.begin(); i != roots.end(); ++i, ++nroot) {
        auto n = *i;
        while (n && txns.contains(n->lsx))
            n = n->next.get();
        *nroot = n;
    #ifndef NDEBUG
        while (n) {
            assert(!txns.contains(n->lsx));
            n = n->next.get();
        }
    #endif
    }

    m_commitInProgress = false;
    m_data.m_metricRoots.store(out);
    return out;
}

//===========================================================================
void DbRootSet::unlock() {
    unique_lock lk(*m_mut);
    assert(m_commitInProgress);
    m_commitInProgress = false;
    m_cv->notify_all();
}


/****************************************************************************
*
*   DbData
*
***/

//===========================================================================
[[maybe_unused]]
static size_t queryPageSize(FileHandle f) {
    if (!f)
        return 0;
    DbData::ZeroPage zp;
    uint64_t bytes;
    if (fileReadWait(&bytes, &zp, sizeof(zp), f, 0); bytes != sizeof(zp))
        return 0;
    if (zp.hdr.type != zp.kPageType)
        return 0;
    if (zp.signature != kDataFileSig)
        return 0;
    return zp.pageSize;
}

//===========================================================================
DbData::~DbData () {
    metricClearCounters();
    s_perfPages -= (unsigned) m_numPages;
    s_perfFreePages -= (unsigned) m_numFree;
}

//===========================================================================
void DbData::openForApply(size_t pageSize, EnumFlags<DbOpenFlags> flags) {
    m_verbose = flags.any(fDbOpenVerbose);
    m_pageSize = pageSize;
}

//===========================================================================
bool DbData::openForUpdate(
    DbTxn & txn,
    IDbDataNotify * notify,
    string_view name,
    EnumFlags<DbOpenFlags> flags
) {
    assert(m_pageSize);
    m_verbose = flags.any(fDbOpenVerbose);

    auto zp = txn.pin<ZeroPage>(kZeroPageNum);
    if (zp->hdr.type == DbPageType::kInvalid) {
        txn.walZeroInit(kZeroPageNum);
        zp = txn.pin<ZeroPage>(kZeroPageNum);
    }

    if (zp->signature != kDataFileSig) {
        logMsgError() << "Bad signature, " << name;
        return false;
    }
    if (zp->pageSize != m_pageSize) {
        logMsgError() << "Mismatched page size, " << name;
        return false;
    }
    m_numPages = txn.numPages();
    s_perfPages += (unsigned) m_numPages;
    m_freeStoreRoot = zp->freeStoreRoot;
    m_deprecatedStoreRoot = zp->deprecatedStoreRoot;
    m_metricStoreRoot = zp->metricStoreRoot;

    auto nameRoot = make_shared<DbRootVersion>(&txn, this);
    nameRoot->root = zp->metricTagStoreRoot;
    m_metricRoots = make_shared<DbRootSet>(
        this,
        make_shared<mutex>(),
        make_shared<condition_variable>()
    );
    m_metricRoots.load()->name = nameRoot;

    if (m_numPages == 1) {
        auto pgno = allocPgno(txn);
        assert(pgno == m_freeStoreRoot);
        txn.walRadixInit(pgno, 0, 0, nullptr, nullptr);
        pgno = allocPgno(txn);
        assert(pgno == m_deprecatedStoreRoot);
        txn.walRadixInit(pgno, 0, 0, nullptr, nullptr);
        pgno = allocPgno(txn);
        assert(pgno == m_metricStoreRoot);
        txn.walRadixInit(pgno, 0, 0, nullptr, nullptr);
        assert(m_metricRoots.load()->name->root == pgno_t::npos);
    }

    if (m_verbose)
        logMsgInfo() << "Load free page list";
    if (!loadFreePages(txn))
        return false;
    if (!loadDeprecatedPages(txn))
        return false;
    if (m_verbose)
        logMsgInfo() << "Build metric index";
    if (!loadMetrics(txn, notify))
        return false;

    return true;
}

//===========================================================================
DbStats DbData::queryStats() const {
    DbStats s;
    s.pageSize = (unsigned) m_pageSize;
    s.bitsPerPage = (unsigned) bitsPerPage();
    s.metricNameSize = (unsigned) metricNameSize(m_pageSize);
    s.samplesPerPage[kSampleTypeInvalid] = 0;
    for (int8_t i = 1; i < kSampleTypes; ++i)
        s.samplesPerPage[i] = (unsigned) samplesPerPage(DbSampleType{i});

    {
        shared_lock lk{m_mposMut};
        s.metrics = m_numMetrics;
    }

    scoped_lock lk{m_pageMut};
    s.numPages = (unsigned) m_numPages;
    s.freePages = (unsigned) m_freePages.count(0, m_numPages);
    return s;
}

//===========================================================================
std::shared_ptr<DbRootSet> DbData::metricRootsInstance() {
    return m_metricRoots.load();
}


/****************************************************************************
*
*   Free store
*
***/

//===========================================================================
bool DbData::loadFreePages(DbTxn & txn) {
    assert(!m_freePages);
    if (!bitLoad(txn, &m_freePages, m_freeStoreRoot))
        return false;
    if (appStopping())
        return false;
    auto num = (unsigned) m_freePages.count();
    m_numFree += num;
    s_perfFreePages += num;

    // Validate that pages in free list are in fact free.
    pgno_t blank = {};
    for (auto && p : m_freePages) {
        auto pgno = (pgno_t) p;
        if (pgno >= m_numPages)
            break;
        auto fp = txn.pin<DbPageHeader>(pgno);
        if (!fp
            || fp->type != DbPageType::kInvalid
                && fp->type != DbPageType::kFree
        ) {
            logMsgError() << "Bad free page #" << pgno << ", type "
                << (unsigned) fp->type;
            return false;
        }
        if (fp->type != DbPageType::kInvalid) {
            if (blank) {
                logMsgError() << "Blank data page #" << pgno << ", type "
                    << (unsigned) fp->type;
                return false;
            }
        } else if (!blank) {
            blank = pgno;
        }
        if (appStopping())
            return false;
    }
    if (blank && blank < m_numPages) {
        auto trimmed = (unsigned) (m_numPages - blank);
        logMsgInfo() << "Trimmed " << trimmed << " blank pages";
        m_numPages = blank;
        s_perfPages -= trimmed;
    }

    return true;
}

//===========================================================================
bool DbData::loadDeprecatedPages(DbTxn & txn) {
    assert(!m_deprecatedPages);
    if (!bitLoad(txn, &m_deprecatedPages, m_deprecatedStoreRoot))
        return false;
    if (appStopping())
        return false;
    while (m_deprecatedPages) {
        auto pgno = (pgno_t) m_deprecatedPages.pop_front();
        freePage(txn, pgno);
    }
    return true;
}

//===========================================================================
pgno_t DbData::allocPgno(DbTxn & txn) {
    scoped_lock lk{m_pageMut};
    DbTxn::PinScope pins(txn);

    auto freed = false;
    auto grew = false;
    auto pgno = pgno_t{};
    assert(m_numFree == m_freePages.count());
    if (m_freePages) {
        freed = true;
        pgno = (pgno_t) m_freePages.pop_front();
        m_numFree -= 1;
        s_perfFreePages -= 1;
    } else {
        pgno = (pgno_t) m_numPages;
    }
    if (pgno >= m_numPages) {
        assert(pgno == m_numPages);
        // This is a new page at the end of the file, either previously
        // untracked or tracked as a "free" page. See the description in
        // freePages() for why this might be "free".
        grew = true;
        m_numPages += 1;
        s_perfPages += 1;
        txn.growToFit(pgno);
    }
    if (freed) {
        // Reusing free page, remove from free page index.
        //
        // This bitUpsert must come after the file grow. Otherwise, if numPages
        // wasn't incremented, pgno is the last free page, and bitUpsert needs
        // to allocate a page, it will take the pgno page that we're trying to
        // use.
        //
        // The reason removing an entry from the bitmap of free pages might
        // need to allocate a page is because if we're removing the last bit of
        // a page, the page will be freed... which means it must be added to
        // this bitmap.
        [[maybe_unused]] bool updated =
            bitUpsert(txn, m_freeStoreRoot, 0, pgno, pgno + 1, false);
        assert(updated);
    }

    // Return with the newly allocated page pinned.
    [[maybe_unused]] auto p = txn.pin<DbPageHeader>(pgno);
    assert(grew && p->type == DbPageType::kInvalid
        || !grew && p->type == DbPageType::kFree
    );
    pins.keep(pgno);
    return pgno;
}

//===========================================================================
void DbData::freePage(DbTxn & txn, pgno_t pgno) {
    scoped_lock lk{m_pageMut};
    DbTxn::PinScope pins(txn);

    assert(pgno < m_numPages);
    auto p = txn.pin<DbPageHeader>(pgno);
    auto type = p->type;
    switch (type) {
    case DbPageType::kMetric:
        metricDestructPage(txn, pgno);
        break;
    case DbPageType::kRadix:
        radixDestructPage(txn, pgno);
        break;
    case DbPageType::kBitmap:
    case DbPageType::kSample:
        break;
    case DbPageType::kFree:
        logMsgFatal() << "freePage(" << (unsigned) pgno
            << "): page already free";
    default:
        logMsgFatal() << "freePage(" << (unsigned) pgno
            << "): invalid page type (" << (unsigned) type << ")";
    }

    auto noPages = !m_freePages && !txn.freePages();
    txn.walPageFree(pgno);
    assert(m_freeStoreRoot);
    [[maybe_unused]] bool updated =
        bitUpsert(txn, m_freeStoreRoot, 0, pgno, pgno + 1, true);
    assert(updated);
    auto bpp = bitsPerPage();
    if (noPages && pgno / bpp == m_numPages / bpp) {
        // There were no free pages and the newly freed page is near the end of
        // the file where it is covered by the last page of the free pages
        // index. Fill the rest of this last page with as many entries as will
        // fit, representing not yet existing pages past the end of the file.
        //
        // By having extra free pages in the free page index, churn is reduced
        // when expanding a full file. Otherwise, when the last free page is
        // used and it's entry is removed from the free page index, the index
        // page is freed, which requires a new entry (and therefore a new index
        // page) to be added to the index.
        auto num = bpp - m_numPages % bpp;
        if (num) {
            bitUpsert(
                txn,
                m_freeStoreRoot,
                0,
                m_numPages,
                m_numPages + num,
                true
            );
            // These pages past the end of the file were already available and
            // not dependent on the transaction being committed, therefore they
            // can be made immediately available for use.
            m_freePages.insert((uint32_t) m_numPages, num);
            m_numFree += num;
            s_perfFreePages += (unsigned) num;
        }
    }
}

//===========================================================================
void DbData::publishFreePages(const UnsignedSet & freePages) {
    if (auto num = freePages.count()) {
        scoped_lock lk(m_pageMut);
        assert(!freePages.intersects(m_freePages));
        m_freePages.insert(freePages);
        m_numFree += num;
        s_perfFreePages += (unsigned) num;
    }
}

//===========================================================================
void DbData::deprecatePage(DbTxn & txn, pgno_t pgno) {
    scoped_lock lk{m_pageMut};
    DbTxn::PinScope pins(txn);

    if constexpr (DIMAPP_LIB_BUILD_DEBUG) {
        auto p = txn.pin<DbPageHeader>(pgno);
        assert(p->type != DbPageType::kInvalid
            && p->type != DbPageType::kFree);
    }
    assert(m_deprecatedStoreRoot);
    [[maybe_unused]] bool updated =
        bitUpsert(txn, m_deprecatedStoreRoot, 0, pgno, pgno + 1, true);
    assert(updated);
    updated = m_deprecatedPages.insert(pgno);
    assert(updated);
}

//===========================================================================
void DbData::freeDeprecatedPage(DbTxn & txn, pgno_t pgno) {
    [[maybe_unused]] bool updated = false;
    updated = bitUpsert(txn, m_deprecatedStoreRoot, 0, pgno, pgno + 1, false);
    assert(updated);
    freePage(txn, pgno);
    scoped_lock lk{m_pageMut};
    updated = m_deprecatedPages.erase(pgno);
    assert(updated);
}


/****************************************************************************
*
*   DbWalRecInfo
*
***/

#pragma pack(push, 1)

namespace {

struct TagRootUpdateRec {
    DbWal::Record hdr;
    pgno_t rootPage;
};

} // namespace

#pragma pack(pop)


static DbWalRegisterRec s_dataRecInfo = {
    { kRecTypeZeroInit,
        DbWalRecInfo::sizeFn<DbWal::Record>,
        [](auto args) {
            args.notify->onWalApplyZeroInit(args.page);
        },
    },
    { kRecTypeTagRootUpdate,
        DbWalRecInfo::sizeFn<TagRootUpdateRec>,
        [](auto args) {
            auto rec = reinterpret_cast<const TagRootUpdateRec *>(args.rec);
            args.notify->onWalApplyTagRootUpdate(args.page, rec->rootPage);
        },
    },
    { kRecTypePageFree,
        DbWalRecInfo::sizeFn<DbWal::Record>,
        [](auto args) {
            args.notify->onWalApplyPageFree(args.page);
        },
    },
};


/****************************************************************************
*
*   DbTxn
*
***/

//===========================================================================
void DbTxn::walZeroInit(pgno_t pgno) {
    auto [rec, bytes] = alloc<DbWal::Record>(kRecTypeZeroInit, pgno);
    wal(rec, bytes);
}

//===========================================================================
void DbTxn::walTagRootUpdate(pgno_t pgno, pgno_t rootPage) {
    auto [rec, bytes] = alloc<TagRootUpdateRec>(kRecTypeTagRootUpdate, pgno);
    rec->rootPage = rootPage;
    wal(&rec->hdr, bytes);
}

//===========================================================================
void DbTxn::walPageFree(pgno_t pgno) {
    auto [rec, bytes] = alloc<DbWal::Record>(kRecTypePageFree, pgno);
    wal(rec, bytes);
    m_freePages.insert(pgno);
}


/****************************************************************************
*
*   Log apply
*
***/

//===========================================================================
void DbData::onWalApplyCheckpoint(Lsn lsn, Lsn startLsn)
{}

//===========================================================================
void DbData::onWalApplyBeginTxn(Lsn lsn, LocalTxn localTxn)
{}

//===========================================================================
void DbData::onWalApplyCommitTxn(Lsn lsn, LocalTxn localTxn)
{}

//===========================================================================
void DbData::onWalApplyGroupCommitTxn(
    Lsn lsn,
    const std::vector<LocalTxn> & localTxns
)
{}

//===========================================================================
void DbData::onWalApplyZeroInit(void * ptr) {
    auto zp = static_cast<ZeroPage *>(ptr);
    assert(zp->hdr.type == DbPageType::kInvalid);
    // We only initialize the zero page when making a new database, so we can
    // forgo the normal logic to memset when initialized from free pages.
    zp->hdr.type = zp->kPageType;
    zp->hdr.id = 0;
    assert(zp->hdr.pgno == kZeroPageNum);
    zp->signature = kDataFileSig;
    zp->pageSize = (unsigned) m_pageSize;
    zp->freeStoreRoot = kDefaultFreeStoreRoot;
    zp->deprecatedStoreRoot = kDefaultDeprecatedStoreRoot;
    zp->metricStoreRoot = kDefaultMetricStoreRoot;
    zp->metricTagStoreRoot = pgno_t::npos;
}

//===========================================================================
void DbData::onWalApplyTagRootUpdate(void * ptr, pgno_t rootPage) {
    auto zp = static_cast<ZeroPage *>(ptr);
    assert(zp->hdr.type == DbPageType::kZero);
    zp->metricTagStoreRoot = rootPage;
}

//===========================================================================
void DbData::onWalApplyPageFree(void * ptr) {
    auto fp = static_cast<FreePage *>(ptr);
    assert(fp->hdr.type != DbPageType::kInvalid
        && fp->hdr.type != DbPageType::kFree);
    fp->hdr.type = DbPageType::kFree;
}
