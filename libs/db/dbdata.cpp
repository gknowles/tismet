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
constexpr auto kDefaultRootStoreRoot = (pgno_t) 1;
constexpr auto kRootRootId = 1;
constexpr auto kRootNameRootId = 2;

const auto kDataFileSig = "66b1e542-541c-4c52-9f61-0cb805980075"_Guid;

#pragma pack(push, 1)

struct DbData::ZeroPage {
    static const auto kPageType = DbPageType::kZero;
    DbPageHeader hdr;
    Guid signature;
    unsigned pageSize;
    pgno_t rootStoreRoot;
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
static auto & s_perfDepPages = uperf("db.data pages (deprecated)");


/****************************************************************************
*
*   DbRootVersion
*
***/

//===========================================================================
DbRootVersion::DbRootVersion(DbTxn * txn, DbData * data, unsigned rootId)
    : rootId(rootId)
    , txn(txn->makeTxn())
    , data(*data)
{}

//===========================================================================
DbRootVersion::~DbRootVersion() {
    // Remove pages that were deprecated (via replacement) when building the
    // next version.
    for (auto&& pgno : deprecatedPages)
        data.freeDeprecatedPage(txn, (pgno_t) pgno);
}

//===========================================================================
void DbRootVersion::loadRoot() {
    assert(root == pgno_t::npos);
    root = data.loadRoot(txn, rootId);
}

//===========================================================================
shared_ptr<DbRootVersion> DbRootVersion::addNextVer(Lsx id) {
    assert(!next);
    next = make_shared<DbRootVersion>(&txn, &data, rootId);
    next->root = (pgno_t) 0;
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
vector<shared_ptr<DbRootVersion> *> DbRootSet::firstRoots() {
    return { &name };
}

//===========================================================================
pair<shared_ptr<DbRootVersion>, size_t> DbRootSet::beginUpdate(
    Lsx id,
    const vector<shared_ptr<DbRootVersion>> & roots
) {
    assert(id);
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
    return {root, pos};
}

//===========================================================================
void DbRootSet::rollbackUpdate(shared_ptr<DbRootVersion> root) {
    unique_lock lk(*m_mut);
    while (root->next && root->next->complete())
        root = root->next;
    assert(!root->next->complete());
    root->next.reset();
    m_cv->notify_all();
}

//===========================================================================
void DbRootSet::commitUpdate(shared_ptr<DbRootVersion> root, pgno_t pgno) {
    unique_lock lk(*m_mut);
    while (root->next)
        root = root->next;
    assert(!root->complete());
    root->root = pgno;
    m_cv->notify_all();
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
        auto ptr = root->get();
        if (ptr)
            ptr = ptr->next.get();
        for (;;) {
            if (!ptr)
                break;
            if (!ptr->complete()) {
                assert(!ptr->next);
                break;
            }
            auto id = ptr->lsx;
            ref[id].insert(found.begin(), found.end());
            found.insert(id);
            ptr = ptr->next.get();
        }
    }

    // Populate reverse reference index.
    unordered_map<Lsx, unordered_set<Lsx>> refBy;
    for (auto&& id : m_writeTxns) {
        // Transactions always reference themselves.
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
        auto n = **i;
        **nroot = n;

        // Search for first version after txns being published:
        //  - Skip first, it's the previous version.
        //  - If next isn't from our txns, keep the "previous" version, it
        //    wasn't updated.
        //  - Find last version from our txns, publish it.
        //  - (Extra credit) Assert that all remaining versions aren't from any
        //    of our txns.
        if (!n) {
            // Root has no versions.
            continue;
        }
        assert(!txns.contains(n->lsx) && "Republishing old root");
        n = n->next;
        while (n && txns.contains(n->lsx)) {
            **nroot = n;
            n = n->next;
        }
    #ifndef NDEBUG
        while (n) {
            assert(!txns.contains(n->lsx) && "Unpublished root update");
            n = n->next;
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
DbData::DbData() {
    using enum DbPageType;
    const RootDef defs[] = {
        { ":root",       kRadix, kRootRootId },
        { ":rootName",   kTrie,  kRootNameRootId },
        { ":free",       kRadix, {}, &m_freeRoot },
        { ":deprecated", kRadix, {}, &m_deprecatedRoot },
        { ":metric",     kRadix, {}, &m_metricRoot },
        { ":metricName", kTrie },
    };
    m_rootDefs.assign_range(defs);
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
    m_readOnly = flags.any(fDbOpenReadOnly);

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
    m_newFile = (m_numPages == 1);

    if (!loadRoots(txn, zp->rootStoreRoot))
        return false;
    if (!loadFreePages(txn))
        return false;
    if (!loadDeprecatedPages(txn))
        return false;

    if (!upgradeRoots(txn))
        return false;

    // Metric root set
    auto nameId = m_rootIdByName[":metricName"];
    assert(nameId);
    auto nameRoot = make_shared<DbRootVersion>(&txn, this, nameId);
    m_metricRoots = make_shared<DbRootSet>(
        this,
        make_shared<mutex>(),
        make_shared<condition_variable>()
    );
    m_metricRoots.load()->name = nameRoot;

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
    s.deprecatedPages = (unsigned) m_deprecatedPages.count();
    return s;
}


/****************************************************************************
*
*   Roots
*
***/

//===========================================================================
bool DbData::loadRoots(DbTxn & txn, pgno_t storeRoot) {
    assert(m_rootNameById.empty());

    m_rootRoot = storeRoot;

    if (!m_rootRoot) {
        m_rootRoot = allocPgno(txn);
        txn.walRadixInit(m_rootRoot, 0, 0, nullptr, nullptr);
        txn.walRootUpdate(kZeroPageNum, m_rootRoot);
    }
    auto nameStoreRoot = kZeroPageNum;
    if (!radixFind(txn, &nameStoreRoot, m_rootRoot, kRootNameRootId)) {
        if (storeRoot) {
            logMsgError() << "Missing :rootName store";
            return false;
        }
        nameStoreRoot = pgno_t::npos;
    }
    DbPageHeap heap(&txn, this, kRootNameRootId, nameStoreRoot);
    StrTrieBase trie(&heap);
    unsigned lastId = 0;
    for (auto&& val : trie) {
        auto&& [kview, id] = trieKeyToId(val);
        if (id > lastId)
            lastId = id;
        if (!kview.size()) {
            logMsgError() << "Invalid key (missing root name) in :rootName";
            return false;
        }
        if (!id) {
            logMsgError() << "Invalid key (missing root id) in :rootName";
            return false;
        }
        auto key = string(kview);
        if (m_rootIdByName.contains(key)) {
            logMsgError() << "Duplicate stored root Id name: '" << key << "'";
            return false;
        }
        m_rootIdByName[key] = id;
    }
    assert(heap.destroyed().empty());
    m_rootNameById.resize(lastId + 1);
    for (auto&& [key, id] : m_rootIdByName) {
        if (!m_rootNameById[id].empty()) {
            logMsgError() << "Duplicate stored root Id: " << id;
            return false;
        }
        m_rootNameById[id] = key;
    }
    for (unsigned i = 1; i < m_rootNameById.size(); ++i) {
        if (m_rootNameById[i].empty())
            m_freeRootIds.insert(i);
    }

    for (auto&& def : m_rootDefs) {
        if (auto i = m_rootIdByName.find(def.name); i != m_rootIdByName.end())
            def.id = i->second;
        if (def.root)
            *def.root = loadRoot(txn, def.id);
    }
    return true;
}

//===========================================================================
bool DbData::upgradeRoots(DbTxn & txn) {
    assert(m_rootRoot);

    // Initialize radix index root pages, this is done specifically to ensure
    // that the free and deprecated lists are initialized.
    for (auto&& def : m_rootDefs) {
        if (def.type == DbPageType::kRadix
            && def.root
            && *def.root == pgno_t::npos
        ) {
            def.changed = true;
            *def.root = allocPgno(txn);
            txn.walRadixInit(*def.root, 0, 0, nullptr, nullptr);
        }
    }

    auto nameStoreRoot = loadRoot(txn, kRootNameRootId);
    DbPageHeap heap(&txn, this, kRootNameRootId, nameStoreRoot);
    StrTrieBase trie(&heap);

    // Add default roots to root indexes if they aren't already there.
    for (auto&& def : m_rootDefs) {
        if (m_rootIdByName.contains(def.name)) {
            auto id = m_rootIdByName[def.name];
            if (def.id) {
                if (def.id != id) {
                    logMsgError() << "Reserved root '" << def.name << "' has "
                        "id " << id << " (expected " << def.id << ")";
                    return false;
                }
                continue;
            }
            def.id = id;
            continue;
        }
        // Assign id (if needed), and add to name by Id index
        if (def.id) {
            if (def.id >= m_rootNameById.size()) {
                m_rootNameById.resize(def.id + 1);
            } else {
                if (!m_rootNameById[def.id].empty()) {
                    logMsgError() << "Reserved root Id " << def.id
                        << " assigned to '" << m_rootNameById[def.id] << "' "
                        << "but is reversed for '" << def.name << "'";
                    return false;
                }
            }
            m_rootNameById[def.id] = def.name;
        } else {
            if (def.root)
                def.changed = true;
            if (m_freeRootIds) {
                def.id = m_freeRootIds.pop_front();
                assert(m_rootNameById[def.id].empty());
                m_rootNameById[def.id] = def.name;
            } else {
                def.id = (unsigned) m_rootNameById.size();
                m_rootNameById.push_back(def.name);
            }
        }
        // Add to Id by name index
        assert(!m_rootIdByName.contains(def.name));
        m_rootIdByName[def.name] = def.id;
        // Add to persistent rootName index
        trie.insert(trieKey(def.name, def.id));
    }
    for (auto&& pgno : heap.destroyed())
        freeDeprecatedPage(txn, (pgno_t) pgno);

    // Save radix index roots
    for (auto&& def : m_rootDefs) {
        if (def.changed) {
            assert(def.id && (!def.root || *def.root != pgno_t::npos));
            updateRoot(txn, def.id, *def.root);
        }
    }

    return true;
}

//===========================================================================
pgno_t DbData::loadRoot(DbTxn & txn, unsigned rootId) {
    scoped_lock lk{m_pageMut};
    DbTxn::PinScope pins(txn);

    pgno_t out = pgno_t::npos;
    if (!radixFind(txn, &out, m_rootRoot, rootId))
        out = pgno_t::npos;
    return out;
}

//===========================================================================
pgno_t DbData::loadRoot(DbTxn & txn, const string & rootName) {
    scoped_lock lk{m_pageMut};
    DbTxn::PinScope pins(txn);

    pgno_t out = pgno_t::npos;
    auto i = m_rootIdByName.find(rootName);
    if (i != m_rootIdByName.end())
        out = loadRoot(txn, i->second);
    return out;
}

//===========================================================================
void DbData::updateRoot(DbTxn & txn, unsigned rootId, pgno_t root) {
    scoped_lock lk{m_pageMut};
    DbTxn::PinScope pins(txn);

    radixSwapValue(txn, m_rootRoot, rootId, root);
}

//===========================================================================
void DbData::updateRoot(DbTxn & txn, const string & name, pgno_t root) {
    scoped_lock lk{m_pageMut};

    auto id = m_rootIdByName[name];
    assert(id && "free page index not found");
    updateRoot(txn, id, root);
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
    if (m_verbose)
        logMsgInfo() << "Load free page list";

    if (m_freeRoot == pgno_t::npos) {
        if (m_readOnly) {
            logMsgError() << "Missing free page list";
            return false;
        }
        m_freeRoot = allocPgno(txn);
        txn.walRadixInit(m_freeRoot, 0, 0, nullptr, nullptr);
    }

    if (!bitLoad(txn, &m_freePages, m_freeRoot))
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
    if (m_deprecatedRoot == pgno_t::npos) {
        if (m_readOnly) {
            logMsgError() << "Missing deprecated page list";
            return false;
        }
        m_deprecatedRoot = allocPgno(txn);
        txn.walRadixInit(m_deprecatedRoot, 0, 0, nullptr, nullptr);
    }
    if (!bitLoad(txn, &m_deprecatedPages, m_deprecatedRoot))
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
        // This bitAssign must come after the file grow. Otherwise, if numPages
        // wasn't incremented, pgno is the last free page, and bitAssign needs
        // to allocate a page, it will take the pgno page that we're trying to
        // use.
        //
        // The reason removing an entry from the bitmap of free pages might
        // need to allocate a page is because if we're removing the last bit of
        // a page, the page will be freed... which means it must be added to
        // this bitmap.
        [[maybe_unused]] bool updated =
            bitAssign(txn, m_freeRoot, 0, pgno, pgno + 1, false);
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
    case DbPageType::kTrie:
        // Trie pages aren't destroyed recursively because pages may be deleted
        // (and replaced with another page) from the middle of a trie index,
        // keeping the preexisting children.
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
    assert(m_freeRoot);
    [[maybe_unused]] bool updated =
        bitAssign(txn, m_freeRoot, 0, pgno, pgno + 1, true);
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
            bitAssign(
                txn,
                m_freeRoot,
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
    assert(m_deprecatedRoot);
    [[maybe_unused]] bool updated =
        bitAssign(txn, m_deprecatedRoot, 0, pgno, pgno + 1, true);
    assert(updated);
    updated = m_deprecatedPages.insert(pgno);
    assert(updated);
    s_perfDepPages += 1;
}

//===========================================================================
void DbData::freeDeprecatedPage(DbTxn & txn, pgno_t pgno) {
    [[maybe_unused]] bool updated = false;
    updated = bitAssign(txn, m_deprecatedRoot, 0, pgno, pgno + 1, false);
    assert(updated);
    freePage(txn, pgno);
    scoped_lock lk{m_pageMut};
    updated = m_deprecatedPages.erase(pgno);
    assert(updated);
    s_perfDepPages -= 1;
}


/****************************************************************************
*
*   Trie indexes
*
***/

//===========================================================================
// static
string DbData::trieKey(string_view name, uint32_t id) {
    string key;
    auto nameLen = name.size();
    uint8_t buf[sizeof id];
    auto bufPos = sizeof buf;
    while (id > 0) {
        buf[--bufPos] = id % 256;
        id >>= 8;
    }
    key.resize(nameLen + 1 + sizeof buf - bufPos);
    memcpy(key.data(), name.data(), nameLen);
    key[nameLen] = '\0';
    memcpy(key.data() + nameLen + 1, buf + bufPos, sizeof buf - bufPos);
    return key;
}

//===========================================================================
// static
pair<string_view, uint32_t> DbData::trieKeyToId(string_view val) {
    auto nameLen = val.find('\0');
    assert(nameLen != string::npos);
    auto ptr = val.data() + nameLen;
    auto eptr = val.data() + val.size();
    uint32_t id = 0;
    for (; ptr < eptr; ++ptr)
        id = 256 * id + (uint8_t) *ptr;
    return {val.substr(0, nameLen), id};
}

//===========================================================================
void DbData::trieApply(
    DbTxn & txn,
    const vector<shared_ptr<DbRootVersion>> & roots,
    const vector<string> & keys,
    function<bool(StrTrieBase* index, const string & key)> fn
) {
    assert(size(roots) == size(keys));
    vector<size_t> ords(roots.size());
    for (size_t i = 0; i < ords.size(); ++i)
        ords[i] = i;
    while (!ords.empty()) {
        DbTxn::PinScope pins(txn);
        auto [root, pos] = txn.roots().beginUpdate(txn.getLsx(), roots);
        assert(root->next);
        assert(!root->next->complete());
        auto key = keys[ords[pos]];
        if (pos != ords.size() - 1)
            ords[pos] = ords.back();
        ords.pop_back();
        DbPageHeap heap(&txn, this, root->rootId, root->root);
        StrTrieBase trie(&heap);
        bool found = fn(&trie, key);
        if (!found) {
            txn.roots().rollbackUpdate(root);
        } else {
            root->deprecatedPages.insert(heap.destroyed());
            txn.roots().commitUpdate(root, (pgno_t) heap.root());
        }
    }
}

//===========================================================================
void DbData::trieInsert(
    DbTxn & txn,
    const vector<shared_ptr<DbRootVersion>> & roots,
    const vector<string> & keys
) {
    trieApply(
        txn,
        roots,
        keys,
        [](auto index, auto key) { return index->insert(key); }
    );
}

//===========================================================================
void DbData::trieErase(
    DbTxn & txn,
    const vector<shared_ptr<DbRootVersion>> & roots,
    const vector<string> & keys
) {
    trieApply(
        txn,
        roots,
        keys,
        [](auto index, auto key) { return index->erase(key); }
    );
}


/****************************************************************************
*
*   DbWalRecInfo
*
***/

#pragma pack(push, 1)

namespace {

struct RootUpdateRec {
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
    { kRecTypeRootUpdate,
        DbWalRecInfo::sizeFn<RootUpdateRec>,
        [](auto args) {
            auto rec = reinterpret_cast<const RootUpdateRec *>(args.rec);
            args.notify->onWalApplyRootUpdate(args.page, rec->rootPage);
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
void DbTxn::walRootUpdate(pgno_t pgno, pgno_t rootPage) {
    auto [rec, bytes] = alloc<RootUpdateRec>(kRecTypeRootUpdate, pgno);
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
    zp->rootStoreRoot = kZeroPageNum;
}

//===========================================================================
void DbData::onWalApplyRootUpdate(void * ptr, pgno_t rootPage) {
    auto zp = static_cast<ZeroPage *>(ptr);
    assert(zp->hdr.type == DbPageType::kZero);
    zp->rootStoreRoot = rootPage;
}

//===========================================================================
void DbData::onWalApplyPageFree(void * ptr) {
    auto fp = static_cast<FreePage *>(ptr);
    assert(fp->hdr.type != DbPageType::kInvalid
        && fp->hdr.type != DbPageType::kFree);
    fp->hdr.type = DbPageType::kFree;
}
