// Copyright Glen Knowles 2017 - 2026.
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
    , txn(txn->makeTxn(false))
    , data(*data)
{}

//===========================================================================
DbRootVersion::~DbRootVersion() {
    if (deprecatedPages) {
        // Remove pages that were deprecated (via replacement) when building
        // the next version.
        data.freeDeprecatedPages(txn, deprecatedPages);
        auto freePages = txn.commit();
        data.publishFreePages(freePages);
    }
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
DbRootSet::DbRootSet(DbData * data) {
    m_shared = make_shared<Shared>(*data);
}

//===========================================================================
vector<shared_ptr<DbRootVersion> *> DbRootSet::firstRoots() {
    return { &info, &idByName };
}

//===========================================================================
pair<shared_ptr<DbRootVersion>, size_t> DbRootSet::beginUpdate(
    Lsx id,
    const vector<shared_ptr<DbRootVersion>> & roots
) {
    assert(id);
    unique_lock lk(m_shared->mut);

    // Wait for available update capacity
    for (;;) {
        if (m_shared->writeTxns.size() == kMaxActiveRootUpdates) {
            if (m_shared->writeTxns.contains(id))
                break;
        } else {
            m_shared->writeTxns.insert(id);
            break;
        }
        m_shared->cv.wait(lk);
    }

    if (m_shared->completeTxns.contains(id)) {
        assert(!"Updating root using completed transaction.");
    }

    // Wait for last update to this root to complete
    shared_ptr<DbRootVersion> root;
    size_t pos = 0;
    for (;;) {
        for (pos = 0; pos < roots.size(); ++pos) {
            root = roots[pos];
            while (root->next)
                root = root->next;
            if (root->complete()) {
                root->addNextVer(id);
                return {root, pos};
            }
        }
        m_shared->cv.wait(lk);
    }
}

//===========================================================================
void DbRootSet::rollbackUpdate(shared_ptr<DbRootVersion> root) {
    unique_lock lk(m_shared->mut);
    assert(root->next);
    assert(!root->next->complete());
    root->next.reset();
    m_shared->cv.notify_all();
}

//===========================================================================
void DbRootSet::commitUpdate(shared_ptr<DbRootVersion> root, pgno_t pgno) {
    unique_lock lk(m_shared->mut);
    assert(root->next);
    assert(!root->next->complete());
    root->next->root = pgno;
    m_shared->cv.notify_all();
}

//===========================================================================
shared_ptr<DbRootSet> DbRootSet::lockForCommit(Lsx id) {
    shared_ptr<DbRootSet> roots;
    unique_lock lk(m_shared->mut);
    if (m_shared->writeTxns.contains(id)) {
        for (;;) {
            if (!m_shared->commitInProgress)
                break;
            m_shared->cv.wait(lk);
        }
        m_shared->commitInProgress = true;
        roots = shared_from_this();
        for (; roots->m_next; roots = roots->m_next) {}
    }
    return roots;
}

//===========================================================================
void DbRootSet::unlock_UNLK(unique_lock<mutex> && lk) {
    assert(lk && lk.mutex() == &m_shared->mut);
    assert(!m_next);
    assert(m_shared->commitInProgress);
    m_shared->commitInProgress = false;
    lk.unlock();
    m_shared->cv.notify_all();
}

//===========================================================================
void DbRootSet::unlock() {
    unique_lock lk(m_shared->mut);
    unlock_UNLK(move(lk));
}

//===========================================================================
unordered_set<Lsx> DbRootSet::findCompleteTxns(Lsx txnId) {
    unique_lock lk(m_shared->mut);
    assert(m_shared->commitInProgress);

    if (!m_shared->writeTxns.contains(txnId))
        return {txnId};
    if (!m_shared->completeTxns.insert(txnId).second)
        assert(!"Transaction already completed.");

    if (m_shared->writeTxns.size() == m_shared->completeTxns.size()) {
        // All write txns are complete, so commit the full set. There's no
        // need to search for a completed subset.
        assert(m_shared->writeTxns == m_shared->completeTxns);
        return m_shared->completeTxns;
    }

    unordered_map<Lsx, unordered_set<Lsx>> blocks;
    auto roots = firstRoots();
    for (auto && root : roots) {
        assert(root);
        unordered_set<Lsx> found;
        auto ptr = root->get()->next.get();
        for (; ptr; ptr = ptr->next.get()) {
            auto id = ptr->lsx;
            for (auto&& f : found)
                blocks[f].insert(id);
            found.insert(id);
        }
    }

    unordered_set<Lsx> ready;
    unordered_set<Lsx> blocked;
    vector<Lsx> check;
    check.reserve(m_shared->writeTxns.size());
    for (auto&& t : m_shared->writeTxns) {
        if (!m_shared->completeTxns.contains(t)) {
            blocked.insert(t);
            check.push_back(t);
        }
    }
    for (auto i = 0; i < check.size(); ++i) {
        auto&& t = check[i];
        for (auto&& dep : blocks[t]) {
            if (blocked.insert(dep).second) {
                check.push_back(dep);
                if (check.size() == check.capacity())
                    return ready;
            }
        }
    }
    for (auto&& t : m_shared->completeTxns) {
        if (!blocked.contains(t))
            ready.insert(t);
    }
    assert(ready.contains(txnId));
    return ready;
}

//===========================================================================
shared_ptr<DbRootSet> DbRootSet::commitNextSet(
    const unordered_set<Lsx> & txns
) {
    unique_lock lk(m_shared->mut);
    assert(!txns.empty());
    assert(m_shared->commitInProgress);
    m_next = make_shared<DbRootSet>(*this);
    for (auto&& id : txns) {
        if (!m_shared->completeTxns.contains(id))
            assert(!"Committing already completed transaction.");
        m_shared->writeTxns.erase(id);
        m_shared->completeTxns.erase(id);
    }

    auto roots = firstRoots();
    auto nexts = m_next->firstRoots();
    assert(roots.size() == nexts.size());
    auto nroot = nexts.begin();
    for (auto i = roots.begin(); i != roots.end(); ++i, ++nroot) {
        auto n = **i;
        assert(**nroot == n);

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

        vector<Lsx> rtxns;
        for (auto i = n; i; i = i->next) {
            rtxns.push_back(i->lsx);
        }
        if (rtxns.size() > 4 && m_shared->completeTxns.size() < 4)
            rtxns.shrink_to_fit();

        n = n->next;
        while (n && txns.contains(n->lsx)) {
            **nroot = n;
            n = n->next;
        }
    #ifndef NDEBUG
        while (n) {
            if (txns.contains(n->lsx)) {
                assert(!"Unpublished root update");
            }
            n = n->next;
        }
    #endif
    }

    m_shared->data.m_metricRoots.store(m_next);
    m_next->unlock_UNLK(move(lk));
    return m_next;
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
        { ":metric",     kTrie },
        { ":sample",     kRadix, {}, &m_sampleRoot },
        { ":metricName", kTrie },
    };
    m_rootDefs.assign_range(defs);
    m_metricRoots = make_shared<DbRootSet>(this);
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

    // Metric root set - modifies in place the root set being used by the
    // active txn. Here, during initialization, we assume no other transactions
    // will get confused as no other transactions should exist.
    auto roots = m_metricRoots.load();
    pair<const char *, shared_ptr<DbRootVersion>*> indexes[] = {
        { ":metric", &roots->info },
        { ":metricName", &roots->idByName },
    };
    for (auto&& index : indexes) {
        auto id = m_rootIdByName[index.first];
        assert(id);
        auto rver = make_shared<DbRootVersion>(&txn, this, id);
        rver->root = loadRoot(txn, id);
        *index.second = rver;
    }

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
    DbPageHeap heap(&txn, this, nameStoreRoot, kRootNameRootId);
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
            logMsgError() << "Duplicate stored root Id name: '" << key << "'"
                << " (" << m_rootIdByName[key] << " and " << id << ")";
            return false;
        }
        m_rootIdByName[key] = id;
    }
    assert(heap.destroyed().empty());
    m_rootNameById.resize(lastId + 1);
    for (auto&& [key, id] : m_rootIdByName) {
        if (!m_rootNameById[id].empty()) {
            logMsgError() << "Duplicate stored root Id: " << id
                << " ('" << m_rootNameById[id] << "' and '" << key << "')";
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
    DbPageHeap heap(&txn, this, nameStoreRoot, kRootNameRootId);
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
    freeDeprecatedPages(txn, heap.destroyed());

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
pgno_t DbData::loadRoot_LK(
    unique_lock<recursive_mutex> & lk,
    DbTxn & txn,
    unsigned rootId
) {
    pgno_t out = pgno_t::npos;
    if (!radixFind(txn, &out, m_rootRoot, rootId))
        out = pgno_t::npos;
    return out;
}

//===========================================================================
pgno_t DbData::loadRoot(DbTxn & txn, unsigned rootId) {
    unique_lock lk{m_pageMut};
    DbTxn::PinScope pins(txn);
    return loadRoot_LK(lk, txn, rootId);
}

//===========================================================================
pgno_t DbData::loadRoot(DbTxn & txn, const string & rootName) {
    unique_lock lk{m_pageMut};
    DbTxn::PinScope pins(txn);

    pgno_t out = pgno_t::npos;
    auto i = m_rootIdByName.find(rootName);
    if (i != m_rootIdByName.end())
        out = loadRoot_LK(lk, txn, i->second);
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
    num = 0;
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
        if (num++ % 1000 == 0 && appStopping())
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
    scoped_lock lk{m_pageMut};
    DbTxn::PinScope pins(txn);

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
    auto num = 0;
    while (m_deprecatedPages) {
        if (num++ % 1000 == 0 && appStopping())
            return false;
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
        // a page of the free list, the page will be freed... which means it
        // must be added to this bitmap.
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
        return;
    default:
        logMsgFatal() << "freePage(" << (unsigned) pgno
            << "): invalid page type (" << (unsigned) type << ")";
        return;
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
    if (freePages) {
        scoped_lock lk(m_pageMut);
        assert(!freePages.intersects(m_freePages));
        m_freePages.insert(freePages);
        auto num = freePages.count();
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
    [[maybe_unused]] bool updated = false;
    updated = bitAssign(txn, m_deprecatedRoot, 0, pgno, pgno + 1, true);
    assert(updated);
    updated = m_deprecatedPages.insert(pgno);
    assert(updated);
    s_perfDepPages += 1;
}

//===========================================================================
void DbData::freeDeprecatedPages(DbTxn & txn, UnsignedSet pgnos) {
    scoped_lock lk{m_pageMut};
    DbTxn::PinScope pins(txn);

    [[maybe_unused]] bool updated = false;
    for (auto&& r : pgnos.ranges()) {
        updated = bitAssign(
            txn,
            m_deprecatedRoot,
            0,
            r.first,
            r.second + 1,
            false
        );
        assert(updated);
        for (auto pgno = r.first; pgno <= r.second; ++pgno)
            freePage(txn, (pgno_t) pgno);
    }

    assert(m_deprecatedPages.contains(pgnos));
    m_deprecatedPages.erase(pgnos);
    s_perfDepPages -= (unsigned) pgnos.count();
}


/****************************************************************************
*
*   Trie indexes
*
***/

//===========================================================================
// static
string DbData::trieKeyMin(uint32_t id) {
    string out;
    switch (countl_zero(id) / 8) {
    case 0:
        out += (unsigned char) (id >> 24);
        [[fallthrough]];
    case 1:
        out += (unsigned char) ((id >> 16) % 256);
        [[fallthrough]];
    case 2:
        out += (unsigned char) ((id >> 8) % 256);
        [[fallthrough]];
    case 3:
        out += (unsigned char) (id % 256);
    }
    return out;
}

//===========================================================================
// static
string DbData::trieKey(uint32_t id) {
    string key(sizeof(id), 0);
    hton32(key.data(), id);
    return key;
}

//===========================================================================
// static
string DbData::trieKey(string_view name, uint32_t id) {
    return string(name) + '\0' + trieKeyMin(id);
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
bool DbData::triePerformAction(
    DbPageHeap & heap,
    DbData::TrieAction::Type type,
    const string & key
) {
    StrTrieBase trie(&heap);
    switch (type) {
    case TrieAction::kClear:
        trie.clear();
        return true;
    case TrieAction::kInsert:
        return trie.insert(key);
    case TrieAction::kErase:
        return trie.erase(key);
    default:
        assert(!"Unknown trie action");
        return false;
    }
}

//===========================================================================
void DbData::trieApply(
    DbTxn & txn,
    const vector<DbData::TrieAction> & actions
) {
    vector<size_t> ords(actions.size());
    vector<shared_ptr<DbRootVersion>> roots(actions.size());
    for (size_t i = 0; i < ords.size(); ++i) {
        ords[i] = i;
        roots[i] = actions[i].root;
    }
    while (!ords.empty()) {
        DbTxn::PinScope pins(txn);
        auto [root, pos] = txn.roots().beginUpdate(txn.getLsxAlways(), roots);
        assert(root->next);
        assert(!root->next->complete());
        auto & action = actions[ords[pos]];
        if (pos != ords.size() - 1) {
            ords[pos] = ords.back();
            roots[pos] = roots.back();
        }
        ords.pop_back();
        roots.pop_back();

        DbPageHeap heap(&txn, this, root->root, root->rootId);
        if (!triePerformAction(heap, action.type, action.key)) {
            txn.roots().rollbackUpdate(root);
        } else {
            root->deprecatedPages.insert(heap.destroyed());
            txn.roots().commitUpdate(root, (pgno_t) heap.root());
        }
    }
}

//===========================================================================
void DbData::trieClear(DbTxn & txn, pgno_t root) {
    assert(root);
    DbPageHeap heap(&txn, this, root);
    StrTrieBase trie(&heap);
    trie.clear();
    for (auto&& pgno : heap.destroyed())
        freePage(txn, (pgno_t) pgno);
}

//===========================================================================
bool DbData::trieVisitWithPrefix(
    DbTxn & txn,
    pgno_t root,
    string_view match,
    const function<bool(DbTxn&, const string & key)> & fn
) {
    if (!root)
        return true;
    DbPageHeap heap(&txn, this, root);
    StrTrieBase trie(&heap);
    for (auto i = trie.lowerBound(match); i != trie.end(); ++i) {
        if ((*i).compare(0, match.size(), match) != 0)
            return true;
        if (!fn(txn, *i))
            return false;
    }
    return true;
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

    // Record for the free page to be published when transaction is committed.
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
