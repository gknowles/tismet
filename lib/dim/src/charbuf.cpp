// charbuf.cpp - dim services
#include "pch.h"
#pragma hdrstop

using namespace std;

namespace Dim {


/****************************************************************************
*
*   Tuning parameters
*
***/

const unsigned kDefaultBlockSize = 4096;


/****************************************************************************
*
*   CharBuf::Buffer
*
***/

struct CharBuf::Buffer {
    int m_used{0};
    int m_reserved{kDefaultBlockSize};
    bool m_heapUsed{false};
    char m_data[kDefaultBlockSize];

    char * base () { return m_data; }
    char * unused () { return base() + m_used; }
    char * end () { return base() + m_reserved; }
};


/****************************************************************************
*
*   CharBuf
*
***/

//===========================================================================
CharBuf::CharBuf () 
{}

//===========================================================================
CharBuf::~CharBuf() 
{}

//===========================================================================
CharBuf & CharBuf::assign (const char s[]) {
    return replace(0, m_size, s);
}

//===========================================================================
CharBuf & CharBuf::assign (const char s[], size_t count) {
    return replace(0, m_size, s, count);
}

//===========================================================================
CharBuf & CharBuf::assign (const string & str, size_t pos, size_t count) {
    assert(pos < str.size());
    return replace(
        0, 
        m_size, 
        str.data() + pos, 
        min(str.size(), pos + count) - pos
    );
}

//===========================================================================
char & CharBuf::front () {
    assert(m_size);
    auto & buf = m_buffers.front();
    return *buf.m_data;
}

//===========================================================================
const char & CharBuf::front () const {
    assert(m_size);
    auto & buf = m_buffers.front();
    return *buf.m_data;
}

//===========================================================================
char & CharBuf::back () {
    assert(m_size);
    auto & buf = m_buffers.back();
    return buf.m_data[buf.m_used - 1];
}

//===========================================================================
const char & CharBuf::back () const {
    assert(m_size);
    auto & buf = m_buffers.back();
    return buf.m_data[buf.m_used - 1];
}

//===========================================================================
bool CharBuf::empty () const {
    return !m_size;
}

//===========================================================================
int CharBuf::size () const {
    return m_size;
}

//===========================================================================
void CharBuf::clear () {
    if (m_size)
        erase(m_buffers.begin(), 0, m_size);
}

//===========================================================================
CharBuf & CharBuf::insert (size_t pos, const char s[]) {
    return replace(pos, 0, s);
}

//===========================================================================
CharBuf & CharBuf::insert (size_t pos, const char s[], size_t count) {
    return replace(pos, 0, s, count);
}

//===========================================================================
CharBuf & CharBuf::erase (size_t pos, size_t count) {
    auto ic = find(pos);
    return erase(ic.first, ic.second, (int) count);
}

//===========================================================================
void CharBuf::pushBack (char ch) {
    append(1, ch);
}

//===========================================================================
void CharBuf::popBack () {
    assert(m_size);
    m_size -= 1;
    auto it = m_buffers.end();
    --it;
    if (--it->m_used)
        return;
    
    m_buffers.erase(it);
}

//===========================================================================
CharBuf & CharBuf::append (size_t count, char ch) {
    assert(m_size + count < numeric_limits<int>::max());
    int add = (int) count;
    if (!add)
        return *this;

    if (!m_size)
        m_buffers.emplace_back();
    m_size += add;
    for (;;) {
        Buffer & buf = m_buffers.back();
        int added = min(add, buf.m_reserved - buf.m_used);
        if (added) {
            char * ptr = buf.m_data + buf.m_used;
            memset(ptr, ch, added);
            buf.m_used += added;
            add -= added;
            if (!add)
                return *this;
        }
        m_buffers.emplace_back();
    }        
}

//===========================================================================
CharBuf & CharBuf::append (const char s[]) {
    if (!*s)
        return *this;

    if (!m_size)
        m_buffers.emplace_back();

    for (;;) {
        Buffer & buf = m_buffers.back();
        char * ptr = buf.m_data + buf.m_used;
        char * eptr = buf.m_data + buf.m_reserved;
        for (;;) {
            if (ptr == eptr) {
                m_size += buf.m_reserved - buf.m_used;
                buf.m_used = buf.m_reserved;
                m_buffers.emplace_back();
                break;
            }
            if (!*s) {
                int added = int(ptr - buf.m_data) - buf.m_used;
                m_size += added;
                buf.m_used += added;
                return *this;
            }
            *ptr++ = *s++;
        }
    }
}

//===========================================================================
CharBuf & CharBuf::append (const char src[], size_t srcLen) {
    int add = (int) srcLen;
    if (!add)
        return *this;

    if (!m_size)
        m_buffers.emplace_back();
    m_size += add;

    for (;;) {
        Buffer & buf = m_buffers.back();
        int added = min(add, buf.m_reserved - buf.m_used);
        memcpy(buf.m_data + buf.m_used, src, added);
        buf.m_used += added;
        add -= added;
        if (!add)
            return *this;
        m_buffers.emplace_back();
    }
}

//===========================================================================
CharBuf & CharBuf::append (const string & str, size_t pos, size_t count) {
    assert(pos < str.size());
    return append(str.data() + pos, min(str.size(), pos + count) - pos);
}

//===========================================================================
CharBuf & CharBuf::append (const CharBuf & buf, size_t pos, size_t count) {
    assert(pos < buf.size());
    auto ic = find(pos);
    auto eb = m_buffers.end();
    int add = (int) count;
    int copied = min(ic.first->m_used - ic.second, add);
    append(ic.first->m_data + ic.second, copied);

    for (;;) {    
        add -= copied;
        if (!add || ++ic.first == eb)
            return *this;
        copied = min(ic.first->m_used, add);
        append(ic.first->m_data, copied);
    }
}

//===========================================================================
int CharBuf::compare (const char s[], size_t count) const {
    for (auto&& buf : m_buffers) {
        if (count < buf.m_used) {
            if (memcmp(buf.m_data, s, count) < 0)
                return -1;
            return 1;
        }
        if (int rc = memcmp(buf.m_data, s, buf.m_used))
            return rc;
        s += buf.m_used;
        count -= buf.m_used;
    }
    return count ? -1 : 0;
}

//===========================================================================
int CharBuf::compare (const string & str) const {
    return compare(data(str), ::size(str));
}

//===========================================================================
int CharBuf::compare (const CharBuf & buf) const {
    auto myi = m_buffers.begin();
    auto mye = m_buffers.end();
    const char * mydata;
    int mycount;
    auto ri = buf.m_buffers.begin();
    auto re = buf.m_buffers.end();
    const char * rdata;
    int rcount;
    goto compare_new_buffers;

    for (;;) {
        if (mycount < rcount) {
            int rc = memcmp(mydata, rdata, mycount);
            if (rc)
                return rc;
            if (++myi == mye)
                return -1;
            rdata += mycount;
            rcount -= mycount;
            mydata = myi->m_data;
            mycount = myi->m_used;
            continue;
        }

        int rc = memcmp(mydata, rdata, rcount);
        if (rc)
            return rc;
        if (mycount > rcount) {
            if (++ri == re)
                return 1;
            mydata += rcount;
            mycount -= rcount;
            rdata = ri->m_data;
            rcount = ri->m_used;
            continue;
        }
        ++myi;
        ++ri;

    compare_new_buffers:
        if (myi == mye) 
            return (ri == re) ? 0 : -1;
        if (ri == re)
            return 1;
        mydata = myi->m_data;
        mycount = myi->m_used;
        rdata = ri->m_data;
        rcount = ri->m_used;
    }
}

//===========================================================================
CharBuf & CharBuf::replace (size_t pos, size_t count, const char s[]) {
    assert(pos + count <= m_size);

    if (pos == m_size)
        return append(s);

    int remove = (int) count;
    auto ic = find(pos);
    auto eb = m_buffers.end();

    list<Buffer>::iterator next;
    char * base = ic.first->m_data + ic.second;
    char * ptr = base;
    char * eptr = ptr + min(ic.first->m_used - ic.second, remove);
    for (;;) {
        if (!*s) {
            return erase(
                ic.first, 
                int(ptr - ic.first->m_data), 
                remove - int(ptr - base)
            );
        }
        *ptr++ = *s++;
        if (ptr == eptr) {
            int copied = ic.first->m_reserved - ic.first->m_used;
            remove -= copied;
            ic.first->m_used += copied;
            if (++ic.first == eb) 
                break;
            ptr = ic.first->m_data;
            eptr = ptr + min(ic.first->m_used, remove);
        }
    }

    next = ic.first;
    ++next;

    if (remove) {
        assert(remove == ic.first->m_data + ic.first->m_used - ptr);
        auto it = m_buffers.emplace(next);
        char * tmp = ptr;
        char * dst = it->m_data;
        memcpy(dst, tmp, remove);
        it->m_used = remove;
    }

    base = ptr;
    eptr = ic.first->m_data + ic.first->m_reserved;
    for (;;) {
        if (!*s) {
            int added = int(ptr - base);
            ic.first->m_used += added;
            m_size += added;
            return *this;
        }
        *ptr++ = *s++;
        if (ptr == eptr) {
            int added = int(ptr - base);
            ic.first->m_used += added;
            m_size += added;
            ic.first = m_buffers.emplace(next);
            base = ptr = ic.first->m_data;
            eptr = ptr + ic.first->m_reserved;
        }
    }
}

//===========================================================================
CharBuf & CharBuf::replace (
    size_t pos, 
    size_t count, 
    const char src[], 
    size_t srcLen
) {
    assert(pos + count <= m_size);
    if (pos == m_size)
        return append(src, srcLen);

    int remove = (int) count;
    int copy = (int) srcLen;
    auto ic = find(pos);
    auto eb = m_buffers.end();
    m_size += copy - remove;

    char * ptr = ic.first->m_data + ic.second;
    int reserved = ic.first->m_reserved - ic.second;
    int used = ic.first->m_used - ic.second;
    int replaced;
    for (;;) {
        if (!copy) {
            return erase(
                ic.first, 
                int(ptr - ic.first->m_data), 
                remove
            );
        }

        replaced = min({reserved, remove, copy});
        memcpy(ptr, src, replaced);
        src += replaced;
        copy -= replaced;
        remove -= min(used, replaced);
        if (!remove) 
            break;
        ic.second = 0;
        ++ic.first;
        assert(ic.first != eb);
        ptr = ic.first->m_data;
        reserved = ic.first->m_reserved;
        used = ic.first->m_used;
    }

    if (copy + used - replaced <= reserved) {
        ptr += replaced;
        if (used > replaced)
            memmove(ptr + copy, ptr, used - replaced);
        memcpy(ptr, src, copy);
        ic.first->m_used += copy;
        return *this;
    }

    auto next = ic.first;
    ++next;

    // shift following to new block
    if (used > replaced) {
        auto it = m_buffers.emplace(next);
        memcpy(it->m_data, ptr, used - replaced);
        it->m_used = used - replaced;
    }
    // copy remaining source into new blocks
    for (;;) {
        memcpy(ptr, src, replaced);
        ic.first->m_used += replaced;
        src += replaced;
        copy -= replaced;
        if (!copy)
            return *this;
        ic.first = m_buffers.emplace(next);
        ptr = ic.first->m_data;
        replaced = min(ic.first->m_reserved, copy);
    }
}

//===========================================================================
void CharBuf::swap (CharBuf & other) {
    ::swap(m_buffers, other.m_buffers);
    ::swap(m_lastUsed, other.m_lastUsed);
    ::swap(m_size, other.m_size);
}

//===========================================================================
// ITempHeap
char * CharBuf::alloc (size_t bytes, size_t align) {
    return nullptr;
}

//===========================================================================
// private
//===========================================================================
CharBuf::Buffer * CharBuf::allocBuffer () {
    m_buffers.emplace_back();
    Buffer & buf = m_buffers.back();
    return &buf;
};

//===========================================================================
pair<list<CharBuf::Buffer>::iterator, int> CharBuf::find (size_t pos) {
    int off = (int) pos;
    if (off <= m_size / 2) {
        auto it = m_buffers.begin();
        for (;;) {
            int used = it->m_used;
            if (off <= used) {
                if (off < used || used < it->m_reserved)
                    return make_pair(it, off);
            }
            off -= used;
            ++it;
        }
    } else {
        assert(pos <= m_size);
        int base = m_size;
        auto it = m_buffers.end();
        if (m_buffers.empty()) {
            assert(pos == 0);
            return make_pair(it, off);
        }
        for (;;) {
            --it;
            base -= it->m_used;
            if (base <= off)
                return make_pair(it, off - base);
        }
    }
}

//===========================================================================
CharBuf & CharBuf::erase (
    list<CharBuf::Buffer>::iterator it,
    int pos,
    int remove
) {
    assert(pos <= it->m_used && pos >= 0);
    assert(m_size > remove && remove >= 0);
    m_size -= remove;
    if (pos) {
        int copied = it->m_used - pos - remove;
        if (copied > 0) {
            char * ptr = it->m_data + pos;
            char * eptr = ptr + remove;
            memmove(ptr, eptr, copied);
            it->m_used -= remove;
            return *this;
        }
        int removed = it->m_used - pos;
        it->m_used -= removed;
        remove -= removed;
        ++it;
    }

    if (!remove)
        return *this;

    auto next = it;
    auto eb = m_buffers.end();
    while (it->m_used <= remove) {
        ++next;
        remove -= it->m_used;
        m_buffers.erase(it);
        it = next;
        if (it == eb) {
            assert(!remove);
            return *this;
        }
    }

    it->m_used -= remove;
    char * ptr = it->m_data;
    char * eptr = ptr + remove;
    memmove(ptr, eptr, it->m_used);
    return *this;
}

} // namespace
