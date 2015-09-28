// charbuf.cpp - dim services
#include "pch.h"
#pragma hdrstop

using namespace std;


/****************************************************************************
*
*   Tuning parameters
*
***/

const unsigned DEFAULT_BLOCK_SIZE = 4096;


/****************************************************************************
*
*   CharBuf::Buffer
*
***/

struct CharBuf::Buffer {
    int m_used{0};
    int m_reserved{DEFAULT_BLOCK_SIZE};
    bool m_heapUsed{false};
    char m_data[DEFAULT_BLOCK_SIZE];

    char * Base () { return m_data; }
    char * Unused () { return Base() + m_used; }
    char * End () { return Base() + m_reserved; }
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
CharBuf & CharBuf::Assign (const char s[]) {
    return Replace(0, m_size, s);
}

//===========================================================================
CharBuf & CharBuf::Assign (const char s[], size_t count) {
    return Replace(0, m_size, s, count);
}

//===========================================================================
CharBuf & CharBuf::Assign (const string & str, size_t pos, size_t count) {
    assert(pos < str.size());
    return Replace(
        0, 
        m_size, 
        str.data() + pos, 
        min(str.size(), pos + count) - pos
    );
}

//===========================================================================
char & CharBuf::Front () {
    assert(m_size);
    auto & buf = m_buffers.front();
    return *buf.m_data;
}

//===========================================================================
const char & CharBuf::Front () const {
    assert(m_size);
    auto & buf = m_buffers.front();
    return *buf.m_data;
}

//===========================================================================
char & CharBuf::Back () {
    assert(m_size);
    auto & buf = m_buffers.back();
    return buf.m_data[buf.m_used - 1];
}

//===========================================================================
const char & CharBuf::Back () const {
    assert(m_size);
    auto & buf = m_buffers.back();
    return buf.m_data[buf.m_used - 1];
}

//===========================================================================
bool CharBuf::Empty () const {
    return !m_size;
}

//===========================================================================
int CharBuf::Size () const {
    return m_size;
}

//===========================================================================
void CharBuf::Clear () {
    if (m_size)
        Erase(m_buffers.begin(), 0, m_size);
}

//===========================================================================
CharBuf & CharBuf::Insert (size_t pos, const char s[]) {
    return Replace(pos, 0, s);
}

//===========================================================================
CharBuf & CharBuf::Insert (size_t pos, const char s[], size_t count) {
    return Replace(pos, 0, s, count);
}

//===========================================================================
CharBuf & CharBuf::Erase (size_t pos, size_t count) {
    auto ic = Find(pos);
    return Erase(ic.first, ic.second, (int) count);
}

//===========================================================================
void CharBuf::PushBack (char ch) {
    Append(1, ch);
}

//===========================================================================
void CharBuf::PopBack () {
    assert(m_size);
    m_size -= 1;
    auto it = m_buffers.end();
    --it;
    if (--it->m_used)
        return;
    
    m_buffers.erase(it);
}

//===========================================================================
CharBuf & CharBuf::Append (size_t count, char ch) {
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
CharBuf & CharBuf::Append (const char s[]) {
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
CharBuf & CharBuf::Append (const char src[], size_t srcLen) {
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
CharBuf & CharBuf::Append (const string & str, size_t pos, size_t count) {
    assert(pos < str.size());
    return Append(str.data() + pos, min(str.size(), pos + count) - pos);
}

//===========================================================================
CharBuf & CharBuf::Replace (size_t pos, size_t count, const char s[]) {
    assert(pos + count <= m_size);

    if (pos == m_size)
        return Append(s);

    int remove = (int) count;
    auto ic = Find(pos);
    auto eb = m_buffers.end();

    list<Buffer>::iterator next;
    char * base = ic.first->m_data + ic.second;
    char * ptr = base;
    char * eptr = ptr + min(ic.first->m_used - ic.second, remove);
    for (;;) {
        if (!*s) {
            return Erase(
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
CharBuf & CharBuf::Replace (
    size_t pos, 
    size_t count, 
    const char src[], 
    size_t srcLen
) {
    assert(pos + count <= m_size);
    if (pos == m_size)
        return Append(src, srcLen);

    int remove = (int) count;
    int copy = (int) srcLen;
    auto ic = Find(pos);
    auto eb = m_buffers.end();
    m_size += copy - remove;

    char * ptr = ic.first->m_data + ic.second;
    int reserved = ic.first->m_reserved - ic.second;
    int used = ic.first->m_used - ic.second;
    int replaced;
    for (;;) {
        if (!copy) {
            return Erase(
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
void CharBuf::Swap (CharBuf & other) {
    swap(m_buffers, other.m_buffers);
    swap(m_lastUsed, other.m_lastUsed);
    swap(m_size, other.m_size);
}

//===========================================================================
// private
//===========================================================================
CharBuf::Buffer * CharBuf::AllocBuffer () {
    m_buffers.emplace_back();
    Buffer & buf = m_buffers.back();
    return &buf;
};

//===========================================================================
pair<list<CharBuf::Buffer>::iterator, int> CharBuf::Find (size_t pos) {
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
CharBuf & CharBuf::Erase (
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
