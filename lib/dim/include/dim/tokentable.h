// tokentable.h - dim core
#pragma once

#include "dim/config.h"

#include <cassert>

namespace Dim {

class TokenTable {
public:
    struct Token {
        int id;
        const char * name;
    };

    class Iterator {
        const Token * m_current{nullptr};
    public:
        Iterator (const Token * ptr) : m_current{ptr} {}
        Iterator & operator++ ();
        const Token & operator* () {
            assert(m_current);
            return *m_current;
        }
    };

public:
    TokenTable (const Token * ptr, size_t count);

    bool find (int * out, const char name[]) const;
    bool find (const char * const * out, int id) const;

    Iterator begin () const;
    Iterator end () const;

private:
    struct Value {
        const char * name{nullptr};
        size_t hash{0};
        int id{0};
    };
    std::vector<Value> m_names;
    int m_hashLen;
};


template <typename E>
E tokenTableGetEnum (const TokenTable & tbl, const char name[], E defId) {
    int id;
    return tbl.find(&id, name)
        ? (E) id
        : defId;
}

template <typename E>
const char * tokenTableGetName (
    const TokenTable & tbl, 
    E id,
    const char defName[] = nullptr
) {
    const char * name;
    return tbl.find(&name, (int) id)
        ? name
        : defName;
}

} // namespace
