// xml.h - dim services
#pragma once

#include "dim/config.h"

#include <string>
#include <vector>

namespace Dim {


/****************************************************************************
*
*   Xml builder
*
***/

class IXBuilder {
public:
    virtual ~IXBuilder () {}

    IXBuilder & elem (const char name[], const char text[] = nullptr);
    IXBuilder & attr (const char name[], const char text[] = nullptr);
    IXBuilder & end ();

    IXBuilder & text (const char text[]);

protected:
    virtual void append (const char text[], size_t count = -1) = 0;
    virtual void appendCopy (size_t pos, size_t count) = 0;
    virtual size_t size () = 0;

private:
    template <bool escapeQuote>
    void addText (const char text[]);

    enum State;
    State m_state;
    struct Pos {
        size_t pos;
        size_t len;
    };
    std::vector<Pos> m_stack;
};

IXBuilder & operator<< (IXBuilder & out, int64_t val);
IXBuilder & operator<< (IXBuilder & out, uint64_t val);
IXBuilder & operator<< (IXBuilder & out, int val);
IXBuilder & operator<< (IXBuilder & out, unsigned val);
IXBuilder & operator<< (IXBuilder & out, char val);
IXBuilder & operator<< (IXBuilder & out, const char val[]);
IXBuilder & operator<< (IXBuilder & out, const std::string & val);


inline IXBuilder & operator<< (
    IXBuilder & out, 
    IXBuilder & (*pfn)(IXBuilder &)
) {
    return pfn(out);
}

inline IXBuilder & elem (IXBuilder & out) { return out.elem(nullptr); }
inline IXBuilder & attr (IXBuilder & out) { return out.attr(nullptr); }
inline IXBuilder & end (IXBuilder & out) { return out.end(); }


class CXBuilder : public IXBuilder {
public:
    CXBuilder (CharBuf & buf) : m_buf(buf) {}

private:
    void append (const char text[], size_t count = -1) override;
    void appendCopy (size_t pos, size_t count) override;
    size_t size () override;

    CharBuf & m_buf;
};


/****************************************************************************
*
*   Xml parser
*
***/

class CXParser {
};

} // namespace
