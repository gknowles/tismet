// xml.h - dim services
#pragma once

#include "dim/config.h"

#include <sstream>
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
    template <bool isContent>
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

template <typename T>
inline IXBuilder & operator<< (IXBuilder & out, const T & val) {
    std::ostringstream os;
    os << val;
    return out.text(os.str());
}

inline IXBuilder & operator<< (
    IXBuilder & out, 
    IXBuilder & (*pfn)(IXBuilder &)
) {
    return pfn(out);
}

inline IXBuilder & elem (IXBuilder & out) { return out.elem(nullptr); }
inline IXBuilder & attr (IXBuilder & out) { return out.attr(nullptr); }
inline IXBuilder & end (IXBuilder & out) { return out.end(); }


class XBuilder : public IXBuilder {
public:
    XBuilder (CharBuf & buf) : m_buf(buf) {}

private:
    void append (const char text[], size_t count = -1) override;
    void appendCopy (size_t pos, size_t count) override;
    size_t size () override;

    CharBuf & m_buf;
};


/****************************************************************************
*
*   Xml stream parser
*
***/

class XStreamParser;

class IXStreamParserNotify {
public:
    virtual ~IXStreamParserNotify () {}
    virtual bool StartDoc (XStreamParser & parser) = 0;
    virtual bool StartElem (
        XStreamParser & parser, 
        const char name[],
        size_t nameLen
    ) = 0;
    virtual bool Attr (
        XStreamParser & parser, 
        const char name[], 
        size_t nameLen,
        const char value[],
        size_t valueLen
    ) = 0;
    virtual bool Text (
        XStreamParser & parser, 
        const char value[], 
        size_t valueLen
    ) = 0;
    virtual bool EndElem (XStreamParser & parser) = 0;
    virtual bool EndDoc (XStreamParser & parser) = 0;
};

class XStreamParser {
public:
    void clear ();
    bool parse (IXStreamParserNotify & notify, char src[], size_t srcLen);

    ITempHeap & heap ();
    bool fail (const char errmsg[]);

private:
    bool parseNode (IXStreamParserNotify & notify, unsigned char *& ptr);
    bool parseElement (
        IXStreamParserNotify & notify, 
        unsigned char *& ptr,
        unsigned char * nextChar
    );
    bool parseElement (
        IXStreamParserNotify & notify, 
        unsigned char *& ptr,
        unsigned char * nextChar,
        unsigned firstChar
    );
    bool parsePI (unsigned char *& ptr);
    bool parseBangNode (IXStreamParserNotify & notify, unsigned char *& ptr);
    bool parseComment (unsigned char *& ptr);
    bool parseCData (IXStreamParserNotify & notify, unsigned char *& ptr);
    bool parseAttrs (IXStreamParserNotify & notify, unsigned char *& ptr);
    bool parseContent (IXStreamParserNotify & notify, unsigned char *& ptr);

    TempHeap m_heap;
    unsigned m_line{0};
    bool m_failed{false};
};


/****************************************************************************
*
*   Xml dom parser
*
***/

struct XElem;

class XParser : IXStreamParserNotify {
public:
    void clear ();
    XElem * parse (char src[]);
    XElem * setRoot (const char elemName[], const char text[] = nullptr);

    ITempHeap & heap ();

private:
    // IXStreamParserNotify
    bool StartDoc (XStreamParser & parser) override;
    bool StartElem (
        XStreamParser & parser, 
        const char name[],
        size_t nameLen
    ) override;
    bool Attr (
        XStreamParser & parser, 
        const char name[], 
        size_t nameLen,
        const char value[],
        size_t valueLen
    ) override;
    bool Text (
        XStreamParser & parser, 
        const char value[], 
        size_t valueLen
    ) override;
    bool EndElem (XStreamParser & parser) override;
    bool EndDoc (XStreamParser & parser) override;

    XStreamParser m_parser;
    XElem * m_root{nullptr};
};

//===========================================================================
// XAttr
//===========================================================================
struct XAttr {
    const char * m_name{nullptr};
    const char * m_value{nullptr};
    XAttr * m_next{nullptr};
    XAttr * m_prev{nullptr};
};

//===========================================================================
// XElem
//===========================================================================
struct XElem {
    template <typename T> class Iterator;

    const char * m_name{nullptr};
    const char * m_value{nullptr};
    XElem * m_next{nullptr};
    XElem * m_prev{nullptr};
};

XElem * prevSibling (XElem * elem, const char name[]);
XElem * nextSibling (XElem * elem, const char name[]);
const XElem * prevSibling (const XElem * elem, const char name[]);
const XElem * nextSibling (const XElem * elem, const char name[]);

template <typename T> 
class XElem::Iterator : public ForwardListIterator<T> {
    const char * m_name{nullptr};
public:
    Iterator (T * node, const char name[]);
    Iterator operator++ ();
};

template <typename T>
struct XElemRange {
    XElem::Iterator<T> begin ();
    XElem::Iterator<T> end ();
};
XElemRange<XElem> elems (XElem * elem, const char name[] = nullptr);
XElemRange<const XElem> elems (const XElem * elem, const char name[] = nullptr);

template <typename T>
struct XAttrRange {
    ForwardListIterator<T> begin ();
    ForwardListIterator<T> end ();
};
XAttrRange<XAttr> attrs (XElem * elem);
XAttrRange<const XAttr> attrs (const XElem * elem);


//===========================================================================
template <typename T>
XElem::Iterator<T>::Iterator (T * node, const char name[]) 
    : ForwardListIterator(node)
    , m_name(name)
{}

//===========================================================================
template <typename T>
auto XElem::Iterator<T>::operator++ () -> Iterator {
    m_current = nextSibling(m_current, m_name);
    return *this;
}

} // namespace
