// xmlbuilder.cpp - dim services
#include "pch.h"
#pragma hdrstop

using namespace std;

namespace Dim {


/****************************************************************************
*
*   Private
*
***/

namespace {

enum TextType : char {
    kTextTypeInvalid = 0,
    kTextTypeNormal = 1,
    kTextTypeNull = 2,
    kTextTypeQuote = 3,
    kTextTypeAmp = 4,
    kTextTypeLess = 5,
    kTextTypeGreater = 6,
    kTextTypes,
};

const char * kTextEntityTable[] = {
    nullptr,
    nullptr,
    nullptr,
    "&quote;",
    "&amp;",
    "&lt;",
    "&gt;",
};
static_assert(size(kTextEntityTable) == kTextTypes, "");

const char kTextTypeTable[256] = {
//  0  1  2  3  4  5  6  7  8  9  a  b  c  d  e  f
    2, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 0, 0, // 0
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 1
    1, 3, 1, 1, 1, 4, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 2
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 5, 1, 6, 1, // 3
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 4
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 5
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 6
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 7
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 8
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 9
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // a
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // b
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // c
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // d
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // e
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // f
};

} // namespace


/****************************************************************************
*
*   IXBuilder
*
***/

enum IXBuilder::State {
    kStateFail,
    kStateDocIntro,         // 
    kStateElemNameIntro,    // <
    kStateElemName,         // <X_
    kStateAttrNameIntro,    // <X _
    kStateAttrName,         // <X a_
    kStateAttrText,         // <X a="_
    kStateAttrEnd,          // <X a="1"_
    kStateText,             // >_
};

//===========================================================================
IXBuilder & IXBuilder::text (const char val[]) {
    switch (m_state) {
    case kStateElemNameIntro:
        m_state = kStateElemName;
        [[fallthrough]];
    case kStateElemName:
        append(val);
        return *this;
    case kStateAttrNameIntro:
        m_state = kStateAttrName;
        [[fallthrough]];
    case kStateAttrName:
        append(val);
        return *this;
    case kStateAttrText:
        addText<false>(val);
        return *this;
    case kStateAttrEnd:
        append(">");
        m_state = kStateText;
    }
    assert(m_state == kStateText);
    addText<true>(val);
    return *this;
}

//===========================================================================
IXBuilder & IXBuilder::elem (const char name[], const char val[]) {
    switch (m_state) {
    case kStateAttrText:
        append("\">\n<");
        break;
    default:
        assert(m_state == kStateDocIntro || m_state == kStateText);
        append("<");
        break;
    }
    m_state = kStateElemNameIntro;
    m_stack.push_back({size(), 0});
    if (!name) {
        assert(val == nullptr);
    } else {
        text(name);
        if (val) {
            end();
            text(val);
            end();
        }
    }
    return *this;
}

//===========================================================================
IXBuilder & IXBuilder::attr (const char name[], const char val[]) {
    switch (m_state) {
    case kStateElemName:
        append(" ");
        break;
    default:
        assert(m_state == kStateAttrEnd);
        append("\" ");
        break;
    }
    m_state = kStateAttrNameIntro;
    if (!name) {
        assert(val == nullptr);
    } else {
        text(name);
        if (val) {
            end();
            text(val);
            end();
        }
    }
    return *this;
}

//===========================================================================
IXBuilder & IXBuilder::end () {
    switch (m_state) {
    case kStateElemName:
        append("/>\n");
        m_stack.pop_back();
        m_state = kStateText;
        return *this;
    case kStateAttrName: // end attribute name
        append("=\"");
        m_state = kStateAttrText;
        return *this;
    case kStateAttrText: // end attribute
        append("\"");
        m_state = kStateAttrEnd;
        return *this;
    }
    assert(m_state == kStateText);
    append("</");
    auto & top = m_stack.back();
    appendCopy(top.pos, top.len);
    m_stack.pop_back();
    append(">\n");
    return *this;
}

//===========================================================================
template <bool isContent>
void IXBuilder::addText (const char val[]) {
    const char * base = val;
    for (;;) {
        TextType type = (TextType) kTextTypeTable[*val];
        switch (type) {
        case kTextTypeGreater:
            // ">" must be escaped when following "]]"
            if (isContent) {
                if (val - base >= 2 
                    && val[-1] == ']' 
                    && val[-2] == ']'
                ) {
                    break;
                }
            }
            [[fallthrough]];
        case kTextTypeNormal:
            val += 1;
            continue;
        case kTextTypeNull:
            append(base, val - base);
            return;
        case kTextTypeQuote:
            if (isContent) {
                val += 1;
                continue;
            }
        case kTextTypeAmp:
        case kTextTypeLess:
            break;
        case kTextTypeInvalid:
            m_state = kStateFail;
            return;
        }

        append(base, val - base);
        append(kTextEntityTable[type]);
        base = ++val;
    }
}


/****************************************************************************
*
*   XBuilder
*
***/

//===========================================================================
void XBuilder::append (const char text[], size_t count) {
    m_buf.append(text, count);
}

//===========================================================================
void XBuilder::appendCopy (size_t pos, size_t count) {
    m_buf.append(m_buf, pos, count);
}

//===========================================================================
size_t XBuilder::size () {
    return m_buf.size();
}


/****************************************************************************
*
*   Public API
*
***/

//===========================================================================
IXBuilder & operator<< (IXBuilder & out, int64_t val) {
    IntegralStr<int64_t> tmp(val);
    return out.text(tmp);
}

//===========================================================================
IXBuilder & operator<< (IXBuilder & out, uint64_t val) {
    IntegralStr<int64_t> tmp(val);
    return out.text(tmp);
}

//===========================================================================
IXBuilder & operator<< (IXBuilder & out, int val) {
    IntegralStr<int> tmp(val);
    return out.text(tmp);
}

//===========================================================================
IXBuilder & operator<< (IXBuilder & out, unsigned val) {
    IntegralStr<unsigned> tmp(val);
    return out.text(tmp);
}

//===========================================================================
IXBuilder & operator<< (IXBuilder & out, char val) {
    char str[] = { val, 0 };
    return out.text(str);
}

//===========================================================================
IXBuilder & operator<< (IXBuilder & out, const char val[]) {
    return out.text(val);
}

//===========================================================================
IXBuilder & operator<< (IXBuilder & out, const std::string & val) {
    return out.text(val.c_str());
}

} // namespace
