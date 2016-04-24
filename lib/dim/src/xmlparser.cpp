// xmlparser.cpp - dim services
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

struct XElemInfo : XElem {
    XElem * m_firstElem{nullptr};
    XAttr * m_firstAttr{nullptr};
};

} // namespace


/****************************************************************************
*
*   XParser
*
***/

enum CharType : uint8_t {
    kInvalid = 0,
    kNull = 1,
    kUtf8Lead2 = 2,
    kUtf8Lead3 = 3,
    kUtf8BomIntro = 4,  // \xef
    kUtf8Lead4 = 5,
    kUtf8Char = 6,
    kWhitespace = 7,    // \t \r ' '
    kLinefeed = 8,      // \n
    kChar = 9,
    kBang = 10,
    kDQuote = 11,
    kCross = 12,
    kAmp = 13,
    kSQuote = 14,
    kSlash = 15,
    kNum = 16,
    kColon = 17,
    kLess = 18,
    kEqual = 19,
    kGreater = 20,
    kQuestion = 21,
    kAlpha = 22,
    kLBracket = 23,
    kRBracket = 24,
    kHexAlpha = 25,
    kSemi = 26,
    kUnder = 27,
    kDash = 28,
    kReturn = 29,
    kDot = 30,
};

static uint8_t s_charMap[] = {
//  0   1   2   3   4   5   6   7   8   9   a   b   c   d   e   f
    1,  0,  0,  0,  0,  0,  0,  0,  0,  7,  8,  0,  0, 29,  0,  0, // 0
    0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0, // 1
    7, 10, 11, 12,  9,  9, 13, 14,  9,  9,  9,  9,  9, 28, 30, 15, // 2
   16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 17, 26, 18, 19, 20, 21, // 3
    9, 25, 25, 25, 25, 25, 25, 22, 22, 22, 22, 22, 22, 22, 22, 22, // 4
   22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 23,  9, 24,  9, 27, // 5
    9, 25, 25, 25, 25, 25, 25, 22, 22, 22, 22, 22, 22, 22, 22, 22, // 6
   22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22,  9,  9,  9,  9,  9, // 7
    6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6, // 8
    6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6, // 9
    6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6, // a
    6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6, // b
    0,  0,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2, // c
    2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2, // d
    3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  4, // e
    5,  5,  5,  5,  5,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0, // f
};

//===========================================================================
bool XStreamParser::parse (
    IXStreamParserNotify & notify, 
    char src[], 
    size_t len
) {
    auto ptr = (unsigned char *) src;
    m_heap.clear();
    m_line = 0;
    m_failed = false;

    for (;;) {
        switch (s_charMap[*ptr]) {
        case kNull:
            return true;
        case kLess:
            ptr += 1;
            if (!parseNode(notify, ptr)) 
                return false;
            break;
        case kLinefeed:
            m_line += 1;
            [[fallthrough]];
        case kWhitespace:
        case kReturn:
            ptr += 1;
            break;
        case kUtf8BomIntro:
            // skip utf-8 bom if present
            if (ptr[1] == 0xbb && ptr[2] == 0xbf && (char *) ptr == src) {
                ptr += 3;
                break;
            }
        default:
            return fail("expected <");
        }
    }
}

//===========================================================================
static bool twoByte (unsigned char *& ptr, unsigned * utf8) {
    if ((ptr[1] & 0xc0) == 0x80) {
        *utf8 = ((*ptr & 0x1f) << 6) + (ptr[1] & 0x3f);
        ptr += 2;
        if (*utf8 >= 0x80)
            return true;
    }
    *utf8 = 0;
    return false;
}

//===========================================================================
static bool threeByte (unsigned char *& ptr, unsigned * utf8) {
    if ((ptr[1] & 0xc0) == 0x80
        && (ptr[2] & 0xc0) == 0x80
    ) {
        *utf8 = ((*ptr & 0x0f) << 12) 
            + ((ptr[1] & 0x3f) << 6) 
            + (ptr[2] & 0x3f);
        ptr += 3;
        if (*utf8 >= 0x800)
            return true;
    }
    *utf8 = 0;
    return false;
}

//===========================================================================
static bool fourByte (unsigned char *& ptr, unsigned * utf8) {
    if ((ptr[1] & 0xc0) == 0x80
        && (ptr[2] & 0xc0) == 0x80
        && (ptr[3] & 0xc0) == 0x80
    ) {
        *utf8 = ((*ptr & 0x07) << 18) 
            + ((ptr[1] & 0x3f) << 12) 
            + ((ptr[2] & 0x3f) << 6)
            + (ptr[3] & 0x3f);
        ptr += 4;
        if (*utf8 >= 0x10000)
            return true;
    }
    *utf8 = 0;
    return false;
}

//===========================================================================
bool XStreamParser::parseNode (
    IXStreamParserNotify & notify, 
    unsigned char *& ptr
) {
    unsigned utf8;
    switch (s_charMap[*ptr]) {
    default: // <
        return fail("expected element name");
    case kUtf8Lead2:
        return twoByte(ptr, &utf8)
            ? parseElement(notify, ptr, ptr + 2, utf8)
            : fail("invalid utf-8 encoding");
    case kUtf8Lead3:
    case kUtf8BomIntro:
        return threeByte(ptr, &utf8)
            ? parseElement(notify, ptr, ptr + 3, utf8)
            : fail("invalid utf-8 encoding");
    case kUtf8Lead4:
        return fourByte(ptr, &utf8)
            ? parseElement(notify, ptr, ptr + 4, utf8)
            : fail("invalid utf-8 encoding");
    case kColon:
    case kAlpha:
    case kHexAlpha:
    case kUnder:
        return parseElement(notify, ptr, ptr + 1);
    case kQuestion: // <?
        ptr += 1;
        return parsePI(ptr);
    case kBang: // <!
        ptr += 1;
        return parseBangNode(notify, ptr);
    }
}

//===========================================================================
static bool isNameStartChar (unsigned ch) {
    // list of half-open unicode character ranges allowed to start an
    // element name.
    const unsigned ranges[] = {
        0,
        0x3a, 0x3b, // :
        0x41, 0x5b, // A-Z
        0x5f, 0x60, // _
        0x61, 0x7b, // a-z
        0xc0, 0xd7, // A-O w/diacritic
        0xd8, 0xf7, // O-o w/diacritic
        0xf8, 0x300, 
        0x370, 0x37e, 
        0x37f, 0x2000,
        0x200c, 0x200e, 
        0x2070, 0x2190, 
        0x2c00, 0x2ff0, 
        0x3001, 0xd800,
        0xf900, 0xfdd0, 
        0xfdf0, 0xfffe, 
        0x10000, 0xf0000,
    };
    auto * range = lower_bound(ranges, ::end(ranges), ch);
    return (range - ranges) % 2 == 1;
}

//===========================================================================
static bool isNameChar (unsigned ch) {
    // list of half-open unicode character ranges allowed to start an
    // element name.
    const unsigned ranges[] = {
        0,
        0x2d, 0x2f, // - .
        0x30, 0x3b, // 0-9 :
        0x41, 0x5b, // A-Z
        0x5f, 0x60, // _
        0x61, 0x7b, // a-z
        0xb7, 0xb8, // Middle dot
        0xc0, 0xd7, // A-O w/diacritic
        0xd8, 0xf7, // O-o w/diacritic
        0xf8, 0x37e, // o-y w/diacritic, 
        0x37f, 0x2000,
        0x200c, 0x200e, 
        0x203f, 0x2041,
        0x2070, 0x2190, 
        0x2c00, 0x2ff0, 
        0x3001, 0xd800,
        0xf900, 0xfdd0, 
        0xfdf0, 0xfffe, 
        0x10000, 0xf0000,
    };
    auto * range = lower_bound(ranges, ::end(ranges), ch);
    return (range - ranges) % 2 == 1;
}

//===========================================================================
bool XStreamParser::parseElement (
    IXStreamParserNotify & notify, 
    unsigned char *& ptr,
    unsigned char * nextChar,
    unsigned firstChar
) {
    if (!isNameStartChar(firstChar))
        return fail("invalid NameStartChar");
    return parseElement(notify, ptr, nextChar);
}

//===========================================================================
bool XStreamParser::parseElement (
    IXStreamParserNotify & notify, 
    unsigned char *& ptr,
    unsigned char * nextChar
) {
    unsigned utf8;
    unsigned char * startTag = ptr;
    ptr = nextChar;
    for (;;) {
        switch (s_charMap[*ptr]) {
        case kNull:
            return fail("expected >");
        case kUtf8Lead2:
            if (!twoByte(ptr, &utf8))
                return fail("invalid utf-8 encoding");
            if (!isNameChar(utf8))
                return fail("invalid name char");
            ptr += 2;
            break;
        case kUtf8Lead3:
        case kUtf8BomIntro:
            if (!threeByte(ptr, &utf8))
                return fail("invalid utf-8 encoding");
            if (!isNameChar(utf8))
                return fail("invalid name char");
            ptr += 3;
            break;
        case kUtf8Lead4:
            if (!fourByte(ptr, &utf8))
                return fail("invalid utf-8 encoding");
            if (!isNameChar(utf8))
                return fail("invalid name char");
            ptr += 4;
            break;
        case kColon:
        case kUnder:
        case kAlpha:
        case kHexAlpha:
        case kDash:
        case kDot:
        case kNum:
            ptr += 1;
            break;
        case kLinefeed:
            m_line += 1;
            [[fallthrough]];
        case kWhitespace:
        case kReturn:
            return parseAttrs(notify, ptr);
        case kGreater:
            ;
            break;
        case kSlash:
            if (!notify.StartElem(*this, (char *) startTag, ptr - startTag))
                return false;
            ;
            break;
        }
    }
}

//===========================================================================
bool XStreamParser::parsePI (unsigned char *& ptr) {
    // skip to ?>
    for (;;) {
        switch (*ptr) {
        case '\0':
            return fail("unexpected end of data");
        case '?':
            if (ptr[1] != '>') {
                ptr += 2;
                return nullptr;
            }
            break;
        case '\n':
            m_line += 1;
            break;
        }
        ptr += 1;
    }
}

//===========================================================================
bool XStreamParser::parseBangNode (
    IXStreamParserNotify & notify, 
    unsigned char *& ptr
) {
    switch (*ptr) {
    case kDash:
        return parseComment(ptr);
    case kLBracket:
        return parseCData(notify, ptr);
    }
    return false;
}

//===========================================================================
bool XStreamParser::parseComment (unsigned char *& ptr) {
    return false;
}

//===========================================================================
bool XStreamParser::parseCData (
    IXStreamParserNotify & notify, 
    unsigned char *& ptr
) {
    return false;
}

//===========================================================================
bool XStreamParser::parseAttrs (
    IXStreamParserNotify & notify, 
    unsigned char *& ptr
) {
    return false;
}

//===========================================================================
bool XStreamParser::parseContent (
    IXStreamParserNotify & notify, 
    unsigned char *& ptr
) {
    return false;
}

//===========================================================================
void XStreamParser::clear () {
    m_heap.clear();
    m_line = 0;
    m_failed = false;
}

//===========================================================================
ITempHeap & XStreamParser::heap () {
    return m_heap;
}


/****************************************************************************
*
*   Dom parser
*
***/

//===========================================================================
XElem * XParser::setRoot (const char elemName[], const char text[]) {
    assert(elemName);
    XElemInfo * elem = heap().emplace<XElemInfo>();
    elem->m_name = elemName;
    elem->m_value = text ? text : "";
    return elem;
}


/****************************************************************************
*
*   Public API
*
***/


} // namespace
