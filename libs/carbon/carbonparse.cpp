// carbonparse.cpp
// Generated by pargen 2.2.0 at 2018-08-13T01:48:27-0700
// clang-format off
#include "pch.h"
#pragma hdrstop


/****************************************************************************
*
*   CarbonParser
*
*   Normalized ABNF of syntax:
*   %root = metric
*   ALPHA = ( %x41-5a / %x61-7a )
*   CR = %xd
*   DIGIT = ( %x30-39 )
*   DQUOTE = %x22
*   LF = %xa
*   SP = %x20
*   decimal-point = %x2e
*   digit1-9 = ( %x31-39 )
*   e = ( %x45 / %x65 )
*   exp = ( e *1( exp-minus / exp-plus ) exp-num )
*   exp-minus = %x2d { End }
*   exp-num = 1*DIGIT { Char+ }
*   exp-plus = %x2b
*   frac = ( decimal-point frac-num )
*   frac-num = 1*DIGIT { Char+ }
*   int = ( *1int-minus int-num )
*   int-minus = %x2d { End }
*   int-num = ( zero / ( digit1-9 *DIGIT ) ) { Char+ }
*   metric = ( path SP value SP timestamp *1CR LF ) { End }
*   now = ( %x2d %x31 ) { End }
*   path = 1*path-chars { Start+, End+ }
*   path-chars = ( ALPHA / DIGIT / DQUOTE / %x21 / %x23-27 / %x2b / %x2d-2e /
*       %x3a-40 / %x5c / %x5e-60 / %x7e )
*   timepoint = 1*DIGIT { Char+ }
*   timestamp = ( now / timepoint )
*   value = ( int *1frac *1exp )
*   zero = %x30
*
***/

//===========================================================================
// Parser function covering:
//  - 21 states
[[gsl::suppress(bounds)]]
bool CarbonParser::parse (char const src[]) {
    char const * ptr = src;
    unsigned char ch;
    goto state2;

state0:
    // <FAILED>
    m_errpos = ptr - src - 1;
    return false;

state1:
    // 1: <DONE>
    return true;

state2:
    // 2:
    ch = *ptr++;
    switch (ch) {
    case '!': case '"': case '#': case '$': case '%': case '&':
    case '\'': case '+': case '-': case '.': case '0': case '1':
    case '2': case '3': case '4': case '5': case '6': case '7':
    case '8': case '9': case ':': case ';': case '<': case '=':
    case '>': case '?': case '@': case 'A': case 'B': case 'C':
    case 'D': case 'E': case 'F': case 'G': case 'H': case 'I':
    case 'J': case 'K': case 'L': case 'M': case 'N': case 'O':
    case 'P': case 'Q': case 'R': case 'S': case 'T': case 'U':
    case 'V': case 'W': case 'X': case 'Y': case 'Z': case '\\':
    case '^': case '_': case '`': case 'a': case 'b': case 'c':
    case 'd': case 'e': case 'f': case 'g': case 'h': case 'i':
    case 'j': case 'k': case 'l': case 'm': case 'n': case 'o':
    case 'p': case 'q': case 'r': case 's': case 't': case 'u':
    case 'v': case 'w': case 'x': case 'y': case 'z': case '~':
        goto state3;
    }
    goto state0;

state3:
    // 3: !
    if (!onPathStart(ptr - 1))
        goto state0;
    ch = *ptr++;
    switch (ch) {
    case ' ':
        goto state4;
    case '!': case '"': case '#': case '$': case '%': case '&':
    case '\'': case '+': case '-': case '.': case '0': case '1':
    case '2': case '3': case '4': case '5': case '6': case '7':
    case '8': case '9': case ':': case ';': case '<': case '=':
    case '>': case '?': case '@': case 'A': case 'B': case 'C':
    case 'D': case 'E': case 'F': case 'G': case 'H': case 'I':
    case 'J': case 'K': case 'L': case 'M': case 'N': case 'O':
    case 'P': case 'Q': case 'R': case 'S': case 'T': case 'U':
    case 'V': case 'W': case 'X': case 'Y': case 'Z': case '\\':
    case '^': case '_': case '`': case 'a': case 'b': case 'c':
    case 'd': case 'e': case 'f': case 'g': case 'h': case 'i':
    case 'j': case 'k': case 'l': case 'm': case 'n': case 'o':
    case 'p': case 'q': case 'r': case 's': case 't': case 'u':
    case 'v': case 'w': case 'x': case 'y': case 'z': case '~':
        goto state20;
    }
    goto state0;

state4:
    // 4: !^x20
    if (!onPathEnd(ptr - 1))
        goto state0;
    ch = *ptr++;
    switch (ch) {
    case '-':
        goto state5;
    case '0':
        goto state6;
    case '1': case '2': case '3': case '4': case '5': case '6':
    case '7': case '8': case '9':
        goto state19;
    }
    goto state0;

state5:
    // 5: ! -
    if (!onIntMinusEnd())
        goto state0;
    ch = *ptr++;
    switch (ch) {
    case '0':
        goto state6;
    case '1': case '2': case '3': case '4': case '5': case '6':
    case '7': case '8': case '9':
        goto state19;
    }
    goto state0;

state6:
    // 6: ! -0
    if (!onIntNumChar(ch))
        goto state0;
    ch = *ptr++;
    switch (ch) {
    case ' ':
        goto state7;
    case '.':
        goto state13;
    case 'E': case 'e':
        goto state15;
    }
    goto state0;

state7:
    // 7: ! -0^x20
    ch = *ptr++;
    switch (ch) {
    case '-':
        goto state8;
    case '0': case '1': case '2': case '3': case '4': case '5':
    case '6': case '7': case '8': case '9':
        goto state12;
    }
    goto state0;

state8:
    // 8: ! -0 -
    ch = *ptr++;
    switch (ch) {
    case '1':
        goto state9;
    }
    goto state0;

state9:
    // 9: ! -0 -1
    if (!onNowEnd())
        goto state0;
    ch = *ptr++;
    switch (ch) {
    case 10:
        goto state10;
    case 13:
        goto state11;
    }
    goto state0;

state10:
    // 10: ! -0 -1^J
    if (!onMetricEnd())
        goto state0;
    ch = *ptr++;
    switch (ch) {
    case 0:
        goto state1;
    }
    goto state0;

state11:
    // 11: ! -0 -1^M
    ch = *ptr++;
    switch (ch) {
    case 10:
        goto state10;
    }
    goto state0;

state12:
    // 12: ! -0 0
    if (!onTimepointChar(ch))
        goto state0;
    ch = *ptr++;
    switch (ch) {
    case 10:
        goto state10;
    case 13:
        goto state11;
    case '0': case '1': case '2': case '3': case '4': case '5':
    case '6': case '7': case '8': case '9':
        goto state12;
    }
    goto state0;

state13:
    // 13: ! -0.
    ch = *ptr++;
    switch (ch) {
    case '0': case '1': case '2': case '3': case '4': case '5':
    case '6': case '7': case '8': case '9':
        goto state14;
    }
    goto state0;

state14:
    // 14: ! -0.0
    if (!onFracNumChar(ch))
        goto state0;
    ch = *ptr++;
    switch (ch) {
    case ' ':
        goto state7;
    case '0': case '1': case '2': case '3': case '4': case '5':
    case '6': case '7': case '8': case '9':
        goto state14;
    case 'E': case 'e':
        goto state15;
    }
    goto state0;

state15:
    // 15: ! -0.0E
    ch = *ptr++;
    switch (ch) {
    case '+':
        goto state16;
    case '-':
        goto state18;
    case '0': case '1': case '2': case '3': case '4': case '5':
    case '6': case '7': case '8': case '9':
        goto state17;
    }
    goto state0;

state16:
    // 16: ! -0.0E+
    ch = *ptr++;
    switch (ch) {
    case '0': case '1': case '2': case '3': case '4': case '5':
    case '6': case '7': case '8': case '9':
        goto state17;
    }
    goto state0;

state17:
    // 17: ! -0.0E+0
    if (!onExpNumChar(ch))
        goto state0;
    ch = *ptr++;
    switch (ch) {
    case ' ':
        goto state7;
    case '0': case '1': case '2': case '3': case '4': case '5':
    case '6': case '7': case '8': case '9':
        goto state17;
    }
    goto state0;

state18:
    // 18: ! -0.0E-
    if (!onExpMinusEnd())
        goto state0;
    ch = *ptr++;
    switch (ch) {
    case '0': case '1': case '2': case '3': case '4': case '5':
    case '6': case '7': case '8': case '9':
        goto state17;
    }
    goto state0;

state19:
    // 19: ! -1
    if (!onIntNumChar(ch))
        goto state0;
    ch = *ptr++;
    switch (ch) {
    case ' ':
        goto state7;
    case '.':
        goto state13;
    case '0': case '1': case '2': case '3': case '4': case '5':
    case '6': case '7': case '8': case '9':
        goto state19;
    case 'E': case 'e':
        goto state15;
    }
    goto state0;

state20:
    // 20: !!
    ch = *ptr++;
    switch (ch) {
    case ' ':
        goto state4;
    case '!': case '"': case '#': case '$': case '%': case '&':
    case '\'': case '+': case '-': case '.': case '0': case '1':
    case '2': case '3': case '4': case '5': case '6': case '7':
    case '8': case '9': case ':': case ';': case '<': case '=':
    case '>': case '?': case '@': case 'A': case 'B': case 'C':
    case 'D': case 'E': case 'F': case 'G': case 'H': case 'I':
    case 'J': case 'K': case 'L': case 'M': case 'N': case 'O':
    case 'P': case 'Q': case 'R': case 'S': case 'T': case 'U':
    case 'V': case 'W': case 'X': case 'Y': case 'Z': case '\\':
    case '^': case '_': case '`': case 'a': case 'b': case 'c':
    case 'd': case 'e': case 'f': case 'g': case 'h': case 'i':
    case 'j': case 'k': case 'l': case 'm': case 'n': case 'o':
    case 'p': case 'q': case 'r': case 's': case 't': case 'u':
    case 'v': case 'w': case 'x': case 'y': case 'z': case '~':
        goto state20;
    }
    goto state0;
}

