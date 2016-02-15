// cmdline.cpp - dim services
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace Dim;
using namespace Dim::CmdLine;


/****************************************************************************
*
*   OptBase
*
***/

//===========================================================================
OptBase::OptBase(
    const std::string & names,
    const std::string & desc,
    bool multiple,
    bool boolean
) {
}

//===========================================================================
OptBase::~OptBase() {
}


/****************************************************************************
*
*   Public Interface
*
***/

//===========================================================================
static bool FindUnescapedQuote(string & arg, const char *& ptr) {
    // only called when a \ is consumed
    assert(ptr[-1] == '\\');

    char ch;
    unsigned num = 1;
    for (;;) {
        ch = *ptr;
        if (ch != '\\') break;
        ptr += 1;
        num += 1;
    }
    if (ch != '"') {
        arg.append(num, '\\');
        // not a quote
        return false;
    }

    // consume the quote, it's about to be either copied or skipped
    ptr += 1;

    arg.append(num / 2, '\\');
    if (num % 2) {
        arg.push_back('"');
        // quote is escaped
        return false;
    }
    // quote is not escaped
    return true;
}

//===========================================================================
static void ParseArg(string & arg, const char *& ptr) {
    for (;;) {
        char ch = *ptr++;
        if (ch == 0 || ch == '\t' || ch == ' ') return;
        if (ch == '\\') {
            if (FindUnescapedQuote(arg, ptr)) {
            }
            unsigned num = 1;
            for (;;) {
                ch = *ptr++;
                if (ch != '\\') break;
                num += 1;
            }
            if (ch != '"') {
                arg.append(num, '\\');
                if (ch == 0 || ch == '\t' || ch == ' ') return;
                arg.push_back(ch);
                continue;
            }
            arg.append(num / 2, '\\');
            if (num % 2) {
                arg.push_back('"');
                continue;
            }
            for (;;) {
                ch = *ptr++;
                if (ch == 0) return;
            }
        }
        if (ch == '"') {
        }
    }
}

//===========================================================================
static void ParseOptions(vector<string> & argv, const char cmdline[]) {
    argv.resize(0);
    const char * ptr = cmdline;

    // skip whitespace
    for (;; ++ptr) {
        char ch = *ptr;
        if (ch == 0) return;
        if (ch == '\t' || ch == ' ') break;
    }

    // get args
    string arg;
    while (*ptr) {
        ParseArg(arg, ptr);
        argv.push_back(move(arg));
    }
}

//===========================================================================
static bool ParseOptions (const vector<string> & argv) {
    return argv.size() || argv.empty();
}

//===========================================================================
bool Dim::CmdLine::ParseOptions(int argc, char * argv[]) {
    ostringstream os;
    int args = 0;
    for (char * ptr = *argv; ptr; ptr = *++argv) {
        if (++args > 1)
            os << ' ';
        os << '"';
        for (; *ptr; ++ptr) {
            switch (*ptr) {
                case '\\':
                case '"':
                    os << '\\';
            }
            os << *ptr;
        }
        os << '"';
    }
    assert(args == argc);

    return ParseOptions(os.str().c_str());
}

//===========================================================================
bool Dim::CmdLine::ParseOptions(const char cmdline[]) {
    vector<string> argv;
    ::ParseOptions(argv, cmdline);
    return ::ParseOptions(argv);
}

void Dim::CmdLine::PrintError(std::ostream & os);
void Dim::CmdLine::PrintHelp(std::ostream & os);
