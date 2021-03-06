// Copyright Glen Knowles 2018.
// Distributed under the Boost Software License, Version 1.0.
//
// gfmain.cpp - genfuncs
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace Dim;


/****************************************************************************
*
*   Declarations
*
***/

char const kVersion[] = "1.1.0";


/****************************************************************************
*
*   Helpers
*
***/

//===========================================================================
static string genFuncEnum(string_view fname) {
    Path path(fname);
    ostringstream os;
    os << 1 + R"(
// )" << path.filename() << R"( - tismet func
// Generated by genfuncs )" << kVersion << R"(
// clang-format off
#pragma once


/****************************************************************************
*
*   Declarations
*
***/

namespace Eval {

namespace Function {
    enum Type : int {
        kInvalid = 0,
)";
    UnsignedSet ids;
    for (auto && f : funcTokenConv().funcTypeTbl()) {
        if (ids.insert(f.id)) {
            os << "        k" << (char) toupper(*f.name) << f.name + 1
                << " = " << f.id << ",\n";
        }
    }
    os << 1 + R"(
    };
}
)";
    for (auto && e : funcEnums()) {
        os << 1 + R"(
namespace )" << (char) toupper(e.name[0]) << e.name.c_str() + 1 << R"( {
    enum Type : int {
        kInvalid = 0,
)";
        ids.clear();
        for (auto && t : *e.table) {
            if (!ids.insert(t.id))
                continue;
            os << "        k" << (char) toupper(*t.name) << t.name + 1
                << " = " << t.id << ",\n";
        }
        os << "    };\n"
            << "}\n";
    }
    os << 1 + R"(

} // namespace
)";
    return os.str();
}

//===========================================================================
static string genQueryFunc(string_view fname) {
    Path path(fname);
    ostringstream os;
    os << 1 + R"(
// )" << path.filename() << R"( - tismet query
// Generated by genfuncs )" << kVersion << R"(
// clang-format off
#pragma once


/****************************************************************************
*
*   Query functions
*
***/
)";
    UnsignedSet ids;
    for (auto && f : funcTokenConv().funcTypeTbl()) {
        if (!ids.insert(f.id))
            continue;
        auto name = string(f.name);
        name[0] = (char) toupper(name[0]);
        os << 1 + R"(

//===========================================================================
inline bool QueryParser::onFn)" << name << R"(Start () {
    return startFunc(Eval::Function::k)" << name << R"();
}
)";
    }
    return os.str();
}

//===========================================================================
static string argTypeName(Eval::FuncArgInfo const & arg) {
    switch (arg.type) {
    case Eval::FuncArg::kFunc: return "arg-func";
    case Eval::FuncArg::kNum: return "arg-num";
    case Eval::FuncArg::kNumOrString: return "(arg-num / arg-string)";
    case Eval::FuncArg::kPath: return "arg-path";
    case Eval::FuncArg::kPathOrFunc: return "arg-path-or-func";
    case Eval::FuncArg::kString: return "arg-string";
    case Eval::FuncArg::kEnum: return "arg-"s + arg.enumName;
    default:
        assert(!"Unknown argument type");
        return "arg-invalid";
    }
}

//===========================================================================
static void genAbnfArg(
    ostream & os,
    Eval::FuncArgInfo const & arg,
    bool first
) {
    auto aname = argTypeName(arg);
    if (first && arg.multiple) {
        assert(arg.require && "First argument must not be optional");
        os << aname << " *( \",\" " << aname << " ) ";
    } else if (first && !arg.multiple) {
        assert(arg.require && "First argument must not be optional");
        os << aname << ' ';
    } else if (!first && arg.require && arg.multiple) {
        os << "1*( \",\" " << aname << " ) ";
    } else if (!first && arg.require && !arg.multiple) {
        os << "\",\" " << aname << ' ';
    } else if (!first && !arg.require && arg.multiple) {
        os << "*( \",\" " << aname << " ) ";
    } else if (!first && !arg.require && !arg.multiple) {
        os << "[ \",\" " << aname << " ] ";
    }
}

//===========================================================================
static string genQueryAbnf(string_view fname) {
    Path path(fname);
    ostringstream os;
    os << 1 + R"(
; )" << path.filename() << R"( - tismet query
; Generated by genfuncs )" << kVersion << R"(

;----------------------------------------------------------------------------
; Functions
;----------------------------------------------------------------------------
)";
    vector<Eval::IFuncFactory const *> factories;
    for (auto && f : funcFactories())
        factories.push_back(&f);
    sort(factories.begin(), factories.end(), [](auto & a, auto & b) {
        return _stricmp(
            a->m_names.front().c_str(),
            b->m_names.front().c_str()
        ) < 0;
    });
    for (auto && f : factories) {
        os << "func =";
        for (auto && n : f->m_names)
            os << "/ fn-" << n << ' ';
        os << "{ End }\n";
        for (auto && n : f->m_names) {
            os << 1 + R"(
fn-)" << n << R"( = %s")" << n << R"((" )";
            for (unsigned i = 0; i < f->m_args.size(); ++i)
                genAbnfArg(os, f->m_args[i], i == 0);
            os << "\")\" { Start";
            if (&n != f->m_names.data())
                os << ", As fn-" << f->m_names.front();
            os << " }\n";
        }
        os << '\n';
    }

    os << 1 + R"(
;----------------------------------------------------------------------------
; Enumeration arguments
;----------------------------------------------------------------------------
)";
    for (auto && e : funcEnums()) {
        os << "arg-" << e.name << " = *WSP (DQUOTE enum-" << e.name << " DQUOTE"
            << " / \"'\" enum-" << e.name << " \"'\") *WSP\n";
        for (auto && t : *e.table) {
            os << "enum-" << e.name << " =/ %s\"" << t.name << "\" "
                << "{ As string, Start+, End+ }\n";
        }
        os << '\n';
    }

    return os.str();
}

//===========================================================================
static void updateFile(string_view fname, string_view content) {
    vector<string_view> lines;
    split(&lines, content, '\n');
    string ncontent{lines[0]};
    for (auto i = 1; i < lines.size(); ++i) {
        ncontent += "\r\n";
        ncontent += lines[i];
    }
    string ocontent;
    if (fileExists(fname))
        fileLoadBinaryWait(&ocontent, fname);
    if (ocontent == ncontent) {
        cout << fname << ", no change\n";
    } else {
        auto f = fileOpen(
            fname,
            File::fReadWrite | File::fCreat | File::fTrunc | File::fBlocking
        );
        fileAppendWait(f, ncontent.data(), ncontent.size());
        fileClose(f);
        cout << fname << ", ";
        ConsoleScopedAttr attr(kConsoleNote);
        cout << "UPDATED\n";
    }
}


/****************************************************************************
*
*   Application
*
***/

//===========================================================================
static void app(int argc, char * argv[]) {
    funcInitialize();

    Cli cli;
    auto version = string(kVersion) + " (" __DATE__ ")";
    cli.header("genfn v"s + version);
    cli.versionOpt(version, "tsm");
    cli.desc("Code generation for metric function enums, abnf, and parser.");
    cli.helpNoArgs();
    auto & root = cli.opt<Path>("<project directory>")
        .desc("Root directory of tismet source code.");
    if (!cli.parse(argc, argv))
        return appSignalUsageError();

    auto sln = *root / "tismet.sln";
    if (!fileExists(sln)) {
        return appSignalUsageError(
            "'" + string(*root) + "' not tismet source root."
        );
    }

    auto funcenum_h = *root / "libs/func/fnenum.h";
    updateFile(funcenum_h, genFuncEnum(funcenum_h));
    auto query_h = *root / "libs/query/qryparseimplfnint.h";
    updateFile(query_h, genQueryFunc(query_h));
    auto query_abnf = *root / "libs/query/queryfunc.abnf";
    updateFile(query_abnf, genQueryAbnf(query_abnf));

    appSignalShutdown();
}


/****************************************************************************
*
*   main
*
***/

//===========================================================================
int main(int argc, char *argv[]) {
    _CrtSetDbgFlag(_CRTDBG_ALLOC_MEM_DF
        | _CRTDBG_LEAK_CHECK_DF
//        | _CRTDBG_DELAY_FREE_MEM_DF
    );
    _set_error_mode(_OUT_TO_MSGBOX);

    int code = appRun(app, argc, argv);
    return code;
}
