// Copyright Glen Knowles 2017 - 2021.
// Distributed under the Boost Software License, Version 1.0.
//
// testquery.cpp - tismet test
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace Dim;


/****************************************************************************
*
*   Declarations
*
***/

#define EXPECT(...) \
    if (!bool(__VA_ARGS__)) { \
        logMsgError() << "Line " << (line ? line : __LINE__) << ": EXPECT(" \
            << #__VA_ARGS__ << ") failed"; \
    }
#define EXPECT_PARSE(text, normal) \
    parseTest(__LINE__, text, normal)


/****************************************************************************
*
*   Helpers
*
***/

//===========================================================================
static void parseTest(
    int line,
    const string & src,
    string_view normal
) {
    Query::QueryInfo qry;
    bool result = parse(qry, src);
    EXPECT(result);
    if (result) {
        EXPECT(qry.text == normal);
    }
}


/****************************************************************************
*
*   Test
*
***/

namespace {

class Test : public ITest {
public:
    Test() : ITest("query", "Query parsing tests.") {}
    void onTestRun() override;
};

} // namespace

static Test s_test;

//===========================================================================
void Test::onTestRun() {
    auto start = timeFromUnix(900'000'000);

    EXPECT_PARSE("sum(sum(a))", "sumSeries(sumSeries(a))");
    EXPECT_PARSE("a.b{,d}", "a.b{,d}");
    EXPECT_PARSE("a{,b}", "a{,b}");
    EXPECT_PARSE("a{ [12] , cd[34] }", "a{cd[34],[12]}");
    EXPECT_PARSE("a.{ xxx ,zzz,xxx, yyyyy }.b", "a.{xxx,yyyyy,zzz}.b");
    EXPECT_PARSE("**", "**");
    EXPECT_PARSE("**.**.*.**.a.*.**", "*.**.a.*.**");
    EXPECT_PARSE("a**b.**c.**.d.***.e", "a*b.*c.**.d.*.e");
    EXPECT_PARSE("a[b]c[de]f", "abc[de]f");
    EXPECT_PARSE("a[62-41]", "a[12346]");
    EXPECT_PARSE("a.b.c", "a.b.c");

    EXPECT_PARSE("alias(a.b, \"legend\" )", "alias(a.b, \"legend\")");
    EXPECT_PARSE("sum( a )", "sumSeries(a)");
    EXPECT_PARSE("sum(maximumAbove(a.b[12-46], 2))",
        "sumSeries(maximumAbove(a.b[12346], 2))");
}
