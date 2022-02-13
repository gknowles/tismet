// Copyright Glen Knowles 2017 - 2021.
// Distributed under the Boost Software License, Version 1.0.
//
// testindex.cpp - tismet test
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
#define EXPECT_FIND(query, result) \
    findTest(__LINE__, index, query, result)


/****************************************************************************
*
*   Helpers
*
***/

//===========================================================================
static void findTest(
    int line,
    const DbIndex & index,
    string_view query,
    string_view result
) {
    UnsignedSet out;
    index.find(&out, query);
    ostringstream os;
    os << out;
    auto found = os.str();
    if (found != result) {
        logMsgError() << "Line " << line << ": EXPECT('"
            << found << "' == '" << result << "') failed";
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
    Test();
    void onTestRun() override;
};

} // namespace

static Test s_test;

//===========================================================================
Test::Test()
    : ITest("index", "Metric index tests.") 
 {}

//===========================================================================
void Test::onTestRun() {
    int line = 0;

    DbIndex index;

    index.clear();
    index.insert(1, "a.bd.c");
    EXPECT_FIND("a.b{,d}.c", "1");

    index.clear();
    index.insert(1, "a");
    index.insert(2, "b");
    index.insert(3, "ab");
    index.insert(4, "ad");
    index.insert(5, "abc");
    index.insert(6, "abd");
    EXPECT_FIND("a{b,c}", "3");
    EXPECT_FIND("{a,c}b", "3");
    EXPECT_FIND("{a,b}", "1-2");
    EXPECT_FIND("{a[bd],b}", "2-4");

    index.clear();
    index.insert(1, "a.z");
    index.insert(2, "a.b.m.z");
    index.insert(3, "a.m.y.z");
    index.insert(4, "a.b.m.y.z");
    EXPECT(index.size() == 4);

    // 2+ exact segments, the least matching of which has no intersection
    // with keys of the requested number of segments.
    EXPECT_FIND("*.z.m.*", "");

    EXPECT_FIND("a*", "");
    EXPECT_FIND("a*.z", "1");
    EXPECT_FIND("a.b*", "");

    UnsignedSet ids;
    uint32_t id;
    index.find(&id, "a.m.y.z");
    EXPECT(id == 3);

    EXPECT_FIND("a.*.*.z", "2-3");
    EXPECT_FIND("**", "1-4");
    EXPECT_FIND("a.b.**", "2 4");
    EXPECT_FIND("**.y.z", "3-4");
    EXPECT_FIND("a.**.z", "1-4");
    EXPECT_FIND("a.**.m.**.z", "2-4");
}
