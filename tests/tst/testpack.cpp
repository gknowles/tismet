// Copyright Glen Knowles 2018 - 2022.
// Distributed under the Boost Software License, Version 1.0.
//
// testpack.cpp - tismet test
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

/****************************************************************************
*
*   Test
*
***/

namespace {

class Test : public ITest {
public:
    Test() : ITest("pack", "Sample compression tests.") {}
    void onTestRun() override;
};

} // namespace

static Test s_test;

//===========================================================================
void Test::onTestRun() {
    int line = 0;
    string buf;
    buf.resize(20);
    DbPack pack(buf.data(), buf.size());
    EXPECT(pack.capacity() == 20);
    EXPECT(pack.size() == 0)
    EXPECT(pack.unusedBits() == 0);
    EXPECT(pack.view().size() == 0);
    pack.put(TimePoint{1s}, 1.0);
    pack.put(TimePoint{2s}, 2.0);
    pack.put(TimePoint{3s}, 3.0);
    pack.put(TimePoint{6s}, 3.0);
    pack.put(TimePoint{8s}, 5.0);
    pack.put(TimePoint{9s}, 7.0);
    DbUnpackIter unpack(pack.data(), pack.size(), pack.unusedBits());
    EXPECT(unpack->time == TimePoint{1s});
    EXPECT(unpack->value == 1.0);
    ++unpack;
    EXPECT(unpack->time == TimePoint{2s});
    EXPECT(unpack->value == 2.0);
    ++unpack;
    EXPECT(unpack->time == TimePoint{3s});
    EXPECT(unpack->value == 3.0);
    ++unpack;
    EXPECT(unpack->time == TimePoint{6s});
    EXPECT(unpack->value == 3.0);
    ++unpack;
    EXPECT(unpack->time == TimePoint{8s});
    EXPECT(unpack->value == 5.0);
    ++unpack;
    EXPECT(unpack->time == TimePoint{9s});
    EXPECT(unpack->value == 7.0);
    ++unpack;
    EXPECT(!unpack);
}
