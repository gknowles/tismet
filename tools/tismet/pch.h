// Copyright Glen Knowles 2015 - 2018.
// Distributed under the Boost Software License, Version 1.0.
//
// pch.h - tismet

// Public header
// External library public headers
#include "dimcli/cli.h"

#include "app/app.h"
#include "core/core.h"
#include "file/file.h"
#include "json/json.h"
#include "msgpack/msgpack.h"
#include "net/net.h"
#include "wintls/wintls.h"

#include "carbon/carbon.h"
#include "eval/eval.h"
#include "db/db.h"

// Standard headers
#include <crtdbg.h>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <mutex>
#include <regex>

// Platform headers
#pragma pack(push)
#pragma pack()
#define _WIN32_WINNT _WIN32_WINNT_WIN8
#define WIN32_LEAN_AND_MEAN
#define NOMINMAX
#include <Windows.h>
#pragma pack(pop)

// External library internal headers
#include "win/winint.h"

// Internal headers
#include "intern.h"
