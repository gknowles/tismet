// Copyright Glen Knowles 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// dblog.cpp - tismet db
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace Dim;


/****************************************************************************
*
*   DbLog
*
***/

//===========================================================================
uint64_t DbLog::beginTrans() {
    return m_lastTxnId += 1 << 16;
}

//===========================================================================
void DbLog::commit(uint64_t & lsn) {
    lsn = 0;
}


/****************************************************************************
*
*   DbTxn
*
***/

//===========================================================================
DbTxn::DbTxn(DbLog & log)
    : m_log{log}
{
    m_lsn = m_log.beginTrans();
}

//===========================================================================
DbTxn::~DbTxn() {
    m_log.commit(m_lsn);
}

//===========================================================================
void DbTxn::logPageFree(uint32_t pgno) {
    assert(m_lsn);
    m_lsn += 1;
}

//===========================================================================
void DbTxn::logSegmentInit(uint32_t pgno) {
    assert(m_lsn);
    m_lsn += 1;
}

//===========================================================================
void DbTxn::logSegmentUpdate(
    uint32_t pgno,
    uint32_t refPage,
    bool free
) {
    assert(m_lsn);
    m_lsn += 1;
}

//===========================================================================
void DbTxn::logRadixInit(
    uint32_t pgno,
    uint16_t height,
    uint32_t * firstPage,
    uint32_t * lastPage
) {
    assert(m_lsn);
    m_lsn += 1;
}

//===========================================================================
void DbTxn::logRadixUpdate(
    uint32_t pgno,
    size_t pos,
    uint32_t refPage
) {
    assert(m_lsn);
    m_lsn += 1;
}

//===========================================================================
void DbTxn::logRadixErase(
    uint32_t pgno,
    size_t firstPos,
    size_t lastPos
) {
    assert(m_lsn);
    m_lsn += 1;
}

//===========================================================================
void DbTxn::logMetricInit(
    uint32_t pgno,
    string_view name,
    Duration retention,
    Duration interval
) {
    assert(m_lsn);
    m_lsn += 1;
}

//===========================================================================
void DbTxn::logMetricUpdate(
    uint32_t pgno,
    Duration retention,
    Duration interval
) {
    assert(m_lsn);
    m_lsn += 1;
}

//===========================================================================
void DbTxn::logMetricClearSamples(uint32_t pgno) {
    assert(m_lsn);
    m_lsn += 1;
}

//===========================================================================
void DbTxn::logMetricUpdateSamples(
    uint32_t pgno,
    size_t pos,
    uint32_t refPage,
    bool updateLast,
    bool updateIndex
) {
    assert(m_lsn);
    m_lsn += 1;
}

//===========================================================================
void DbTxn::logSampleInit(
    uint32_t pgno,
    TimePoint pageTime,
    size_t lastPos
) {
    assert(m_lsn);
    m_lsn += 1;
}

//===========================================================================
void DbTxn::logSampleUpdate(
    uint32_t pgno,
    size_t firstPos,
    size_t lastPos,
    float value,
    bool updateLast
) {
    assert(m_lsn);
    m_lsn += 1;
}

//===========================================================================
void DbTxn::logSampleUpdateTime(
    uint32_t pgno,
    TimePoint pageTime
) {
    assert(m_lsn);
    m_lsn += 1;
}
