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
void DbTxn::logFree(uint32_t pgno) {
    assert(m_lsn);
    m_lsn += 1;
}

//===========================================================================
void DbTxn::logInitSegment(uint32_t pgno) {
    assert(m_lsn);
    m_lsn += 1;
}

//===========================================================================
void DbTxn::logUpdateSegment(
    uint32_t pgno,
    uint32_t refPage,
    bool free
) {
    assert(m_lsn);
    m_lsn += 1;
}

//===========================================================================
void DbTxn::logInitRadix(
    uint32_t pgno,
    uint16_t height,
    uint32_t * firstPage,
    uint32_t * lastPage
) {
    assert(m_lsn);
    m_lsn += 1;
}

//===========================================================================
void DbTxn::logUpdateRadix(
    uint32_t pgno,
    size_t pos,
    uint32_t refPage
) {
    assert(m_lsn);
    m_lsn += 1;
}

//===========================================================================
void DbTxn::logCopyRadix(
    uint32_t pgno,
    uint16_t height,
    uint32_t * firstPage,
    uint32_t * lastPage
) {
    assert(m_lsn);
    m_lsn += 1;
}

//===========================================================================
void DbTxn::logEraseRadix(
    uint32_t pgno,
    size_t firstPos,
    size_t lastPos
) {
    assert(m_lsn);
    m_lsn += 1;
}

//===========================================================================
void DbTxn::logPromoteRadix(uint32_t pgno, uint16_t height) {
    assert(m_lsn);
    m_lsn += 1;
}

//===========================================================================
void DbTxn::logInitMetric(
    uint32_t pgno,
    string_view name,
    Duration retention,
    Duration interval
) {
    assert(m_lsn);
    m_lsn += 1;
}

//===========================================================================
void DbTxn::logUpdateMetric(
    uint32_t pgno,
    Duration retention,
    Duration interval
) {
    assert(m_lsn);
    m_lsn += 1;
}

//===========================================================================
void DbTxn::logClearSampleIndex(uint32_t pgno) {
    assert(m_lsn);
    m_lsn += 1;
}

//===========================================================================
void DbTxn::logUpdateSampleIndex(
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
void DbTxn::logInitSample(
    uint32_t pgno,
    TimePoint pageTime,
    size_t lastPos
) {
    assert(m_lsn);
    m_lsn += 1;
}

//===========================================================================
void DbTxn::logUpdateSample(
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
void DbTxn::logUpdateSampleTime(
    uint32_t pgno,
    TimePoint pageTime
) {
    assert(m_lsn);
    m_lsn += 1;
}
