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
*   Declarations
*
***/


/****************************************************************************
*
*   Helpers
*
***/


/****************************************************************************
*
*   DbLog
*
***/

//===========================================================================
DbLog::DbLog(DbData & data, DbPage & work)
    : m_data(data)
    , m_page(work)
{}

//===========================================================================
bool DbLog::open(string_view logfile) {
    return true;
}

//===========================================================================
unsigned DbLog::beginTxn() {
    for (;;) {
        if (++m_lastTxn)
            break;
    }
    logBeginTxn(m_lastTxn);
    return m_lastTxn;
}

//===========================================================================
void DbLog::commit(unsigned txn) {
    logCommit(txn);
}

//===========================================================================
void DbLog::apply(const Record * log) {
    auto pgno = getPgno(log);
    auto lsn = getLsn(log);
    auto vptr = make_unique<char[]>(m_page.pageSize());
    auto ptr = new(vptr.get()) DbPageHeader;
    auto rptr = m_page.rptr(lsn, pgno);
    memcpy(ptr, rptr, m_page.pageSize());
    ptr->pgno = pgno;
    ptr->lsn = lsn;
    apply(ptr, log);
    m_page.writePage(ptr);
}

//===========================================================================
void DbLog::applyBeginTxn(uint16_t txn) {
}

//===========================================================================
void DbLog::applyCommit(uint16_t txn) {
}


/****************************************************************************
*
*   DbTxn
*
***/

//===========================================================================
DbTxn::DbTxn(DbLog & log, DbPage & work)
    : m_log{log}
    , m_page{work}
{}

//===========================================================================
DbTxn::~DbTxn() {
    if (m_txn)
        m_log.commit(m_txn);
}
