// handle.cpp - dim services
#include "pch.h"
#pragma hdrstop

using namespace std;


/****************************************************************************
*
*   HamdleMap
*
***/

//===========================================================================
DimHandleMapBase::DimHandleMapBase () {
    m_values.push_back({nullptr, 0});
}

//===========================================================================
DimHandleMapBase::~DimHandleMapBase() {
    assert(Empty());
}

//===========================================================================
bool DimHandleMapBase::Empty () const {
    return !m_numUsed;
}

//===========================================================================
void * DimHandleMapBase::Find (DimHandleBase handle) {
    return handle.pos >= (int) m_values.size() 
        ? NULL 
        : m_values[handle.pos].value;
}

//===========================================================================
DimHandleBase DimHandleMapBase::Insert (void * value) {
    int pos;
    if (!m_firstFree) {
        m_values.push_back({value, 0});
        pos = (int) m_values.size() - 1;
    } else {
        pos = m_firstFree;
        Node & node = m_values[pos];
        m_firstFree = node.next;
        node = {value, 0};
    }

    m_numUsed += 1;
    return {pos};
}

//===========================================================================
void * DimHandleMapBase::Release (DimHandleBase handle) {
    if (handle.pos >= (int) m_values.size())
        return nullptr;
    Node & node = m_values[handle.pos];
    if (!node.value)
        return nullptr;

    m_numUsed -= 1;
    void * value = node.value;
    node = {nullptr, m_firstFree};
    m_firstFree = handle.pos;
    return value;
}
