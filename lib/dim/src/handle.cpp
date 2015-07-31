// handle.cpp - dim services
#include "dim/handle.h"

#include <cassert>

using namespace std;


/****************************************************************************
*
*   HamdleMap
*
***/

//===========================================================================
HandleMapBase::HandleMapBase () {
    m_values.push_back({nullptr, 0});
}

//===========================================================================
HandleMapBase::~HandleMapBase() {
    assert(Empty());
}

//===========================================================================
bool HandleMapBase::Empty () const {
    return !m_numUsed;
}

//===========================================================================
void * HandleMapBase::Find (HandleBase handle) {
    return handle.pos >= (int) m_values.size() 
        ? NULL 
        : m_values[handle.pos].value;
}

//===========================================================================
HandleBase HandleMapBase::Insert (void * value) {
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
void * HandleMapBase::Release (HandleBase handle) {
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
