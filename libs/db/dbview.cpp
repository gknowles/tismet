// Copyright Glen Knowles 2017 - 2018.
// Distributed under the Boost Software License, Version 1.0.
//
// dbview.cpp - tismet db
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace Dim;


/****************************************************************************
*
*   DbFileView
*
***/

//===========================================================================
template<bool Writable>
DbFileView<Writable>::~DbFileView() {
    close();
}

//===========================================================================
template<bool Writable>
bool DbFileView<Writable>::open(
    FileHandle file,
    size_t viewSize,
    size_t pageSize
) {
    assert(!m_file && "file view already open");
    assert(pageSize == pow2Ceil(pageSize));
    assert(viewSize % fileViewAlignment(file) == 0);
    assert(viewSize % pageSize == 0);

    m_viewSize = viewSize;
    m_pageSize = pageSize;

    // First view is the size of the entire file rounded up to segment size,
    // and always at least two segments.
    auto len = fileSize(file);
    m_firstViewSize = len + viewSize - 1;
    m_firstViewSize -= m_firstViewSize % viewSize;
    m_firstViewSize = max(m_firstViewSize, minFirstSize());

    int64_t commit = m_firstViewSize > minFirstSize() ? m_firstViewSize : 0;
    m_view = nullptr;
    if (!fileOpenView(
        m_view,
        file,
        kMode,
        0,      // offset
        commit, // length committed
        m_firstViewSize
    )) {
        logMsgError() << "Open view failed, " << filePath(file);
        return false;
    }

    m_file = file;
    return true;
}

//===========================================================================
template<bool Writable>
void DbFileView<Writable>::close() {
    if (m_view) {
        fileCloseView(m_file, m_view);
        m_view = nullptr;
    }
    for (auto && v : m_views) {
        fileCloseView(m_file, v);
    }
    m_views.clear();
    m_file = {};
}

//===========================================================================
template<bool Writable>
void DbFileView<Writable>::growToFit(pgno_t pgno) {
    auto pos = pgno * m_pageSize;
    if (pos < m_firstViewSize) {
        if (pos < minFirstSize()) {
            fileExtendView(m_file, m_view, pos + m_pageSize);
        }
        return;
    }

    auto viewPos = pos - m_firstViewSize;
    auto iview = viewPos / m_viewSize;
    if (iview < m_views.size())
        return;
    assert(iview == m_views.size() && "non-contiguous grow request");
    Pointer view;
    if (!fileOpenView(
        view,
        m_file,
        kMode,
        pos,
        m_viewSize,
        m_viewSize
    )) {
        logMsgFatal() << "Extend file failed on " << filePath(m_file);
    }
    m_views.push_back(view);
}

//===========================================================================
template<bool Writable>
void const * DbFileView<Writable>::rptr(pgno_t pgno) const {
    return ptr(pgno);
}

//===========================================================================
template<bool Writable>
size_t DbFileView<Writable>::minFirstSize() const {
    return 2 * m_viewSize;
}

//===========================================================================
template<bool Writable>
auto DbFileView<Writable>::ptr(pgno_t pgno) const
    -> Pointer
{
    auto pos = pgno * m_pageSize;
    if (pos < m_firstViewSize)
        return m_view + pos;
    auto viewPos = pos - m_firstViewSize;
    auto iview = viewPos / m_viewSize;
    if (iview < m_views.size())
        return m_views[iview] + viewPos % m_viewSize;
    return nullptr;
}

//===========================================================================
template<bool Writable>
pgno_t DbFileView<Writable>::pgno(void const * vptr) const {
    auto ptr = (char const *) vptr;
    if (ptr >= m_view && ptr < m_view + m_firstViewSize)
        return pgno_t((ptr - m_view) / m_pageSize);
    auto num = m_firstViewSize / m_pageSize;
    for (auto && v : m_views) {
        if (ptr >= v && ptr < v + m_viewSize)
            return pgno_t(num + (ptr - v) / m_pageSize);
        num += m_viewSize / m_pageSize;
    }
    return kMaxPageNum;
}


/****************************************************************************
*
*   DbWriteView
*
***/

//===========================================================================
void * DbWriteView::wptr(pgno_t pgno) const {
    return ptr(pgno);
}


/****************************************************************************
*
*   Public API
*
***/

template class DbFileView<false>;   // read only
template class DbFileView<true>;    // read/write
