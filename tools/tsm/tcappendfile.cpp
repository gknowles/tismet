// Copyright Glen Knowles 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// tcappendfile.cpp - tsm
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace Dim;


/****************************************************************************
*
*   FileAppendQueue
*
***/

//===========================================================================
FileAppendQueue::FileAppendQueue(int numBufs) 
    : m_numBufs{numBufs}
{}

//===========================================================================
FileAppendQueue::~FileAppendQueue() {
    close();
    if (m_buffers)
        aligned_free(m_buffers);
}

//===========================================================================
bool FileAppendQueue::open(std::string_view path, OpenExisting mode) {
    auto flags = File::fReadWrite | File::fAligned;
    switch (mode) {
    case kFail: flags |= File::fCreat | File::fExcl; break;
    case kAppend: flags |= File::fCreat; break;
    case kTrunc: flags |= File::fCreat | File::fTrunc; break;
    }
    auto f = fileOpen(path, flags);
    if (!f)
        return Cli{}.fail(EX_DATAERR, string(path) + ": open failed");
    if (!attach(f))
        fileClose(f);
    return (bool) *this;
}

//===========================================================================
bool FileAppendQueue::attach(Dim::FileHandle f) {
    close();
    m_filePos = fileSize(f);
    if (!m_filePos && errno)
        return Cli{}.fail(EX_DATAERR, string(filePath(f)) + ": open failed");

    m_file = f;
    if (!m_buffers) {
        m_bufLen = envMemoryConfig().pageSize;
        m_buffers = (char *) aligned_alloc(m_bufLen, m_numBufs * m_bufLen);
    }

    auto used = m_filePos % m_bufLen;
    m_buf = string_view{m_buffers + used, m_bufLen - used};
    m_filePos -= used;
    if (m_filePos)
        fileReadWait(m_buffers, m_bufLen, m_file, m_filePos);
    return true;
}

//===========================================================================
void FileAppendQueue::close() {
    if (!m_file)
        return;

    unique_lock<mutex> lk{m_mut};
    while (m_usedBufs) 
        m_cv.wait(lk);

    if (auto used = m_bufLen - m_buf.size()) {
        if (~fileMode(m_file) & File::fAligned) {
            fileAppendWait(m_file, m_buf.data() - used, used);
        } else {
            // Since the old file handle was opened with fAligned we can't use 
            // it to write the trailing partial buffer.
            Path path = filePath(m_file);
            fileClose(m_file);
            m_file = fileOpen(path, File::fReadWrite | File::fBlocking);
            if (m_file)
                fileAppendWait(m_file, m_buf.data() - used, used);
        }
    }
    fileClose(m_file);
    m_file = {};
}

//===========================================================================
void FileAppendQueue::append(std::string_view data) {
    if (!m_file)
        return;
    auto bytes = min(data.size(), m_buf.size());
    memcpy((char *) m_buf.data(), data.data(), bytes);
    m_buf.remove_prefix(bytes);
    if (!m_buf.empty()) 
        return;

    {
        unique_lock<mutex> lk{m_mut};
        while (m_usedBufs == m_numBufs)
            m_cv.wait(lk);

        m_usedBufs += 1;
    }

    fileWrite(
        this, 
        m_file, 
        m_filePos, 
        m_buf.data() - m_bufLen, 
        m_bufLen,
        taskComputeQueue()
    );
    m_filePos += m_bufLen;
    if (m_buf.data() == m_buffers + m_numBufs * m_bufLen) {
        m_buf = {m_buffers, m_bufLen};
    } else {
        m_buf = {m_buf.data(), m_bufLen};
    }
    if (auto left = data.size() - bytes) {
        memcpy((char *) m_buf.data(), data.data() + bytes, left);
        m_buf.remove_prefix(left);
    }
}

//===========================================================================
void FileAppendQueue::onFileWrite(
    int written,
    string_view data,
    int64_t offset,
    FileHandle f
) {
    {
        unique_lock<mutex> lk{m_mut};
        m_usedBufs -= 1;
    }
    m_cv.notify_all();
}
