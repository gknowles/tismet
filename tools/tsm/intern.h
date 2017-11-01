// Copyright Glen Knowles 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// intern.h - tsm
#pragma once


/****************************************************************************
*
*   Declarations
*
***/

class FileAppendQueue {
public:
    FileAppendQueue(int numBuf);
    ~FileAppendQueue();

    explicit operator bool() const { return (bool) m_file; }

    enum OpenExisting {
        kFail,
        kAppend,
        kTrunc,
    };
    bool open(std::string_view path, OpenExisting mode);
    bool attach(Dim::FileHandle f);
    void close();

    void append(std::string_view data);

private:
    char * m_buffers = nullptr;  // aligned to page boundary
    size_t m_bufLen;
    int m_numBufs;

    Dim::FileHandle m_file;
    std::string_view m_buf;
    size_t m_filePos;
};
