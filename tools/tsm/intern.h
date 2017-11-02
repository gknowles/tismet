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

class FileAppendQueue : Dim::IFileWriteNotify {
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
    void onFileWrite(
        int written,
        std::string_view data,
        int64_t offset,
        Dim::FileHandle f
    ) override;

    std::mutex m_mut;
    std::condition_variable m_cv;
    int m_usedBufs = 0;
    int m_numBufs;
    char * m_buffers = nullptr;  // aligned to page boundary
    size_t m_bufLen = 0;

    Dim::FileHandle m_file;
    std::string_view m_buf;
    size_t m_filePos = 0;
};
