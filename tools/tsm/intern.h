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
    FileAppendQueue(int numBufs, int maxWrites);
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
    void write_UNLK(std::unique_lock<std::mutex> & lk);

    void onFileWrite(
        int written,
        std::string_view data,
        int64_t offset,
        Dim::FileHandle f
    ) override;

    std::mutex m_mut;
    std::condition_variable m_cv;
    int m_fullBufs = 0;     // ready to be written
    int m_lockedBufs = 0;   // being written
    int m_numBufs;
    int m_maxWrites;
    int m_numWrites = 0;
    char * m_buffers = nullptr;  // aligned to page boundary
    size_t m_bufLen = 0;

    Dim::FileHandle m_file;
    std::string_view m_buf;
    size_t m_filePos = 0;
};
