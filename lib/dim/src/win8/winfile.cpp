// winfile.cpp - dim core - windows platform
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace std::experimental::filesystem;

namespace Dim {


/****************************************************************************
*
*   Incomplete public types
*
***/

/****************************************************************************
*
*   Private declarations
*
***/

namespace {
    class File : public IFile {
    public:
        path m_path;
        HANDLE m_handle;
        OpenMode m_mode;

        virtual ~File ();
    };

    class FileReader : public ITaskNotify {
    public:
        FileReader (
            File * file,
            IFileNotify * notify,
            void * outBuf,
            size_t outBufLen
        );
        void read (int64_t off, int64_t len);

        // ITaskNotify
        void onTask () override;

    private:
        WinOverlappedEvent m_iocpEvt;
        int64_t m_offset{0};
        int64_t m_length{0};

        File * m_file;
        IFileNotify * m_notify;
        char * m_outBuf;
        int m_outBufLen;
    };
}

/****************************************************************************
*
*   Variables
*
***/


/****************************************************************************
*
*   Helpers
*
***/

//===========================================================================
static bool setErrno (int error) {
    _set_doserrno(error);

    switch (error) {
        case ERROR_ALREADY_EXISTS: 
        case ERROR_FILE_EXISTS:
            _set_errno(EEXIST); break;
        case ERROR_FILE_NOT_FOUND: 
            _set_errno(ENOENT); break;
        case ERROR_SHARING_VIOLATION:
            _set_errno(EBUSY); break;
        case ERROR_ACCESS_DENIED:
            _set_errno(EACCES); break;
        default:
            _set_errno(EIO); break;
    }

    return false;
}


/****************************************************************************
*
*   FileReader
*
***/

//===========================================================================
FileReader::FileReader (
    File * file,
    IFileNotify * notify,
    void * outBuf,
    size_t outBufLen
)
    : m_file(file)
    , m_notify(notify)
    , m_outBuf((char *) outBuf)
    , m_outBufLen((int) outBufLen)
{
    m_iocpEvt.notify = this;
    m_iocpEvt.overlapped = {};
}

//===========================================================================
void FileReader::read (int64_t off, int64_t len) {
    m_offset = off;
    m_length = len;
    m_iocpEvt.overlapped = {};
    m_iocpEvt.overlapped.Offset = (DWORD) off;
    m_iocpEvt.overlapped.OffsetHigh = (DWORD) (off >> 32);

    if (!len || len > m_outBufLen)
        len = m_outBufLen;

    if (!ReadFile(
        m_file->m_handle,
        m_outBuf,
        (DWORD) len,
        NULL,
        &m_iocpEvt.overlapped
    )) {
        WinError err;
        if (err != ERROR_IO_PENDING) {
            logMsgError() << "ReadFile (" << m_file->m_path << "): " << err;
            m_notify->onFileEnd(m_offset, m_file);
            delete this;
        }
    }
}

//===========================================================================
void FileReader::onTask () {
    DWORD bytes;
    if (!GetOverlappedResult(NULL, &m_iocpEvt.overlapped, &bytes, false)) {
        WinError err;
        if (err != ERROR_OPERATION_ABORTED)
            logMsgError() << "ReadFile result, " << err;
        m_notify->onFileEnd(m_offset, m_file);
        delete this;
        return;
    }

    bool more = bytes
        ? m_notify->onFileRead(m_outBuf, bytes, m_offset, m_file)
        : false;

    if (!more || m_length && m_length <= bytes) {
        m_notify->onFileEnd(m_offset + bytes, m_file);
        delete this;
    } else {
        read(m_offset + bytes, m_length ? m_length - bytes : 0);
    }
}


/****************************************************************************
*
*   File
*
***/

//===========================================================================
File::~File () {
    if (m_handle != INVALID_HANDLE_VALUE)
        CloseHandle(m_handle);
}


/****************************************************************************
*
*   Internal API
*
***/

//===========================================================================
void iFileInitialize () {
    winIocpInitialize();
}


/****************************************************************************
*
*   Public API
*
***/

//===========================================================================
bool fileOpen (
    unique_ptr<IFile> & out,
    const path & path,
    IFile::OpenMode mode
) {
    using om = IFile::OpenMode;

    out.reset();
    auto file = make_unique<File>();
    file->m_mode = mode;
    file->m_path = path;

    int access = 0;
    if (mode & om::kReadOnly) {
        assert(~mode & om::kReadWrite);
        access = GENERIC_READ;
    } else {
        assert(mode & om::kReadWrite);
        access = GENERIC_READ | GENERIC_WRITE;
    }
    
    int share = 0;
    if (mode & om::kDenyWrite) {
        assert(~mode & om::kDenyNone);
        share = FILE_SHARE_READ;
    } else if (mode & om::kDenyNone) {
        share = FILE_SHARE_READ | FILE_SHARE_WRITE;
    }

    int creation = 0;
    if (mode & om::kCreat) {
        if (mode & om::kExcl) {
            assert(~mode & om::kTrunc);
            creation = CREATE_NEW;
        } else if (mode & om::kTrunc) {
            creation = CREATE_ALWAYS;
        } else {
            creation = OPEN_ALWAYS;
        }
    } else {
        assert(~mode & om::kExcl);
        if (mode & om::kTrunc) {
            creation = TRUNCATE_EXISTING;
        } else {
            creation = OPEN_EXISTING;
        }
    }

    int flags = FILE_FLAG_OVERLAPPED;

    file->m_handle = CreateFileW(
        path.c_str(),
        access,
        share,
        NULL,       // security attributes
        creation,
        flags,
        NULL        // template file
    );
    if (file->m_handle == INVALID_HANDLE_VALUE)
        return setErrno(WinError{});

    if (!winIocpBindHandle(file->m_handle))
        return setErrno(WinError{});

    out = move(file);
    return true;
}

//===========================================================================
void fileRead (
    IFileNotify * notify,
    void * outBuf,
    size_t outBufLen,
    IFile * file,
    int64_t off,
    int64_t len
) {
    auto ptr = new FileReader(
        static_cast<File *>(file), 
        notify, 
        outBuf, 
        outBufLen
    );
    ptr->read(off, len);
}

} // namespace
