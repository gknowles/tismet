// file.h - dim core
#pragma once

#include "dim/config.h"

#include "dim/types.h"

#include <experimental/filesystem>
#include <memory>

namespace Dim {

class IFile {
public:
    enum OpenMode {
        kReadOnly   = 0x01,
        kReadWrite  = 0x02,
        kCreat      = 0x04, // create if not exist
        kExcl       = 0x08, // fail if already exists
        kTrunc      = 0x10, // truncate if already exists
        kDenyWrite  = 0x20,
        kDenyNone   = 0x40,
    };

public:
    virtual ~IFile () {}
};

class IFileNotify {
public:
    virtual ~IFileNotify () {}

    // return false to prevent more reads, otherwise reads continue until the
    // requested length has been received.
    virtual bool onFileRead (
        char * data, 
        int bytes,
        int64_t offset,
        IFile * file
    ) = 0;

    virtual void onFileEnd (
        int64_t offset, 
        IFile * file
    ) = 0;
};

bool fileOpen (
    std::unique_ptr<IFile> & file,
    const std::experimental::filesystem::path & path,
    IFile::OpenMode mode
);

void fileRead (
    IFileNotify * notify,
    void * outBuffer,
    size_t outBufferSize,
    IFile * file,
    int64_t offset = 0,
    int64_t length = 0  // 0 to read until the end
);

} // namespace
