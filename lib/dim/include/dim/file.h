// file.h - dim core
#ifndef DIM_FILE_INCLUDED
#define DIM_FILE_INCLUDED

#include "dim/config.h"

#include "dim/types.h"

#include <experimental/filesystem>
#include <limits>

enum DimFileAccess {
    kDimFileWrite,
    kDimFile
};

class IDimFile {
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
    virtual ~IDimFile () {}
};

class IDimFileNotify {
public:
    virtual ~IDimFileNotify () {}

    virtual void OnFileRead (
        char * data, 
        int bytes,
        int64_t offset,
        IDimFile * file
    ) = 0;
    virtual void OnFileEnd (
        int64_t offset, 
        IDimFile * file
    ) = 0;
};

bool DimFileOpen (
    std::unique_ptr<IDimFile> & file,
    const std::experimental::filesystem::path & path,
    IDimFile::OpenMode mode
);

void DimFileRead (
    IDimFileNotify * notify,
    void * outBuffer,
    size_t outBufferSize,
    IDimFile * file,
    int64_t offset = 0,
    int64_t length = 0  // 0 for all
);

#endif
