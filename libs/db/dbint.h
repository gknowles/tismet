// Copyright Glen Knowles 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// dbint.h - tismet db
#pragma once


/****************************************************************************
*
*   Declarations
*
***/

template<bool Writable>
class DbFileView {
public:
    ~DbFileView();

    bool open(Dim::FileHandle file, size_t segmentSize, size_t pageSize);
    void close();
    void growToFit(uint32_t pgno);

    const void * rptr(uint32_t pgno) const;

protected:
    using Pointer = std::conditional_t<Writable, char *, const char *>;
    static constexpr Dim::File::ViewMode kMode = Writable 
        ? Dim::File::kViewReadWrite
        : Dim::File::kViewReadOnly;
    size_t minFirstSize() const;
    Pointer ptr(uint32_t pgno) const;

private:
    Dim::FileHandle m_file;
    size_t m_firstViewSize = 0;
    Pointer m_view = nullptr;
    std::vector<Pointer> m_views;
    size_t m_segmentSize = 0;
    size_t m_pageSize = 0;
};

class DbReadView : public DbFileView<false> 
{};

class DbWriteView : public DbFileView<true> {
public:
    void * wptr(uint32_t pgno) const;
};
