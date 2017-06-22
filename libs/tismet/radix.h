// Copyright Glen Knowles 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// radix.h - tismet
#pragma once


/****************************************************************************
*
*   Declarations
*
***/

class RadixDigits {
public:
    RadixDigits() {}
    RadixDigits(
        size_t pageSize, 
        size_t rootOffset,
        size_t pageOffset = 64,
        size_t maxPage = std::numeric_limits<uint32_t>::max()
    );
    void init(
        size_t pageSize, 
        size_t rootOffset,
        size_t pageOffset = 64,
        size_t maxPage = std::numeric_limits<uint32_t>::max()
    );
    size_t convert(int * digits, size_t maxDigits, size_t value) const;
    size_t rootEntries() const;
    size_t pageEntries() const;

private:
    friend std::ostream & operator<< (
        std::ostream & os, 
        const RadixDigits & rd
    );

    size_t m_pageSize{0};   // page size - offset = space for radix list 
    size_t m_rootOffset{0}; // offset, in sizeof(pgno), to radix list on root
    size_t m_pageOffset{0}; // offset to radix list on all non-root pages
    size_t m_maxPage{0};    // maximum page number that is convertable
};

std::ostream & operator<< (std::ostream & os, const RadixDigits & rd);
