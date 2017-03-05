// radix.h - tismet
#pragma once


/****************************************************************************
*
*   Declarations
*
***/

class RadixDigits {
public:
    RadixDigits(
        size_t blkSize, 
        size_t maxPage = std::numeric_limits<uint32_t>::max());
    void convert(int * digits, size_t maxDigits, uint32_t value);

private:
    friend std::ostream & operator<< (
        std::ostream & os, 
        const RadixDigits & rd
    );

    std::vector<uint32_t> m_divs;

    size_t m_blkSize{0};    // bytes available for holding page numbers
    size_t m_offset{0};     // offset, in sizeof(pgno), to radix page list
    size_t m_maxPage{0};    // maximum page number that is convertable
};

std::ostream & operator<< (std::ostream & os, const RadixDigits & rd);
