// charbuf.h - dim services
#ifndef DIM_CHARBUF_INCLUDED
#define DIM_CHARBUF_INCLUDED

#include "dim/config.h"
#include "dim/tempheap.h"

#include <list>

class CharBuf : public IDimTempHeap {
public:
    CharBuf ();
    CharBuf (CharBuf && from);
    ~CharBuf ();

    CharBuf & operator= (char ch) { return Assign(ch); }
    CharBuf & operator= (const char s[]) { return Assign(s); }
    CharBuf & operator= (const std::string & str) { return Assign(str); }
    CharBuf & operator= (CharBuf && buf);
    CharBuf & operator+= (char ch) { return Append(1, ch); }
    CharBuf & operator+= (const char s[]) { return Append(s); }
    CharBuf & operator+= (const std::string & str) { return Append(str); }
    CharBuf & operator+= (const CharBuf & src) { return Append(src); }
    CharBuf & Assign (char ch) { return Assign(&ch, 1); }
    CharBuf & Assign (const char s[]);
    CharBuf & Assign (const char s[], size_t count);
    CharBuf & Assign (const std::string & str, size_t pos = 0, size_t count = -1);
    CharBuf & Assign (const CharBuf & src, size_t pos = 0, size_t count = -1);
    char & Front ();
    const char & Front () const;
    char & Back ();
    const char & Back () const;
    bool Empty () const;
    int Size () const;
    void Clear ();
    CharBuf & Insert (size_t pos, const char s[]);
    CharBuf & Insert (size_t pos, const char s[], size_t count);
    CharBuf & Erase (size_t pos = 0, size_t count = -1);
    void PushBack (char ch);
    void PopBack ();
    CharBuf & Append (size_t count, char ch);
    CharBuf & Append (const char s[]);
    CharBuf & Append (const char s[], size_t count);
    CharBuf & Append (const std::string & str, size_t pos = 0, size_t count = -1);
    CharBuf & Append (const CharBuf & src, size_t pos = 0, size_t count = -1);
    int Compare (const char s[], size_t count) const;
    int Compare (size_t pos, size_t count, const char src[], size_t srcLen) const;
    int Compare (const std::string & str) const;
    int Compare (size_t pos, size_t count, const std::string & str) const;
    int Compare (const CharBuf & buf) const;
    int Compare (size_t pos, size_t count, const CharBuf & buf) const;
    int Compare (
        size_t pos, 
        size_t count, 
        const CharBuf & buf, 
        size_t bufPos, 
        size_t bufLen
    ) const;
    CharBuf & Replace (size_t pos, size_t count, const char src[]);
    CharBuf & Replace (size_t pos, size_t count, const char src[], size_t srcLen);
    CharBuf & Replace (
        size_t pos, 
        size_t count, 
        const CharBuf & src, 
        size_t srcPos = 0, 
        size_t srcLen = -1
    );
    size_t Copy (char * out, size_t count, size_t pos = 0) const;
    void Swap (CharBuf & other);

    // IDimTempHeap
    char * Alloc (size_t bytes, size_t align) override;

private:
    struct Buffer;
    Buffer * AllocBuffer ();
    std::pair<std::list<Buffer>::iterator, int> Find (size_t pos);
    CharBuf & Erase (std::list<Buffer>::iterator it, int pos, int count);

    std::list<Buffer> m_buffers;
    int m_lastUsed{0};
    int m_size{0};
};

bool operator== (const CharBuf & left, const std::string & right);
bool operator== (const std::string & left, const CharBuf & right);
bool operator== (const CharBuf & left, const CharBuf & right);

std::string to_string (const CharBuf & buf);

#endif
