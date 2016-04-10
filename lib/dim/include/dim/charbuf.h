// charbuf.h - dim services
#pragma once

#include "dim/config.h"
#include "dim/tempheap.h"

#include <list>
#include <string>
#include <utility>  // std::pair

namespace Dim {

class CharBuf : public ITempHeap {
public:
    CharBuf ();
    CharBuf (CharBuf && from);
    ~CharBuf ();

    CharBuf & operator= (char ch) { return assign(ch); }
    CharBuf & operator= (const char s[]) { return assign(s); }
    CharBuf & operator= (const std::string & str) { return assign(str); }
    CharBuf & operator= (CharBuf && buf);
    CharBuf & operator+= (char ch) { return append(1, ch); }
    CharBuf & operator+= (const char s[]) { return append(s); }
    CharBuf & operator+= (const std::string & str) { return append(str); }
    CharBuf & operator+= (const CharBuf & src) { return append(src); }
    CharBuf & assign (char ch) { return assign(&ch, 1); }
    CharBuf & assign (const char s[]);
    CharBuf & assign (const char s[], size_t count);
    CharBuf & assign (const std::string & str, size_t pos = 0, size_t count = -1);
    CharBuf & assign (const CharBuf & src, size_t pos = 0, size_t count = -1);
    char & front ();
    const char & front () const;
    char & back ();
    const char & back () const;
    bool empty () const;
    size_t size () const;
    const char * data () const;
    const char * data (size_t pos, size_t count = -1) const;
    void clear ();
    CharBuf & insert (size_t pos, const char s[]);
    CharBuf & insert (size_t pos, const char s[], size_t count);
    CharBuf & erase (size_t pos = 0, size_t count = -1);
    CharBuf & ltrim (char ch);
    CharBuf & rtrim (char ch);
    void pushBack (char ch);
    void popBack ();
    CharBuf & append (size_t count, char ch);
    CharBuf & append (const char s[]);
    CharBuf & append (const char s[], size_t count);
    CharBuf & append (const std::string & str, size_t pos = 0, size_t count = -1);
    CharBuf & append (const CharBuf & src, size_t pos = 0, size_t count = -1);
    int compare (const char s[], size_t count) const;
    int compare (size_t pos, size_t count, const char src[], size_t srcLen) const;
    int compare (const std::string & str) const;
    int compare (size_t pos, size_t count, const std::string & str) const;
    int compare (const CharBuf & buf) const;
    int compare (size_t pos, size_t count, const CharBuf & buf) const;
    int compare (
        size_t pos, 
        size_t count, 
        const CharBuf & buf, 
        size_t bufPos, 
        size_t bufLen
    ) const;
    CharBuf & replace (size_t pos, size_t count, const char src[]);
    CharBuf & replace (size_t pos, size_t count, const char src[], size_t srcLen);
    CharBuf & replace (
        size_t pos, 
        size_t count, 
        const CharBuf & src, 
        size_t srcPos = 0, 
        size_t srcLen = -1
    );
    size_t copy (char * out, size_t count, size_t pos = 0) const;
    void swap (CharBuf & other);

    // ITempHeap
    char * alloc (size_t bytes, size_t align) override;

private:
    struct Buffer;
    static Buffer * allocBuffer ();
    static Buffer * allocBuffer (size_t reserve);
    std::pair<std::vector<Buffer*>::iterator, int> find (size_t pos);
    CharBuf & erase (std::vector<Buffer*>::iterator it, int pos, int count);

    std::vector<Buffer*> m_buffers;
    int m_lastUsed{0};
    int m_size{0};
};

bool operator== (const CharBuf & left, const std::string & right);
bool operator== (const std::string & left, const CharBuf & right);
bool operator== (const CharBuf & left, const CharBuf & right);

std::string to_string (const CharBuf & buf);

} // namespace
