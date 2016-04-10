// list.h - dim services
#pragma once

#include "dim/config.h"

namespace Dim {

template <typename T>
class List;

template <typename T>
class ListMemberHook {
public:
    bool isLinked () const;
    T * next () const;
    T * prev () const;
    void * reset ();
private:
    friend class List<T>;

    T * m_next{nullptr};
    ListMemberHook * m_prev{nullptr};
};

template <typename T>
class List {
public:
    List ();
    List (List && from);
    ~List ();

    List & operator= (List && from);
    bool operator== (const List & right) const;

    T & front ();
    const T & front () const;
    T & back ();
    const T & back () const;
    bool empty () const;
    int size () const;
    void clear ();
    T * insert (const T * pos, T * value);
    T * insert (const T * pos, T * first, T * last);
    T * insert (const T * pos, List && other);
    T * insertAfter (const T * pos, T * value);
    T * insertAfter (const T * pos, T * first, T * last);
    T * insertAfter (const T * pos, List && other);
    void removeAll ();
    T * remove (T * value);
    T * remove (T * first, T * last);
    void pushBack (T * value);
    void pushBack (List && other);
    T * popBack ();
    void pushFront (T * value);
    void pushFront (List && other);
    T * popFront ();
    void swap (List & other);

private:
    ListMemberHook m_base;
};

} // namespace
