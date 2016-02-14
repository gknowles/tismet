// handle.h - dim services
#ifndef DIM_HANDLE_INCLUDED
#define DIM_HANDLE_INCLUDED

#include "dim/config.h"

#include <string>
#include <vector>


/****************************************************************************
*
*   Base handle class
*   Clients inherit from this class to make different kinds of handles
*
*   Expected usage:
*   struct HWidget : DimHandleBase {};
*
***/

struct DimHandleBase {
    int pos;

    explicit operator bool () const { return pos != 0; }
    template<typename H> 
    H As () const {
        H handle;
        static_cast<DimHandleBase&>(handle) = *this;
        return handle;
    }
};


/****************************************************************************
*
*   Handle map base type - internal only
*
***/

class DimHandleMapBase {
public:
    template<typename H, typename T> class Iterator;
    struct Node {
        void * value;
        int next;
    };

public:
    DimHandleMapBase ();
    ~DimHandleMapBase ();
    bool Empty () const;
    void * Find (DimHandleBase handle);

    DimHandleBase Insert (void * value);
    void * Release (DimHandleBase handle);
    
    template<typename H, typename T> Iterator<H,T> begin ();
    template<typename H, typename T> Iterator<H,T> end ();

private:
    std::vector<Node> m_values;
    int m_numUsed{0};
    int m_firstFree{0};
};

template<typename H, typename T>
class DimHandleMapBase::Iterator {
    DimHandleMapBase::Node * node{nullptr};
    DimHandleMapBase::Node * base{nullptr};
    DimHandleMapBase::Node * end{nullptr};
public:
    Iterator () {}
    Iterator (Node * base, Node * end) 
        : node{base}, base{base}, end{end} 
    {
        for (; node != end; ++node) {
            if (node->value)
                return;
        }
        node = nullptr;
    }
    bool operator!= (const Iterator & right) const {
        return node != right.node;
    }
    std::pair<H,T*> operator* () { 
        H handle;
        handle.pos = int(node - base);
        return make_pair(handle, static_cast<T*>(node->value)); 
    }
    Iterator & operator++ () {
        node += 1;
        for (; node != end; ++node) {
            if (node->value)
                return *this;
        }
        node = nullptr;
        return *this;
    }
};

//===========================================================================
template<typename H, typename T>
inline auto DimHandleMapBase::begin () -> Iterator<H,T> {
    auto data = m_values.data();
    return Iterator<H,T>(data, data + m_values.size());
}

//===========================================================================
template<typename H, typename T>
inline auto DimHandleMapBase::end () -> Iterator<H,T> {
    return Iterator<H,T>{};
}


/****************************************************************************
*
*   Handle map
*   Container of handles
*
*   Expected usage:
*   DimHandleMap<HWidget, WidgetClass> widgets;
*
***/

template<typename H, typename T>
class DimHandleMap : public DimHandleMapBase {
public:
    T * Find (H handle) { 
        return static_cast<T*>(DimHandleMapBase::Find(handle)); 
    }
    void Clear () { 
        for (auto&& ht : *this) 
            Erase(ht);
    }
    H Insert (T * value) { return DimHandleMapBase::Insert(value).As<H>(); }
    void Erase (H handle) { delete Release(handle); }
    T * Release (H handle) { 
        return static_cast<T*>(DimHandleMapBase::Release(handle)); 
    }

    Iterator<H,T> begin () { return DimHandleMapBase::begin<H,T>(); }
    Iterator<H,T> end () { return DimHandleMapBase::end<H,T>(); }
};

#endif
