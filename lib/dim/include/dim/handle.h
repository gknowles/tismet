// handle.h - dim services
#pragma once

#include "dim/config.h"

#include <utility>
#include <vector>

namespace Dim {


/****************************************************************************
*
*   Base handle class
*   Clients inherit from this class to make different kinds of handles
*
*   Expected usage:
*   struct HWidget : DimHandleBase {};
*
***/

struct HandleBase {
    int pos;

    explicit operator bool () const { return pos != 0; }
    template<typename H> 
    H as () const {
        H handle;
        static_cast<HandleBase&>(handle) = *this;
        return handle;
    }
};


/****************************************************************************
*
*   Handle map base type - internal only
*
***/

class HandleMapBase {
public:
    template<typename H, typename T> class Iterator;
    struct Node {
        void * value;
        int next;
    };

public:
    HandleMapBase ();
    ~HandleMapBase ();
    bool empty () const;
    void * find (HandleBase handle);

    HandleBase insert (void * value);
    void * release (HandleBase handle);
    
    template<typename H, typename T> Iterator<H,T> begin ();
    template<typename H, typename T> Iterator<H,T> end ();

private:
    std::vector<Node> m_values;
    int m_numUsed{0};
    int m_firstFree{0};
};

template<typename H, typename T>
class HandleMapBase::Iterator {
    HandleMapBase::Node * node{nullptr};
    HandleMapBase::Node * base{nullptr};
    HandleMapBase::Node * end{nullptr};
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
inline auto HandleMapBase::begin () -> Iterator<H,T> {
    auto data = m_values.data();
    return Iterator<H,T>(data, data + m_values.size());
}

//===========================================================================
template<typename H, typename T>
inline auto HandleMapBase::end () -> Iterator<H,T> {
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
class HandleMap : public HandleMapBase {
public:
    T * find (H handle) { 
        return static_cast<T*>(HandleMapBase::find(handle)); 
    }
    void clear () { 
        for (auto&& ht : *this) 
            erase(ht);
    }
    H insert (T * value) { return HandleMapBase::insert(value).as<H>(); }
    void erase (H handle) { delete release(handle); }
    T * release (H handle) { 
        return static_cast<T*>(HandleMapBase::release(handle)); 
    }

    Iterator<H,T> begin () { return HandleMapBase::begin<H,T>(); }
    Iterator<H,T> end () { return HandleMapBase::end<H,T>(); }
};

} // namespace
