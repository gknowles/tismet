// handle.h - dim services
#ifndef DIM_HANDLE_INCLUDED
#define DIM_HANDLE_INCLUDED

#include "dim/config.h"

#include <string>
#include <vector>

struct HandleBase {
    int pos;

    template<typename H> 
    H As () const {
        H handle;
        static_cast<HandleBase&>(handle) = *this;
        return handle;
    }
};

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
    bool Empty () const;
    void * Find (HandleBase handle);

    HandleBase Insert (void * value);
    void * Release (HandleBase handle);
    
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

template<typename H, typename T>
class HandleMap : public HandleMapBase {
public:
    T * Find (H handle) { 
        return static_cast<T*>(HandleMapBase::Find(handle)); 
    }
    void Clear () { 
        for (auto&& ht : *this) 
            Erase(ht);
    }
    H Insert (T * value) { return HandleMapBase::Insert(value).As<H>(); }
    void Erase (H handle) { delete Release(handle); }
    T * Release (H handle) { 
        return static_cast<T*>(HandleMapBase::Release(handle)); 
    }

    Iterator<H,T> begin () { return HandleMapBase::begin<H,T>(); }
    Iterator<H,T> end () { return HandleMapBase::end<H,T>(); }
};

#endif
