//
// Created by panrh on 2022/6/16.
//

#include "art_node.h"
#include <cassert>

template<typename Small, typename Big>
void N::insertGrow(Small* cur, N* parent, uint8_t pk, uint8_t key, N* new_node) {
    auto* new_cur = new Big();
    new_cur->setPrefix(cur->getPrefix(), cur->getPrefixLen());
    cur->copyTo(new_cur);

    N::setChild(new_cur, key, new_node);
    N::setChild(parent, pk, new_cur);
}

void N::insertAndGrow(N* cur, N* parent, uint8_t pk, uint8_t key, N* new_node) {
    switch (cur->getType()) {
        case NT4: {
            auto n = static_cast<N4 *>(cur);
            insertGrow<N4, N16>(n, parent, pk, key, new_node);
            break;
        }
        case NT16: {
            auto n = static_cast<N16 *>(cur);
            insertGrow<N16, N48>(n, parent, pk, key, new_node);
            break;
        }
        case NT48: {
            auto n = static_cast<N48 *>(cur);
            insertGrow<N48, N256>(n, parent, pk, key, new_node);
            break;
        }
        case NT256:
            assert(false);
    }
}

N* N::getChild(N* cur, const uint8_t k) {
    switch (cur->getType()) {
        case NT4: {
            auto n = static_cast<N4 *>(cur);
            return n->getChild(k);
        }
        case NT16: {
            auto n = static_cast<N16 *>(cur);
            return n->getChild(k);
        }
        case NT48: {
            auto n = static_cast<N48 *>(cur);
            return n->getChild(k);
        }
        case NT256: {
            auto n = static_cast<N256 *>(cur);
            return n->getChild(k);
        }
    }
    return nullptr;
}

void N::setChild(N* cur, const uint8_t k, N *child) {
    switch (cur->getType()) {
        case NT4: {
            auto n = static_cast<N4 *>(cur);
            n->setChild(k, child);
            break;
        }
        case NT16: {
            auto n = static_cast<N16 *>(cur);
            n->setChild(k, child);
            break;
        }
        case NT48: {
            auto n = static_cast<N48 *>(cur);
            n->setChild(k, child);
            break;
        }
        case NT256: {
            auto n = static_cast<N256 *>(cur);
            n->setChild(k, child);
            break;
        }
    }
}

bool N::isFull() const {
    switch (this->type_) {
        case NT4:
            return count_ == 4;
        case NT16:
            return count_ == 16;
        case NT48:
            return count_ == 48;
        case NT256:
            return count_ == 256;
    }
    return false;
}
