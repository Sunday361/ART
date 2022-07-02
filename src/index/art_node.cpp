//
// Created by panrh on 2022/6/16.
//

#include "art_node.h"
#include "art_obj_pool.h"

namespace Index {

    template<typename Small, typename Big>
    void N::InsertGrow(Small *small, Big *big, N *parent, uint8_t pk,
                       uint8_t key, N *new_node) {
        big->SetPrefix(small->GetPrefix(), small->GetPrefixLen());
        small->CopyTo(big);

        N::SetChild(big, key, new_node);
        N::ChangeChild(parent, pk, big);
    }

    void N::InsertAndGrow(N *cur, N *parent, uint8_t pk, uint8_t key, N *new_node, Index::ArtObjPool *pool) {
        switch (cur->GetType()) {
            case NT4: {
                auto small = static_cast<N4 *>(cur);
                auto big = static_cast<N16 *>(pool->NewNode(NT16));
                InsertGrow<N4, N16>(small, big, parent, pk, key, new_node);
                break;
            }
            case NT16: {
                auto small = static_cast<N16 *>(cur);
                auto big = static_cast<N48 *>(pool->NewNode(NT48));
                InsertGrow<N16, N48>(small, big, parent, pk, key, new_node);
                break;
            }
            case NT48: {
                auto small = static_cast<N48 *>(cur);
                auto big = static_cast<N256 *>(pool->NewNode(NT256));
                InsertGrow<N48, N256>(small, big, parent, pk, key, new_node);
                break;
            }
            case NT256:
                __builtin_unreachable();
        }
    }

    N *N::GetChild(N *cur, const uint8_t k) {
        switch (cur->GetType()) {
            case NT4: {
                auto n = static_cast<N4 *>(cur);
                return n->GetChild(k);
            }
            case NT16: {
                auto n = static_cast<N16 *>(cur);
                return n->GetChild(k);
            }
            case NT48: {
                auto n = static_cast<N48 *>(cur);
                return n->GetChild(k);
            }
            case NT256: {
                auto n = static_cast<N256 *>(cur);
                return n->GetChild(k);
            }
        }
        __builtin_unreachable();
    }

    void N::SetChild(N *cur, const uint8_t k, N *child) {
        switch (cur->getType()) {
            case NT4: {
                auto n = static_cast<N4 *>(cur);
                n->SetChild(k, child);
                break;
            }
            case NT16: {
                auto n = static_cast<N16 *>(cur);
                n->SetChild(k, child);
                break;
            }
            case NT48: {
                auto n = static_cast<N48 *>(cur);
                n->SetChild(k, child);
                break;
            }
            case NT256: {
                auto n = static_cast<N256 *>(cur);
                n->SetChild(k, child);
                break;
            }
        }
    }

    bool N::IsFull() const {
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

    bool N::ChangeChild(N *cur, const uint8_t k, N *child) {
        switch (cur->getType()) {
            case NT4: {
                auto n = static_cast<N4 *>(cur);
                return n->ChangeChild(k, child);
            }
            case NT16: {
                auto n = static_cast<N16 *>(cur);
                return n->ChangeChild(k, child);
            }
            case NT48: {
                auto n = static_cast<N48 *>(cur);
                return n->ChangeChild(k, child);
            }
            case NT256: {
                auto n = static_cast<N256 *>(cur);
                return n->ChangeChild(k, child);
            }
        }
        return false;
    }
}