#pragma once

#include <atomic>

#include "art_node.h"

namespace Index {

    struct Node {
        struct Node* next;
    };

    class ArtObjPool {
    private:
        std::atomic<Node*> list4_{nullptr};
        std::atomic<Node*> list16_{nullptr};
        std::atomic<Node*> list48_{nullptr};
        std::atomic<Node*> list256_{nullptr};

    public:
        ~ArtObjPool() {
            Node *head = nullptr;
            uint64_t count = 0;
            while ((head = list4_.load()) != nullptr) {
                list4_.store(head->next);
                delete (N4*)head;
                count++;
            }
            cout << count << endl;
            count = 0;
            while ((head = list16_.load()) != nullptr) {
                list16_.store(head->next);
                delete (N16*)head;
                count++;
            }
            cout << count << endl;
            count = 0;
            while ((head = list48_.load()) != nullptr) {
                list48_.store(head->next);
                delete (N48*)head;
                count++;
            }
            cout << count << endl;
            count = 0;
            while ((head = list256_.load()) != nullptr) {
                list256_.store(head->next);
                delete (N256*)head;
                count++;
            }
            cout << count << endl;
        }

        N* __newNode(type t) {
            switch (t) {
                case NT4: return new N4();
                case NT16: return new N16();
                case NT48: return new N48();
                case NT256: return new N256();
            }
            return nullptr;
        }

        N* newNode(type t) {
            Node *head = nullptr;
            switch (t) {
                case NT4: {
                    do {
                        head = list4_.load(std::memory_order_relaxed);
                    } while (head && !list4_.compare_exchange_weak(head, head->next, std::memory_order_relaxed));
                    if (head) return new(head) N4;
                    break;
                }
                case NT16: {
                    do {
                        head = list16_.load(std::memory_order_relaxed);
                    } while (head && !list16_.compare_exchange_weak(head, head->next, std::memory_order_relaxed));
                    if (head) return new(head) N16;
                    break;
                }
                case NT48: {
                    do {
                        head = list48_.load(std::memory_order_relaxed);
                    } while (head && !list48_.compare_exchange_weak(head, head->next, std::memory_order_relaxed));
                    if (head) return new(head) N48;
                    break;
                }
                case NT256: {
                    do {
                        head = list256_.load(std::memory_order_relaxed);
                    } while (head && !list256_.compare_exchange_weak(head, head->next, std::memory_order_relaxed));
                    if (head) return new(head) N256;
                    break;
                }
            }
            return __newNode(t);
        }

        void gcNode(N* n) {
            Node* head = reinterpret_cast<Node*>(n);
            switch (n->getType()) {
                case NT4: {
                    do {
                        head->next = list4_.load(std::memory_order_relaxed);
                    } while (!list4_.compare_exchange_weak(head->next, head, std::memory_order_relaxed));
                    break;
                }
                case NT16: {
                    do {
                        head->next = list16_.load(std::memory_order_relaxed);
                    } while (!list16_.compare_exchange_weak(head->next, head, std::memory_order_relaxed));
                    break;
                }
                case NT48: {
                    do {
                        head->next = list48_.load(std::memory_order_relaxed);
                    } while (!list48_.compare_exchange_weak(head->next, head, std::memory_order_relaxed));
                    break;
                }
                case NT256: {
                    do {
                        head->next = list256_.load(std::memory_order_relaxed);
                    } while (!list256_.compare_exchange_weak(head->next, head, std::memory_order_relaxed));
                    break;
                }
            }
        }
    };
}
