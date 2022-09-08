//
// Created by panrenhua on 2022/6/15.
//

#include <gtest/gtest.h>
#include <random>
#include <unordered_set>
#include <thread>

#include <index/art_key.h>
#include <index/art_tree.h>
#include <index/art_obj_pool.h>

const uint16_t KEY32 = 32;
const uint16_t KEY64 = 64;
const uint16_t KEY128 = 128;
const uint16_t KEY256 = 256;

using namespace Index;

class ART_TEST : public ::testing::Test {
protected:
    ART<KEY32> *art_tree_32;
    ART<KEY64> *art_tree_64;
    ART<KEY128> *art_tree_128;
    ART<KEY256> *art_tree_256;

    Index::ArtObjPool pool;

    std::default_random_engine gen;

    template<uint16_t KeyLen>
    void GenOrderedKey(vector<KEY<KeyLen>>& v, int count) {
        KEY<KeyLen> r;
        int idx = KeyLen - 1;
        for (int i = 0; i < count; i++) {
            v.push_back(r);

            if (r[idx] == UINT8_MAX) {
                while (r[idx] == UINT8_MAX) {
                    r[idx--] = 0;
                    r[idx] += 1;
                }
            } else {
                r[idx] += 1;
            }
            idx = KeyLen - 1;
        }
    }

    template<uint16_t KeyLen>
    void GenRandomKey(vector<KEY<KeyLen>>& v, uint64_t count) {
        KEY<KeyLen> r;
        for (uint64_t i = 0; i < count; i++) {
            uint64_t num = gen();
            for (int j = 0; j < KeyLen/8; j++) {
                memmove(&r[0] + 8 * j, &num, sizeof(uint64_t));
            }
            memmove(&r[0] + 8, &i, sizeof (uint64_t));

            v.push_back(r);
        }
    }

    void SetUp() override {
        art_tree_32 = new ART<KEY32>(&pool);
        art_tree_64 = new ART<KEY64>(&pool);
        art_tree_128 = new ART<KEY128>(&pool);
        art_tree_256 = new ART<KEY256>(&pool);
    }

    void TearDown() override {

    }
};

TEST_F(ART_TEST, CURRENT_INSERT)
{
    const int NUM = (1024*1024*16);
    vector<KEY<KEY128>> key_list;
    GenOrderedKey<KEY128>(key_list, NUM);

    std::function<void(size_t, size_t)> lookup = [&](size_t i, size_t j) {
        for (size_t k = i; k < j; k++) {
            TID tid;
            art_tree_128->lookup(key_list[k], tid);
        }
    };

    std::function<void(size_t, size_t)> insert = [&](size_t i, size_t j) {
        for (size_t k = i; k < j; k++) {
            art_tree_128->insert(key_list[k], k);
        }
    };

    vector<std::thread*> threads;
    size_t ThreadNum;
    size_t CountPerThread;

//    ThreadNum = 1;
//    CountPerThread = NUM / ThreadNum;
//    auto now1 = std::chrono::steady_clock::now();
//    for (size_t i = 0; i < ThreadNum; i++) {
//        threads.emplace_back(new thread(insert, CountPerThread * i, CountPerThread * (i + 1)));
//    }
//    for (size_t i = 0; i < ThreadNum; i++) {
//        threads[i]->join();
//    }
//    auto end1 = std::chrono::steady_clock::now();
//
//    cout << std::setw(9) << std::chrono::duration<double>(end1 - now1).count() << endl;
    threads.clear();
    delete art_tree_128;
    art_tree_128 = new ART<KEY128>(&pool);

    ThreadNum = 4;
    CountPerThread = NUM / ThreadNum;
    auto now2 = std::chrono::steady_clock::now();
    for (size_t i = 0; i < ThreadNum; i++) {
        threads.emplace_back(new thread(insert, CountPerThread * i, CountPerThread * (i + 1)));
    }
    for (size_t i = 0; i < ThreadNum; i++) {
        threads[i]->join();
    }
    auto end2 = std::chrono::steady_clock::now();

    cout << std::setw(9) << std::chrono::duration<double>(end2 - now2).count() << endl;
    threads.clear();
    delete art_tree_128;
    art_tree_128 = new ART<KEY128>(&pool);

    ThreadNum = 8;
    CountPerThread = NUM / ThreadNum;
    auto now3 = std::chrono::steady_clock::now();
    for (size_t i = 0; i < ThreadNum; i++) {
        threads.emplace_back(new thread(insert, CountPerThread * i, CountPerThread * (i + 1)));
    }
    for (size_t i = 0; i < ThreadNum; i++) {
        threads[i]->join();
    }
    auto end3 = std::chrono::steady_clock::now();

    cout << std::setw(9) << std::chrono::duration<double>(end3 - now3).count() << endl;
    threads.clear();
    delete art_tree_128;
    art_tree_128 = new ART<KEY128>(&pool);

    ThreadNum = 16;
    CountPerThread = NUM / ThreadNum;
    auto now4 = std::chrono::steady_clock::now();
    for (size_t i = 0; i < ThreadNum; i++) {
        threads.emplace_back(new thread(insert, CountPerThread * i, CountPerThread * (i + 1)));
    }
    for (size_t i = 0; i < ThreadNum; i++) {
        threads[i]->join();
    }
    auto end4 = std::chrono::steady_clock::now();

    cout << std::setw(9) << std::chrono::duration<double>(end4 - now4).count() << endl;
    threads.clear();
    delete art_tree_128;
    art_tree_128 = new ART<KEY128>(&pool);

    ThreadNum = 32;
    CountPerThread = NUM / ThreadNum;
    auto now5 = std::chrono::steady_clock::now();
    for (size_t i = 0; i < ThreadNum; i++) {
        threads.emplace_back(new thread(insert, CountPerThread * i, CountPerThread * (i + 1)));
    }
    for (size_t i = 0; i < ThreadNum; i++) {
        threads[i]->join();
    }
    auto end5 = std::chrono::steady_clock::now();

    cout << std::setw(9) << std::chrono::duration<double>(end5 - now5).count() << endl;
    threads.clear();
    delete art_tree_128;
    art_tree_128 = new ART<KEY128>(&pool);
}
