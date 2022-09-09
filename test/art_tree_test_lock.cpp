//
// Created by panrenhua on 2022/6/15.
//

#include <gtest/gtest.h>
#include <random>
#include <unordered_set>
#include <thread>

#include <index/art_key.h>
#include <index/art_tree_lock.h>
#include <index/art_obj_pool.h>

const uint16_t KEY32 = 32;
const uint16_t KEY64 = 64;
const uint16_t KEY128 = 128;
const uint16_t KEY256 = 256;

using namespace Index;

class ART_TEST : public ::testing::Test {
protected:
    ART_Lock<KEY32> *art_tree_32;
    ART_Lock<KEY64> *art_tree_64;
    ART_Lock<KEY128> *art_tree_128;
    ART_Lock<KEY256> *art_tree_256;

    Index::ArtObjPool pool;

    std::default_random_engine gen;

    template<uint16_t KeyLen>
    KEY<KeyLen> GenKey() {
        uint8_t *c = (uint8_t*)malloc(KeyLen);
        for (int i = 0; i < KeyLen; i++) {
            c[i] = gen() % 255;
        }
        return KEY<KeyLen>(c, KeyLen);
    }

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

    void GenRandomSeq(vector<uint64_t>& v, size_t hi) {
        size_t i;
        unordered_set<uint64_t> tmp;
        for (i = 0; i <= hi; i++) {
            int j = rand() % (hi + 1);
            while (tmp.count(j)) {
                j = (j + 1) % (hi + 1);
            }
            tmp.insert(j);
        }
        for (auto& t : tmp) v.push_back(t);
    }

    void SetUp() override {
        art_tree_32 = new ART_Lock<KEY32>(&pool);
        art_tree_64 = new ART_Lock<KEY64>(&pool);
        art_tree_128 = new ART_Lock<KEY128>(&pool);
        art_tree_256 = new ART_Lock<KEY256>(&pool);
    }

    void TearDown() override {

    }
};

TEST_F(ART_TEST, CURRENT_LOOKUP)
{
    vector<KEY<KEY32>> key_list;
    GenOrderedKey<KEY32>(key_list, 100000);

    std::function<void(size_t, size_t)> lookup = [&](size_t i, size_t j) {
        for (size_t k = i; k < j; k++) {
            TID tid;
            bool find = art_tree_32->lookup(key_list[k], tid);
            EXPECT_EQ(find, true);
            EXPECT_EQ(tid, k);
        }
    };

    for (int i = 0; i < 100000; i++) {
        TID tid1 = i;
        art_tree_32->insert(key_list[i], tid1);
    }
    vector<std::thread*> threads;
    size_t ThreadNum = 4;
    size_t CountPerThread = 100000 / ThreadNum;
    for (size_t i = 0; i < ThreadNum; i++) {
        threads.emplace_back(new thread(lookup, i * CountPerThread, (i + 1) * CountPerThread));
    }

    for (size_t i = 0; i < ThreadNum; i++) {
        threads[i]->join();
    }
}

TEST_F(ART_TEST, ORDER_INSERT_TEST)
{
    vector<KEY<KEY32>> key_list;
    GenOrderedKey<KEY32>(key_list, 100000);
    for (int i = 0; i < 100000; i++) {
        TID tid1 = i;
        art_tree_32->insert(key_list[i], tid1);
    }

    for (int i = 0; i < 100000; i++) {
        TID tid2;
        art_tree_32->lookup(key_list[i], tid2);
        EXPECT_EQ(tid2, i);
    }
}

TEST_F(ART_TEST, RANDOM_INSERT_TEST)
{
    const size_t NUM = 256*256*16;
    vector<KEY<KEY32>> key_list;
    GenRandomKey<KEY32>(key_list, NUM);

    std::map<uint64_t, vector<uint64_t>> maps;

    auto now1 = std::chrono::steady_clock::now();
    for (size_t i = 0; i < NUM; i++) {
        auto now = std::chrono::steady_clock::now();
        art_tree_32->insert(key_list[i], i);
        auto end = std::chrono::steady_clock::now();
        auto dis = std::chrono::duration_cast<chrono::nanoseconds>(end - now).count();

        maps[dis / 20].push_back(dis);
    }
    auto end1 = std::chrono::steady_clock::now();
    auto dis1 = std::chrono::duration_cast<chrono::nanoseconds>(end1 - now1).count();

    uint64_t p95 = 0, p99 = 0;
    uint64_t count95 = 0, count99 = 0;
    uint64_t bar95 = NUM * 0.95;
    uint64_t bar99 = NUM * 0.99;
    for (auto& [k, v] : maps) {
        if (count95 + v.size() > bar95) {
            p95 = k;
            break;
        } else {
            count95 += v.size();
        }
    }

    if (maps.count(p95))
        p95 = maps[p95][bar95 - count95];

    for (auto& [k, v] : maps) {
        if (count99 + v.size() > bar99) {
            p99 = k;
            break;
        } else {
            count99 += v.size();
        }
    }

    if (maps.count(p99))
        p99 = maps[p99][bar99 - count99];

    std::cout << "Avg: " << dis1 / NUM << endl;
    std::cout << "P95: " << p95 << endl;
    std::cout << "P99: " << p99 << endl;
    std::cout << "Total: " << dis1 << endl;
}

TEST_F(ART_TEST, CONCURRENT_INSERT_TEST)
{
    const size_t NUM = 256*256*256;
    const size_t ThreadNum = 16;
    const size_t CountPerThread = NUM / ThreadNum;
    vector<KEY<KEY32>> key_list;
    vector<std::thread*> threads;

    GenRandomKey<KEY32>(key_list, NUM);

    std::function<void(size_t, size_t)> insert = [&](size_t i, size_t j) {
        for (size_t k = i; k < j; k++) {
            art_tree_32->insert(key_list[k], k);
        }
    };

    std::function<void(size_t, size_t)> lookup = [&](size_t i, size_t j) {
        for (size_t k = i; k <= j; k++) {
            TID tid;
            bool find = art_tree_32->lookup(key_list[k], tid);
            EXPECT_EQ(find, true);
            EXPECT_EQ(tid, k);
        }
    };

    for (size_t i = 0; i < ThreadNum; i++) {
        threads.emplace_back(new thread(insert, i * CountPerThread, (i + 1) * CountPerThread));
    }

    for (size_t i = 0; i < ThreadNum; i++) {
        threads[i]->join();
    }

    for (size_t j = 0; j < NUM; j++) {
        TID tid2;
        art_tree_32->lookup(key_list[j], tid2);
        EXPECT_EQ(tid2, j);
    }
}

TEST_F(ART_TEST, CONCURRENT_INSERT1_AND_LOOKUP4_TEST)
{
    const size_t NUM = 256*256*64;
    const size_t ThreadNum = 4;
    const size_t CountPerThread = NUM / ThreadNum;
    vector<KEY<KEY32>> key_list;
    vector<std::thread*> threads;

    GenRandomKey<KEY32>(key_list, NUM + CountPerThread);

    std::map<uint64_t, vector<uint64_t>> maps[ThreadNum + 1];

    std::function<void(size_t, size_t)> insertWithout = [&](size_t i, size_t j) {
        for (size_t k = i; k < j; k++) {
            art_tree_32->insert(key_list[k], k);
        }
    };

    std::function<void(size_t, size_t)> insert = [&](size_t i, size_t j) {
        for (size_t k = i; k < j; k++) {
            auto now = std::chrono::steady_clock::now();
            art_tree_32->insert(key_list[k], k);
            auto end = std::chrono::steady_clock::now();
            auto dis = std::chrono::duration_cast<chrono::nanoseconds>(end - now).count();

            maps[ThreadNum][dis / 20].push_back(dis);
        }
    };

    std::function<void(size_t, size_t)> lookup = [&](size_t i, size_t j) {
        int thid = i / CountPerThread;
        for (size_t k = i; k < j; k++) {
            TID tid;
            auto now = std::chrono::steady_clock::now();
            art_tree_32->lookup(key_list[k], tid);
            auto end = std::chrono::steady_clock::now();
            auto dis = std::chrono::duration_cast<chrono::nanoseconds>(end - now).count();

            maps[thid][dis / 20].push_back(dis);
        }
    };

    for (size_t i = 0; i < ThreadNum; i++) {
        threads.emplace_back(new thread(insertWithout, i * CountPerThread, (i + 1) * CountPerThread));
    }

    for (size_t i = 0; i < ThreadNum; i++) {
        threads[i]->join();
    }

    threads.clear();

    auto now1 = std::chrono::steady_clock::now();
    for (size_t i = 0; i < ThreadNum; i++) {
        threads.emplace_back(new thread(lookup, i * CountPerThread, (i + 1) * CountPerThread));
    }
    threads.emplace_back(new thread(insert, ThreadNum * CountPerThread, (ThreadNum + 1) * CountPerThread));

    for (size_t i = 0; i < threads.size(); i++) {
        threads[i]->join();
    }
    auto end1 = std::chrono::steady_clock::now();

    auto dis1 = std::chrono::duration_cast<chrono::nanoseconds>(end1 - now1).count();

    uint64_t p95 = 0, p99 = 0;
    uint64_t count95 = 0, count99 = 0;
    uint64_t bar95 = CountPerThread * 0.95 * (ThreadNum + 1);
    uint64_t bar99 = CountPerThread * 0.99 * (ThreadNum + 1);

    std::map<uint64_t, vector<uint64_t>> total;
    for (size_t i = 0; i <= ThreadNum; i++) {
        for (auto& [k, v] : maps[i]) {
            total[k].insert(total[k].end(), v.begin(), v.end());
        }
    }

    for (auto& [k, v] : total) {
        if (count95 + v.size() > bar95) {
            p95 = k;
            break;
        } else {
            count95 += v.size();
        }
    }

    p95 = total[p95][bar95 - count95];

    for (auto& [k, v] : total) {
        if (count99 + v.size() > bar99) {
            p99 = k;
            break;
        } else {
            count99 += v.size();
        }
    }

    p99 = total[p99][bar99 - count99];

    std::cout << "Avg: " << dis1 / NUM << endl;
    std::cout << "P95: " << p95 << endl;
    std::cout << "P99: " << p99 << endl;
    std::cout << "Total: " << dis1 << endl;
}

TEST_F(ART_TEST, CONCURRENT_INSERT1_AND_LOOKUP8_TEST)
{
    const size_t NUM = 256*256*128;
    const size_t ThreadNum = 8;
    const size_t CountPerThread = NUM / ThreadNum;
    vector<KEY<KEY32>> key_list;
    vector<std::thread*> threads;

    GenRandomKey<KEY32>(key_list, NUM + CountPerThread);

    std::map<uint64_t, vector<uint64_t>> maps[ThreadNum + 1];

    std::function<void(size_t, size_t)> insertWithout = [&](size_t i, size_t j) {
        for (size_t k = i; k < j; k++) {
            art_tree_32->insert(key_list[k], k);
        }
    };

    std::function<void(size_t, size_t)> insert = [&](size_t i, size_t j) {
        for (size_t k = i; k < j; k++) {
            auto now = std::chrono::steady_clock::now();
            art_tree_32->insert(key_list[k], k);
            auto end = std::chrono::steady_clock::now();
            auto dis = std::chrono::duration_cast<chrono::nanoseconds>(end - now).count();

            maps[ThreadNum][dis / 20].push_back(dis);
        }
    };

    std::function<void(size_t, size_t)> lookup = [&](size_t i, size_t j) {
        int thid = i / CountPerThread;
        for (size_t k = i; k < j; k++) {
            TID tid;
            auto now = std::chrono::steady_clock::now();
            art_tree_32->lookup(key_list[k], tid);
            auto end = std::chrono::steady_clock::now();
            auto dis = std::chrono::duration_cast<chrono::nanoseconds>(end - now).count();

            maps[thid][dis / 20].push_back(dis);
        }
    };

    for (size_t i = 0; i < ThreadNum; i++) {
        threads.emplace_back(new thread(insertWithout, i * CountPerThread, (i + 1) * CountPerThread));
    }

    for (size_t i = 0; i < ThreadNum; i++) {
        threads[i]->join();
    }

    threads.clear();

    auto now1 = std::chrono::steady_clock::now();
    for (size_t i = 0; i < ThreadNum; i++) {
        threads.emplace_back(new thread(lookup, i * CountPerThread, (i + 1) * CountPerThread));
    }
    threads.emplace_back(new thread(insert, ThreadNum * CountPerThread, (ThreadNum + 1) * CountPerThread));

    for (size_t i = 0; i < threads.size(); i++) {
        threads[i]->join();
    }
    auto end1 = std::chrono::steady_clock::now();

    auto dis1 = std::chrono::duration_cast<chrono::nanoseconds>(end1 - now1).count();

    uint64_t p95 = 0, p99 = 0;
    uint64_t count95 = 0, count99 = 0;
    uint64_t bar95 = CountPerThread * 0.95 * (ThreadNum + 1);
    uint64_t bar99 = CountPerThread * 0.99 * (ThreadNum + 1);

    std::map<uint64_t, vector<uint64_t>> total;
    for (size_t i = 0; i <= ThreadNum; i++) {
        for (auto& [k, v] : maps[i]) {
            total[k].insert(total[k].end(), v.begin(), v.end());
        }
    }

    for (auto& [k, v] : total) {
        if (count95 + v.size() > bar95) {
            p95 = k;
            break;
        } else {
            count95 += v.size();
        }
    }

    p95 = total[p95][bar95 - count95];

    for (auto& [k, v] : total) {
        if (count99 + v.size() > bar99) {
            p99 = k;
            break;
        } else {
            count99 += v.size();
        }
    }

    p99 = total[p99][bar99 - count99];

    std::cout << "Avg: " << dis1 / NUM << endl;
    std::cout << "P95: " << p95 << endl;
    std::cout << "P99: " << p99 << endl;
    std::cout << "Total: " << dis1 << endl;
}

TEST_F(ART_TEST, CONCURRENT_INSERT1_AND_LOOKUP16_TEST)
{
    const size_t NUM = 256*256*256;
    const size_t ThreadNum = 16;
    const size_t CountPerThread = NUM / ThreadNum;
    vector<KEY<KEY32>> key_list;
    vector<std::thread*> threads;

    GenRandomKey<KEY32>(key_list, NUM + CountPerThread);

    std::map<uint64_t, vector<uint64_t>> maps[ThreadNum + 1];

    std::function<void(size_t, size_t)> insertWithout = [&](size_t i, size_t j) {
        for (size_t k = i; k < j; k++) {
            art_tree_32->insert(key_list[k], k);
        }
    };

    std::function<void(size_t, size_t)> insert = [&](size_t i, size_t j) {
        for (size_t k = i; k < j; k++) {
            auto now = std::chrono::steady_clock::now();
            art_tree_32->insert(key_list[k], k);
            auto end = std::chrono::steady_clock::now();
            auto dis = std::chrono::duration_cast<chrono::nanoseconds>(end - now).count();

            maps[ThreadNum][dis / 20].push_back(dis);
        }
    };

    std::function<void(size_t, size_t)> lookup = [&](size_t i, size_t j) {
        int thid = i / CountPerThread;
        for (size_t k = i; k < j; k++) {
            TID tid;
            auto now = std::chrono::steady_clock::now();
            art_tree_32->lookup(key_list[k], tid);
            auto end = std::chrono::steady_clock::now();
            auto dis = std::chrono::duration_cast<chrono::nanoseconds>(end - now).count();

            maps[thid][dis / 20].push_back(dis);
        }
    };

    for (size_t i = 0; i < ThreadNum; i++) {
        threads.emplace_back(new thread(insertWithout, i * CountPerThread, (i + 1) * CountPerThread));
    }

    for (size_t i = 0; i < ThreadNum; i++) {
        threads[i]->join();
    }

    threads.clear();

    auto now1 = std::chrono::steady_clock::now();
    for (size_t i = 0; i < ThreadNum; i++) {
        threads.emplace_back(new thread(lookup, i * CountPerThread, (i + 1) * CountPerThread));
    }
    threads.emplace_back(new thread(insert, ThreadNum * CountPerThread, (ThreadNum + 1) * CountPerThread));

    for (size_t i = 0; i < threads.size(); i++) {
        threads[i]->join();
    }
    auto end1 = std::chrono::steady_clock::now();

    auto dis1 = std::chrono::duration_cast<chrono::nanoseconds>(end1 - now1).count();

    uint64_t p95 = 0, p99 = 0;
    uint64_t count95 = 0, count99 = 0;
    uint64_t bar95 = CountPerThread * 0.95 * (ThreadNum + 1);
    uint64_t bar99 = CountPerThread * 0.99 * (ThreadNum + 1);

    std::map<uint64_t, vector<uint64_t>> total;
    for (size_t i = 0; i <= ThreadNum; i++) {
        for (auto& [k, v] : maps[i]) {
            total[k].insert(total[k].end(), v.begin(), v.end());
        }
    }

    for (auto& [k, v] : total) {
        if (count95 + v.size() > bar95) {
            p95 = k;
            break;
        } else {
            count95 += v.size();
        }
    }

    p95 = total[p95][bar95 - count95];

    for (auto& [k, v] : total) {
        if (count99 + v.size() > bar99) {
            p99 = k;
            break;
        } else {
            count99 += v.size();
        }
    }

    p99 = total[p99][bar99 - count99];

    std::cout << "Avg: " << dis1 / NUM << endl;
    std::cout << "P95: " << p95 << endl;
    std::cout << "P99: " << p99 << endl;
    std::cout << "Total: " << dis1 << endl;
}

TEST_F(ART_TEST, CONCURRENT_INSERT1_AND_LOOKUP32_TEST)
{
    const size_t NUM = 256*256*1024;
    const size_t ThreadNum = 32;
    const size_t CountPerThread = NUM / ThreadNum;
    vector<KEY<KEY32>> key_list;
    vector<std::thread*> threads;

    GenRandomKey<KEY32>(key_list, NUM + CountPerThread);

    std::map<uint64_t, vector<uint64_t>> maps[ThreadNum + 1];

    std::function<void(size_t, size_t)> insertWithout = [&](size_t i, size_t j) {
        for (size_t k = i; k < j; k++) {
            art_tree_32->insert(key_list[k], k);
        }
    };

    std::function<void(size_t, size_t)> insert = [&](size_t i, size_t j) {
        for (size_t k = i; k < j; k++) {
            auto now = std::chrono::steady_clock::now();
            art_tree_32->insert(key_list[k], k);
            auto end = std::chrono::steady_clock::now();
            auto dis = std::chrono::duration_cast<chrono::nanoseconds>(end - now).count();

            maps[ThreadNum][dis / 20].push_back(dis);
        }
    };

    std::function<void(size_t, size_t)> lookup = [&](size_t i, size_t j) {
        int thid = i / CountPerThread;
        for (size_t k = i; k < j; k++) {
            TID tid;
            auto now = std::chrono::steady_clock::now();
            art_tree_32->lookup(key_list[k], tid);
            auto end = std::chrono::steady_clock::now();
            auto dis = std::chrono::duration_cast<chrono::nanoseconds>(end - now).count();

            maps[thid][dis / 20].push_back(dis);
        }
    };

    for (size_t i = 0; i < ThreadNum; i++) {
        threads.emplace_back(new thread(insertWithout, i * CountPerThread, (i + 1) * CountPerThread));
    }

    for (size_t i = 0; i < ThreadNum; i++) {
        threads[i]->join();
    }

    threads.clear();

    auto now1 = std::chrono::steady_clock::now();
    for (size_t i = 0; i < ThreadNum; i++) {
        threads.emplace_back(new thread(lookup, i * CountPerThread, (i + 1) * CountPerThread));
    }
    threads.emplace_back(new thread(insert, ThreadNum * CountPerThread, (ThreadNum + 1) * CountPerThread));

    for (size_t i = 0; i < threads.size(); i++) {
        threads[i]->join();
    }
    auto end1 = std::chrono::steady_clock::now();

    auto dis1 = std::chrono::duration_cast<chrono::nanoseconds>(end1 - now1).count();

    uint64_t p95 = 0, p99 = 0;
    uint64_t count95 = 0, count99 = 0;
    uint64_t bar95 = CountPerThread * 0.95 * (ThreadNum + 1);
    uint64_t bar99 = CountPerThread * 0.99 * (ThreadNum + 1);

    std::map<uint64_t, vector<uint64_t>> total;
    for (size_t i = 0; i <= ThreadNum; i++) {
        for (auto& [k, v] : maps[i]) {
            total[k].insert(total[k].end(), v.begin(), v.end());
        }
    }

    for (auto& [k, v] : total) {
        if (count95 + v.size() > bar95) {
            p95 = k;
            break;
        } else {
            count95 += v.size();
        }
    }

    p95 = total[p95][bar95 - count95];

    for (auto& [k, v] : total) {
        if (count99 + v.size() > bar99) {
            p99 = k;
            break;
        } else {
            count99 += v.size();
        }
    }

    p99 = total[p99][bar99 - count99];

    std::cout << "Avg: " << dis1 / NUM << endl;
    std::cout << "P95: " << p95 << endl;
    std::cout << "P99: " << p99 << endl;
    std::cout << "Total: " << dis1 << endl;
}

//TEST_F(ART_TEST, ART_OBJ_POOL)
//{
//    Index::ArtObjPool pool = Index::ArtObjPool();
//
//    const size_t NUM1 = 1;
//    const size_t NUM2 = 2;
//    const size_t NUM3 = 3;
//    const size_t NUM4 = 4;
//
//    N* n = nullptr;
//
//    for (size_t i = 0; i < NUM1; i++) pool.gcNode(new N4());
//    for (size_t i = 0; i < NUM2; i++) pool.gcNode(new N16());
//    for (size_t i = 0; i < NUM3; i++) pool.gcNode(new N48());
//    for (size_t i = 0; i < NUM4; i++) pool.gcNode(new N256());
//
//    for (size_t i = 0; i < NUM1; i++) {
//        n = pool.newNode(NT4);
//        EXPECT_NE(n, nullptr);
//    }
//    for (size_t i = 0; i < NUM2; i++) {
//        n = pool.newNode(NT16);
//        EXPECT_NE(n, nullptr);
//    }
//    for (size_t i = 0; i < NUM3; i++) {
//        n = pool.newNode(NT48);
//        EXPECT_NE(n, nullptr);
//    }
//    for (size_t i = 0; i < NUM4; i++) {
//        n = pool.newNode(NT256);
//        EXPECT_NE(n, nullptr);
//    }
//
//    EXPECT_EQ(pool.newNode(NT4), nullptr);
//    EXPECT_EQ(pool.newNode(NT16), nullptr);
//    EXPECT_EQ(pool.newNode(NT48), nullptr);
//    EXPECT_EQ(pool.newNode(NT256), nullptr);
//}

