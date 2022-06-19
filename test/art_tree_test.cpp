//
// Created by panrenhua on 2022/6/15.
//

#include <gtest/gtest.h>
#include <random>
#include <art_key.h>
#include <art_tree.h>

const uint16_t KEY32 = 32;
const uint16_t KEY64 = 64;
const uint16_t KEY128 = 128;
const uint16_t KEY256 = 256;

class ART_TEST : public ::testing::Test {
protected:
    ART<KEY32> *art_tree_32;
    ART<KEY64> *art_tree_64;
    ART<KEY128> *art_tree_128;
    ART<KEY256> *art_tree_256;

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

    void SetUp() override {
        art_tree_32 = new ART<KEY32>();
        art_tree_64 = new ART<KEY64>();
        art_tree_128 = new ART<KEY128>();
        art_tree_256 = new ART<KEY256>();
    }

    void TearDown() override {

    }
};

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
    const int NUM = 100000;
    vector<KEY<KEY32>> key_list;
    vector<int> value_list(NUM);

    GenOrderedKey<KEY32>(key_list, NUM);
    for (int i = 0; i < NUM; i++) {
        TID tid1 = i;
        int r = gen() % NUM;
        art_tree_32->insert(key_list[r], tid1);
        value_list[r] = tid1;
    }

    for (int i = 0; i < NUM; i++) {
        TID tid2;
        if (value_list[i]) {
            art_tree_32->lookup(key_list[i], tid2);
            EXPECT_EQ(tid2, value_list[i]);
        }
    }
}

