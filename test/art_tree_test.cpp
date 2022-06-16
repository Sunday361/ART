//
// Created by panrenhua on 2022/6/15.
//

#include <gtest/gtest.h>
#include <random>
#include <art_key.h>
#include <art_tree.h>

const uint16_t KEY64 = 64;
const uint16_t KEY128 = 128;
const uint16_t KEY256 = 256;

class ART_TEST : public ::testing::Test {
protected:
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

    void SetUp() override {
        art_tree_64 = new ART<KEY64>();
        art_tree_128 = new ART<KEY128>();
        art_tree_256 = new ART<KEY256>();
    }

    void TearDown() override {

    }
};

TEST_F(ART_TEST, INSERTTEST)
{

}

TEST_F(ART_TEST, LOOKUPTEST)
{
    KEY<KEY64> k = GenKey<KEY64>();
    TID  tid = 1;
    art_tree_64->lookup(k, tid);
}

TEST_F(ART_TEST, LOOKUPRANGETEST)
{

}

