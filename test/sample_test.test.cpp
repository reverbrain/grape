#include <gtest/gtest.h>
#include <iostream>

TEST(MathTest, DISABLED_TwoPlusTwoEqualsFour) {
    EXPECT_EQ(2 + 2, 4);
}

TEST(MathTest, DISABLED_TwoByTwoEqualsFour) {
    EXPECT_EQ(2 * 2, 5) << "WAT???" << std::endl;
}
