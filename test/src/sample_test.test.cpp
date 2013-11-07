#include <gtest/gtest.h>
#include <iostream>

TEST(MathTest, TwoPlusTwoEqualsFour) {
	EXPECT_EQ(2 + 2, 4);
}

TEST(MathTest, TwoByTwoEqualsFour) {
	EXPECT_EQ(2 * 2, 5) << "WAT???" << std::endl;
}
