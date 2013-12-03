// #include <iostream>
// #include <thread>
// #include <chrono>
// #include <atomic>

#include <gtest/gtest.h>
#include <grape/data_array.hpp>

using namespace ioremap::grape;

TEST(DataArray, Iteration) {
    data_array a;

    std::vector<std::string> data_entries;

    for (int i = 0; i < 10; ++i) {
        std::string data("entry-");
        data += std::to_string(i);
        data_entries.push_back(data);
    }

    for (int i = 0; i < 10; ++i) {
        a.append(data_entries[i].data(), data_entries[i].size(), entry_id{8, i});
    }

    {
        int n = 0;
        for (auto entry : a) {
            EXPECT_EQ(entry.entry_id.chunk, 8) << "bad chunk number in entry " << n;
            EXPECT_EQ(entry.entry_id.pos, n) << "bad pos number in entry " << n;
            EXPECT_EQ(std::string(entry.data, entry.size), data_entries[n]) << "bad data in entry " << n;
            ++n;
        }
        EXPECT_EQ(n, 10) << "wrong number of entries";
    }
    {
        int n = 0;
        for (data_array::iterator i = a.begin(); i != a.end(); ++i) {
            EXPECT_EQ(i->entry_id.chunk, 8) << "bad chunk number in entry " << n;
            EXPECT_EQ(i->entry_id.pos, n) << "bad pos number in entry " << n;
            EXPECT_EQ(std::string(i->data, i->size), data_entries[n]) << "bad data in entry " << n;
            ++n;
        }
        EXPECT_EQ(n, 10) << "wrong number of entries";
    }
}

TEST(DataArray, EnumerateArray) {
    data_array a;
    {
        a.append(std::string("entry"), entry_id{1, 2});
    }
    std::vector<data_array::entry> entries(a.begin(), a.end());
    EXPECT_EQ(std::string(entries[0].data, entries[0].size), "entry");
}

TEST(DataArray, EnumerateConstArray) {
    data_array a;
    {
        a.append(std::string("entry"), entry_id{1, 2});
    }
    const data_array b = a;
    std::vector<data_array::entry> entries(b.begin(), b.end());
    EXPECT_EQ(std::string(entries[0].data, entries[0].size), "entry");
}
