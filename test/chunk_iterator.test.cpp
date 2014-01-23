// #include <iostream>
// #include <thread>
// #include <chrono>
// #include <atomic>

#include <vector>
#include <cstdlib>
#include <iostream>

#include <gtest/gtest.h>

#include <grape/logger_adapter.hpp>
#include "src/queue/chunk.hpp"

const int meta_size = 100;

class ChunkIterator : public ::testing::Test {
public:
	static const int max_entry_size = 1000;
	ioremap::grape::chunk_meta meta;
	ioremap::grape::iteration state;

	std::vector<int> chunk_sizes;

	ChunkIterator()
		: meta(meta_size)
	{}

	virtual void SetUp() {
		extern void grape_queue_module_set_logger(std::shared_ptr<cocaine::framework::logger_t>);
		ioremap::elliptics::file_logger log("/dev/stderr", 0);
		grape_queue_module_set_logger(std::make_shared<ioremap::grape::logger_adapter>(log));

		chunk_sizes.resize(meta_size);
		for (int i = 0; i < meta_size; ++i) {
			chunk_sizes[i] = (rand() % max_entry_size) + 1;
		}

		for (int i = 0; i < meta_size; ++i) {
			meta.push(chunk_sizes[i]);
		}
	}
};

TEST_F(ChunkIterator, CheckForwardIteratorPassesMeta) {
	ioremap::grape::forward_iterator forward_iter(state, meta);

	for (forward_iter.begin(); !forward_iter.at_end(); forward_iter.advance()) {
		ASSERT_EQ(state.byte_offset, meta.byte_offset(state.entry_index));
		ASSERT_EQ(chunk_sizes[state.entry_index], meta[state.entry_index].size);
	}

	ASSERT_EQ(state.entry_index, meta_size);
}

TEST_F(ChunkIterator, CheckForwardIteratorReusesState) {
	int passed_count = 0;

	while (true) {
		ioremap::grape::forward_iterator forward_iter(state, meta);
		forward_iter.begin();

		ASSERT_EQ(passed_count, state.entry_index);

		if (forward_iter.at_end()) {
			ASSERT_EQ(state.entry_index, meta_size);
			break;
		}

		int skip_size = (rand() % (meta_size - state.entry_index)) + 1;
		for (int step = 0; step < skip_size; ++step) {
			ASSERT_EQ(chunk_sizes[passed_count + step], meta[state.entry_index].size);
			forward_iter.advance();
		}

		passed_count += skip_size;
	}
}

TEST_F(ChunkIterator, CheckReplayIteratorIsAtEndOnEmptyMeta) {
	ioremap::grape::replay_iterator replay_iter(state, meta);

	ASSERT_EQ(state.entry_index, 0);
	ASSERT_TRUE(replay_iter.at_end());
}

TEST_F(ChunkIterator, CheckReplayIteratorIsAtEndOnCompleteMeta) {
	for (int i = 0; i < meta_size; ++i) {
		meta.pop();
		meta.ack(i, ioremap::grape::chunk_entry::STATE_ACKED);
	}

	ioremap::grape::replay_iterator replay_iter(state, meta);
	replay_iter.begin();
	ASSERT_TRUE(replay_iter.at_end());
	ASSERT_EQ(state.entry_index, meta_size);
}

TEST_F(ChunkIterator, CheckReplayIteratorStopsOnUnackedEntry) {
	const int acked_entry_count = meta_size / 2;
	const int nonacked_entry_count = 5;
	for (int i = 0; i < acked_entry_count; ++i) {
		meta.pop();
		meta.ack(i, ioremap::grape::chunk_entry::STATE_ACKED);
	}
	for (int i = 0; i < nonacked_entry_count; ++i) {
		meta.pop();
	}

	ioremap::grape::replay_iterator replay_iter(state, meta);
	replay_iter.begin();
	ASSERT_EQ(state.entry_index, acked_entry_count);
	while (!replay_iter.at_end()) {
		replay_iter.advance();
	}
	ASSERT_TRUE(replay_iter.at_end());
	ASSERT_EQ(state.entry_index, acked_entry_count + nonacked_entry_count);
}

TEST_F(ChunkIterator, CheckReplayIteratorPassesExhausted) {
	for (int i = 0; i < meta_size; ++i) {
		meta.pop();
	}

	ioremap::grape::replay_iterator replay_iter(state, meta);

	int passed_count = 0;
	replay_iter.begin();
	while(!replay_iter.at_end()) {
		ASSERT_EQ(passed_count, state.entry_index);
		ASSERT_EQ(state.byte_offset, meta.byte_offset(state.entry_index));
		ASSERT_EQ(chunk_sizes[passed_count], meta[state.entry_index].size);
		++passed_count;
		replay_iter.advance();
	}

	int expect_passed = meta_size;
	ASSERT_EQ(passed_count, expect_passed);

	ASSERT_EQ(state.entry_index, meta_size);
	ASSERT_EQ(state.entry_index, meta.low_mark());
}

TEST_F(ChunkIterator, CheckForwardAndReplayIteratorsWorkTogether) {
	int passed_count = 0;

	while (true) {
		ioremap::grape::forward_iterator forward_iter(state, meta);
		forward_iter.begin();

		ASSERT_EQ(passed_count, state.entry_index);

		if (forward_iter.at_end()) {
			break;
		}

		int skip_size = (rand() % (meta_size - state.entry_index)) + 1;

		// skip block (contains of next skip_size elements)
		for (int step = 0; step < skip_size; ++step) {
			ASSERT_EQ(passed_count + step, state.entry_index);
			ASSERT_EQ(chunk_sizes[passed_count + step], meta[state.entry_index].size);
			forward_iter.advance();
		}

		// ack all but first element in the block
		for (int step = 1; step < skip_size; ++step) {
			meta.ack(passed_count + step, ioremap::grape::chunk_entry::STATE_ACKED);
		}

		ioremap::grape::replay_iterator replay_iter(state, meta);
		replay_iter.begin();

		// replay_iter should point on first element in the block
		ASSERT_EQ(passed_count, state.entry_index);
		ASSERT_EQ(chunk_sizes[passed_count], meta[state.entry_index].size);

		replay_iter.advance();

		// all other elements in the block have been acked
		ASSERT_TRUE(replay_iter.at_end());

		// ack first element in the block
		meta.ack(passed_count, ioremap::grape::chunk_entry::STATE_ACKED);

		passed_count += skip_size;
	}

	// this way all elements have been acked
	int expect_passed = meta_size;
	ASSERT_EQ(expect_passed, passed_count);
}

