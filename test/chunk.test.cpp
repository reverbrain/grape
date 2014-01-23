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

class ChunkMeta : public ::testing::Test {
public:
	static const int meta_size = 100;
	ioremap::grape::chunk_meta meta;
	std::vector<int> chunk_sizes;

	ChunkMeta()
		: meta(meta_size)
	{}

	virtual void SetUp() {
		extern void grape_queue_module_set_logger(std::shared_ptr<cocaine::framework::logger_t>);
		ioremap::elliptics::file_logger log("/dev/stderr", 0);
		grape_queue_module_set_logger(std::make_shared<ioremap::grape::logger_adapter>(log));

		for (int i = 0; i < meta_size; ++i) {
			chunk_sizes.push_back( (rand() % 100) + 1 );
		}
	}
};

TEST_F(ChunkMeta, CheckConstructedMetaSize) {
	ASSERT_EQ(meta.data().size(), sizeof(ioremap::grape::chunk_disk) + meta_size * sizeof(ioremap::grape::chunk_entry));
}

TEST_F(ChunkMeta, CheckMetaPushesCorrectSizes) {
	for (int i = 0; i < meta_size; ++i) {
		meta.push(chunk_sizes[i]);
	}

	for (int i = 0; i < meta_size; ++i) {
		ASSERT_EQ(meta[i].size, chunk_sizes[i]);
	}
}

TEST_F(ChunkMeta, CheckByteOffsetWorks) {
	for (int i = 0; i < meta_size; ++i) {
		meta.push(chunk_sizes[i]);
	}

	uint64_t offset = 0;
	for (int i = 0; i < meta_size; ++i) {
		ASSERT_EQ(offset, meta.byte_offset(i));
		offset += meta[i].size;
	}
}

TEST_F(ChunkMeta, CheckPopThrowsOnEmptyMeta) {
	ASSERT_THROW(meta.pop(), ioremap::elliptics::error);
}

TEST_F(ChunkMeta, CheckMetaIsFull) {
	for (int i = 0; i < meta_size; ++i) {
		meta.push(chunk_sizes[i]);
	}
	ASSERT_TRUE(meta.full());
}

TEST_F(ChunkMeta, CheckMetaIsExhausted) {
	for (int i = 0; i < meta_size; ++i) {
		meta.push(chunk_sizes[i]);
	}
	for (int i = 0; i < meta_size; ++i) {
		meta.pop();
	}
	ASSERT_TRUE(meta.exhausted());
}

TEST_F(ChunkMeta, CheckPopThrowsOnExhaustedMeta) {
	for (int i = 0; i < meta_size; ++i) {
		meta.push(chunk_sizes[i]);
	}
	for (int i = 0; i < meta_size; ++i) {
		meta.pop();
	}

	ASSERT_THROW(meta.pop(), ioremap::elliptics::error);
}

TEST_F(ChunkMeta, CheckAckThrowsOnEmptyMeta) {
	ASSERT_THROW(meta.ack(0, ioremap::grape::chunk_entry::STATE_ACKED), ioremap::elliptics::error);
}

TEST_F(ChunkMeta, CheckAckThrowsOnNotPoppedMeta) {
	for (int i = 0; i < meta_size; ++i) {
		meta.push(chunk_sizes[i]);
	}

	ASSERT_THROW(meta.ack(0, ioremap::grape::chunk_entry::STATE_ACKED), ioremap::elliptics::error);
}

TEST_F(ChunkMeta, CheckMetaIsComplete) {
	for (int i = 0; i < meta_size; ++i) {
		meta.push(chunk_sizes[i]);
	}

	for (int i = 0; i < meta_size; ++i) {
		meta.pop();
	}

	for (int i = 0; i < meta_size; ++i) {
		meta.ack(i, ioremap::grape::chunk_entry::STATE_ACKED);
	}

	ASSERT_TRUE(meta.complete());
}

TEST_F(ChunkMeta, CheckAckNoThrowOnAckedMeta) {
	for (int i = 0; i < meta_size; ++i) {
		meta.push(chunk_sizes[i]);
	}

	for (int i = 0; i < meta_size; ++i) {
		meta.pop();
	}

	for (int i = 0; i < meta_size; ++i) {
		meta.ack(i, ioremap::grape::chunk_entry::STATE_ACKED);
	}

	for (int i = 0; i < meta_size; ++i) {
		ASSERT_NO_THROW(meta.ack(i, ioremap::grape::chunk_entry::STATE_ACKED));
	}
}
