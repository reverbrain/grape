#include <iostream>
#include <thread>
#include <chrono>
#include <atomic>

#include <gtest/gtest.h>
#include <grape/concurrent-pump.hpp>

TEST(PumpTest, InitWorks) {
	ioremap::grape::concurrent_pump pump;
	pump.concurrency_limit = 10;
}

TEST(PumpTest, QueueRandomSleep) {
	std::atomic_int queue_size {0};
	const int messages_limit {21};
	const int concurrency_limit {25};

	std::atomic_int received_messages;
	std::atomic_int sent_messages;

	std::mutex mutex;
	std::condition_variable condition;

	ioremap::grape::concurrent_pump pump;
	pump.concurrency_limit = concurrency_limit;

	auto reader = [&] {
		received_messages = 0;
		while(true) {
			std::unique_lock<std::mutex> lock{mutex};
			if(received_messages >= messages_limit && queue_size == 0)
				break;
			if(queue_size == 0) continue;

			// read message
			++received_messages;
			--queue_size;
			pump.complete_request();
			lock.unlock();

			std::this_thread::sleep_for(std::chrono::milliseconds{rand() % 500});
		}
	};

	sent_messages = 0;
	auto writer = [&] {
		std::unique_lock<std::mutex> lock{mutex};
		if(sent_messages >= messages_limit) {
			pump.stop();
			pump.complete_request();
			return;
		}
		assert(queue_size < concurrency_limit);

		// write message
		++sent_messages;
		++queue_size;
		lock.unlock();

		std::this_thread::sleep_for(std::chrono::milliseconds{rand() % 500});
	};

	std::thread tReader{reader};
	std::thread tWriter{[&]{ pump.run(writer);}};

	tWriter.join();
	tReader.join();

	EXPECT_EQ(sent_messages, messages_limit);
	EXPECT_EQ(received_messages, messages_limit);
}
