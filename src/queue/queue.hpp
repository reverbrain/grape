#ifndef __QUEUE_HPP
#define __QUEUE_HPP

#include <map>

#include <msgpack.hpp>

#include <elliptics/session.hpp>

#include <cocaine/framework/logging.hpp>

#include <grape/elliptics_client_state.hpp>
#include <grape/data_array.hpp>
#include <grape/entry_id.hpp>

#include "chunk.hpp"

namespace ioremap { namespace grape {

struct queue_state {
	int chunk_id_push;
	int chunk_id_ack;
};

struct queue_statistics {
	uint64_t push_count;
	uint64_t pop_count;
	uint64_t ack_count;
	uint64_t timeout_count;

	uint64_t state_write_count;

	chunk_stat chunks_popped;
	chunk_stat chunks_pushed;
};

class queue {
	public:
		ELLIPTICS_DISABLE_COPY(queue);

		queue(const std::string &queue_id);

		void initialize(const std::string &config);

		// single entry methods
		void push(const elliptics::data_pointer &d);
		elliptics::data_pointer peek(entry_id *entry_id);
		void ack(const entry_id id);
		elliptics::data_pointer pop();

		// multiple entries methods
		data_array peek(int num);
		void ack(const std::vector<entry_id> &ids);
		data_array pop(int num);

		// content manipulation
		void clear();

		void reply(const ioremap::elliptics::exec_context &context,
				const ioremap::elliptics::data_pointer &d,
				ioremap::elliptics::exec_context::final_state state);
		void final(const ioremap::elliptics::exec_context &context, const ioremap::elliptics::data_pointer &d);

		const std::string &queue_id() const;
		const queue_state &state();
		const queue_statistics &statistics();
		void clear_counters();

	private:
		int m_chunk_max;
		uint64_t m_ack_wait_timeout;
		uint64_t m_timeout_check_period;

		std::string m_queue_id;
		std::string m_queue_state_id;

		elliptics_client_state m_client;
		std::shared_ptr<elliptics::session> m_reply_client;
		std::shared_ptr<elliptics::session> m_data_client;

		queue_state m_state;
		queue_statistics m_statistics;

		std::map<int, shared_chunk> m_chunks;
		std::map<int, shared_chunk> m_wait_ack;
		uint64_t m_last_timeout_check_time;

		void write_state();

		void update_chunk_timeout(int chunk_id, shared_chunk chunk);

		void check_timeouts();
};

}} // namespace ioremap::grape

#endif /* __QUEUE_HPP */
