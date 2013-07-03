#ifndef __QUEUE_HPP
#define __QUEUE_HPP

#include <map>

#include <msgpack.hpp>

#include <elliptics/session.hpp>

#include <cocaine/framework/logging.hpp>

#include <grape/elliptics_client_state.hpp>
#include <grape/data_array.hpp>
#include <grape/entry_id.hpp>

namespace ioremap { namespace grape {

struct chunk_entry {
	int		size;
	int		state;
};

struct chunk_disk {
	int max;  // size
	int low;  // indicies: low/high marks
	int high;
	int acked;
	struct chunk_entry entries[];
};

class chunk_meta {
	public:
		ELLIPTICS_DISABLE_COPY(chunk_meta);

		chunk_meta(int max);

		// Increases high mark.
		// Returns true when given chunk is full
		bool push(int size);
		// Increases low mark
		void pop();
		// Marks entry at @pos position with @state state.
		// Returns true when given chunk is fully acked
		bool ack(int32_t pos, int state);

		std::string &data();
		void assign(char *data, size_t size);

		int low_mark() const;
		int high_mark() const;
		int acked() const;

		bool full() const;
		bool exhausted() const;
		bool complete() const;

		chunk_entry operator[] (int32_t pos) const;
		uint64_t byte_offset(int32_t pos) const;

	private:
		std::string m_data;
		struct chunk_disk *m_ptr;
};

struct iteration {
	uint64_t byte_offset;
	int entry_index;

	iteration()
		: byte_offset(0), entry_index(0)
	{}
};

struct iterator {
	enum iteration_mode {
		REPLAY = 0,
		FORWARD,
	};
	const iteration_mode mode;
	iteration &state;
	chunk_meta &meta;

	iterator(iteration_mode mode, iteration &state, chunk_meta &meta)
		: mode(mode), state(state), meta(meta)
	{}

	virtual void begin() = 0;
	virtual void advance() = 0;
	virtual bool at_end() = 0;
};

struct forward_iterator : public iterator {

	forward_iterator(iteration &state, chunk_meta &meta)
		: iterator(FORWARD, state, meta)
	{}

	virtual void begin() {
		state.entry_index = meta.low_mark();
		state.byte_offset = meta.byte_offset(state.entry_index);
	}
	virtual void advance() {
		int size = meta[state.entry_index].size;
		// advance low_mark
		meta.pop();
		state.entry_index = meta.low_mark();
		state.byte_offset += size;
	}
	virtual bool at_end() {
		return (state.entry_index >= meta.high_mark());
	}
};

struct replay_iterator : public iterator {

	replay_iterator(iteration &state, chunk_meta &meta)
		: iterator(REPLAY, state, meta)
	{}

	void step() {
		int size = meta[state.entry_index].size;
		++state.entry_index;
		state.byte_offset += size;
	}
	void skip_acked() {
		while (meta[state.entry_index].state == 1) {
			step();
			if (at_end()) {
				break;
			}
		}
	}

	virtual void begin() {
		state.byte_offset = 0;
		state.entry_index = 0;
		skip_acked();
	}
	virtual void advance() {
		skip_acked();
		if (!at_end()) {
			step();
		}
	}
	virtual bool at_end() {
		return (state.entry_index >= meta.low_mark());
	}
};

struct chunk_stat {
	uint64_t write_data;
	uint64_t write_meta;
	uint64_t read;
	uint64_t remove;
	uint64_t push;
	uint64_t pop;
	uint64_t ack;
};

class chunk {
	public:
		ELLIPTICS_DISABLE_COPY(chunk);

		chunk(elliptics::session &session, const std::string &queue_id, int chunk_id, int max);
		~chunk();

		void load_meta();
		const chunk_meta &meta();

		// single entry methods
		bool push(const elliptics::data_pointer &d); // returns true if chunk is full
		elliptics::data_pointer pop(int32_t *pos);
		bool ack(int32_t pos);

		// multiple entries methods
		data_array pop(int num);

		void reset_iteration();

		void remove();

		struct chunk_stat stat(void);
		void add(struct chunk_stat *st);

		int id() const;
		void reset_time(double timeout);
		double get_time(void);

	private:
		int m_chunk_id;
		elliptics::key m_data_key;
		elliptics::key m_meta_key;
		elliptics::session m_session_data;
		elliptics::session m_session_meta;

		struct chunk_stat m_stat;

		iteration iteration_state;
		std::unique_ptr<iterator> iter;

		// whole chunk data is cached here
		// cache is being filled when ::pop is invoked and @m_pop_offset is >= than cache size
		elliptics::data_pointer m_data;

		chunk_meta m_meta;

		double m_fire_time;

		void write_meta();
		void reset_iteration_mode();
		void prepare_iteration();
};

typedef std::shared_ptr<chunk> shared_chunk;

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

		std::string m_queue_id;
		std::string m_queue_state_id;

		elliptics_client_state m_client;

		queue_state m_state;
		queue_statistics m_statistics;

		std::map<int, shared_chunk> m_chunks;
		std::map<int, shared_chunk> m_wait_ack;
		double m_last_timeout_check_time;

		void write_state();

		void update_chunk_timeout(int chunk_id, shared_chunk chunk);

		void check_timeouts();
};

}} // namespace ioremap::grape

#endif /* __QUEUE_HPP */
