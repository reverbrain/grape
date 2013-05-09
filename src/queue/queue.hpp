#ifndef __QUEUE_HPP
#define __QUEUE_HPP

#include "grape/elliptics_client_state.hpp"

#include <elliptics/cppdef.h>
#include <map>

namespace ioremap { namespace grape {

static std::string lexical_cast(size_t value) {
	if (value == 0) {
		return std::string("0");
	}
	std::string result;
	size_t length = 0;
	size_t calculated = value;
	while (calculated) {
		calculated /= 10;
		++length;
	}
	result.resize(length);
	while (value) {
		--length;
		result[length] = '0' + (value % 10);
		value /= 10;
	}
	return result;
}

struct chunk_entry {
	int		size;
	int		state;
};

struct chunk_disk {
	int			acked, used, max;
	struct chunk_entry	states[];
};

class chunk_ctl {
	public:
		ELLIPTICS_DISABLE_COPY(chunk_ctl);

		chunk_ctl(int max);

		bool push(int size); // returns true when given chunk is full

		std::string &data(void);
		void assign(char *data, int size);

		int used(void);

		struct chunk_entry operator[] (int pos);

	private:
		std::string m_chunk;
		struct chunk_disk *m_ptr;
};

class chunk {
	public:
		ELLIPTICS_DISABLE_COPY(chunk);

		chunk(elliptics::session &session, const std::string &queue_id, int chunk_id, int max);
		~chunk();

		bool push(const elliptics::data_pointer &d); // returns true if chunk is full
		elliptics::data_pointer pop(void);

	private:
		int m_chunk_id;
		std::string m_queue_id;
		elliptics::key m_data_key;
		elliptics::key m_ctl_key;
		elliptics::session m_session_data;
		elliptics::session m_session_ctl;

		int m_pop_position;
		int m_pop_index;

		// whole chunk data is cached here
		// cache is being filled when ::pop is invoked and @m_pop_position is >= than cache size
		elliptics::data_pointer m_chunk_data;

		chunk_ctl m_chunk;
};

typedef std::shared_ptr<chunk> shared_chunk;

class queue {
	public:
		ELLIPTICS_DISABLE_COPY(queue);

		queue(const std::string &config, const std::string &queue_id, int max);

		void push(const elliptics::data_pointer &d);
		elliptics::data_pointer pop(void);

	private:
		int m_chunk_max;

		std::map<int, shared_chunk> m_chunks;

		std::string m_queue_id;

		int m_chunk_id_push;
		int m_chunk_id_pop;

		elliptics_client_state m_client;
};

}} /* namespace ioremap::grape */

#endif /* __QUEUE_HPP */
