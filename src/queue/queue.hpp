#ifndef QUEUE_HPP__
#define QUEUE_HPP__

#include "block.hpp"

#include <elliptics/cppdef.h>

//
// Queue receives commands by exec events with its own queue-id as a key.
//
struct queue_t
{
	queue_t();

	bool _state_save(ioremap::elliptics::session *client);
	bool _state_restore(ioremap::elliptics::session *client);

	// create new queue
	void new_id(ioremap::elliptics::session *client, int id);

	// restoring state of the already existing queue
	bool existing_id(ioremap::elliptics::session *client, int id);

	void push(ioremap::elliptics::session *client, const ioremap::elliptics::data_pointer &d);
	void pop(ioremap::elliptics::session *client, ioremap::elliptics::data_pointer *elem_data, size_t *elem_size);
	void dump_state(std::string *text);

	std::string _make_block_key(uint64_t block);
	std::string _make_queue_key(int id);

	// config
	int _id;
	std::string _prefix;
	uint64_t _block_depth;

	// running state
	uint64_t _low_block;
	uint64_t _low_elem;
	uint64_t _high_block;
	uint64_t _high_elem;

	// internal details
	// keeping a block from the output end of the queue
	ioremap::elliptics::data_pointer _low_block_data;
	block_t _low_block_cached;
};

std::string make_block_key(const char *prefix, int id, uint64_t block);
std::string make_queue_key(const char *prefix, int id);

#endif //QUEUE_HPP__
