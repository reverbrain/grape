#include "queue.hpp"

#include "rapidjson/document.h"
#include "rapidjson/prettywriter.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/filestream.h"

#include <cocaine/json.hpp>
#include <cocaine/framework/logging.hpp>

namespace {
	//DEBUG: only for debug ioremap::elliptics::sessions
	std::shared_ptr<cocaine::framework::logger_t> Log;
	#define LOG_INFO(...) COCAINE_LOG_INFO(Log, __VA_ARGS__)
	#define LOG_ERROR(...) COCAINE_LOG_ERROR(Log, __VA_ARGS__)
	#define LOG_DEBUG(...) COCAINE_LOG_DEBUG(Log, __VA_ARGS__)

	const char *hex(std::string *dest, const char *p, size_t len)
	{
		dest->clear();
		char buf[10] = {0};
		for (size_t i = 0; i < len; ++i) {
			sprintf(buf, "%02x ", (unsigned char)p[i]);
			*dest += buf;
		}

		return dest->c_str();
	}

	// synchronous write to all groups set in client ioremap::elliptics::session
	bool _lowlevel_write(ioremap::elliptics::session *client, const ioremap::elliptics::key &id,
			const ioremap::elliptics::data_pointer &d, bool append = false) {
		try {
			//FIXME: this will drop all preset flags
			if (append) {
				client->set_ioflags(DNET_IO_FLAGS_APPEND);
			} else {
				client->set_ioflags(0);
			}
			client->set_filter(ioremap::elliptics::filters::negative);
			client->set_checker(ioremap::elliptics::checkers::quorum);
			//NOTE: async call turns sync (magically) by using sync_*_result type,
			// actually there is a wait incorporated in convertion from
			// async_*_result to sync_*_result 
			ioremap::elliptics::sync_write_result result = client->write_data(id, d, 0);
			for (const auto &i : result) {
				//FIXME: dnet_server_convert_dnet_addr uses static buffer and so is not thread safe
				LOG_ERROR("singular write error on %s: %s",
					dnet_server_convert_dnet_addr(i.address()),
					i.error().message().c_str()
					);
			}
			return true;
		} catch(const ioremap::elliptics::error &e) {
			// failed to even send commands
			// or failed to reach a quorum on the write 
			LOG_ERROR("write error: %s", e.what());
			return false;
		}
	}

	// synchronous read latest data (choosing from all groups)
	bool _lowlevel_read(ioremap::elliptics::session *client, const ioremap::elliptics::key &id, ioremap::elliptics::data_pointer *d) {
		try {
			client->set_filter(ioremap::elliptics::filters::all);
			client->set_checker(ioremap::elliptics::checkers::quorum);
			//NOTE: async call turns sync (magically) by using sync_*_result type,
			// actually there is a wait incorporated in convertion from
			// async_*_result to sync_*_result 
			//FIXME: read_latest apparently doesn't work if there is ony one group
			// switching to read_data for now
			//sync_read_result result = client->read_latest(id, 0, 0);
			ioremap::elliptics::sync_read_result result = client->read_data(id, 0, 0);
			const ioremap::elliptics::read_result_entry *positive_result = nullptr;
			for (const auto &i : result) {
				if (i.status() != 0) {
					//FIXME: dnet_server_convert_dnet_addr uses static buffer and so is not thread safe
					LOG_ERROR("singular read error on %s: %s",
						dnet_server_convert_dnet_addr(i.address()),
						i.error().message().c_str()
						);
				} else if (positive_result == nullptr) {
					positive_result = &i;
				}
			}
			*d = positive_result->file();
			return true;
		} catch(const ioremap::elliptics::error &e) {
			// failed to even send commands
			// or failed to reach a quorum on the read
			LOG_ERROR("read error: %s", e.what());
			return false;
		}
	}
}

// externally accessible function to set cocaine logger object
void _queue_module_set_logger(std::shared_ptr<cocaine::framework::logger_t> logger) {
	Log = logger;
}

//
// Naming functions
//
std::string make_block_key(const char *prefix, int id, uint64_t block)
{
	const int BUFSIZE = 100;
	char buf[BUFSIZE];
	std::snprintf(buf, BUFSIZE, "%s-%d-%ld", prefix, id, block);
	return buf;
}
std::string make_queue_key(const char *prefix, int id)
{
	const int BUFSIZE = 100;
	char buf[BUFSIZE];
	std::snprintf(buf, BUFSIZE, "%s-%d", prefix, id);
	return buf;
}

//
// Queue receives commands by exec events with its own queue-id as a key.
//
queue_t::queue_t()
	: _id(-1)
	, _prefix("queue")
	, _block_depth(1000)
	//, _block_depth(2)
	, _low_block(1)
	, _low_elem(0)
	, _high_block(0)
	, _high_elem(0)
{}

bool queue_t::_state_save(ioremap::elliptics::session *client) {
	std::string text;
	{
		rapidjson::StringBuffer stream;
		rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(stream);
		rapidjson::Document root;

		root.SetObject();
		root.AddMember("low_block", _low_block, root.GetAllocator());
		root.AddMember("low_elem", _low_elem, root.GetAllocator());
		root.AddMember("high_block", _high_block, root.GetAllocator());
		root.AddMember("high_elem", _high_elem, root.GetAllocator());

		root.Accept(writer);
		std::string text;
		text.assign(stream.GetString(), stream.GetSize());
	}

	ioremap::elliptics::key queue_id(_make_queue_key(_id));
	if (!_lowlevel_write(client, queue_id, ioremap::elliptics::data_pointer(text))) {
		//FIXME: log error and do something
		LOG_ERROR("failed to save state");
		return false;
	}

	return true;
}

bool queue_t::_state_restore(ioremap::elliptics::session *client) {
	ioremap::elliptics::key queue_id(_make_queue_key(_id));
	ioremap::elliptics::data_pointer p;

	if (!_lowlevel_read(client, queue_id, &p)) {
		//FIXME: log error and do something
		return false;
	}
	{
		rapidjson::Document root;

		root.Parse<rapidjson::kParseDefaultFlags, rapidjson::UTF8<>>(p.to_string().c_str());

		//TODO: error checking in queue state reading
		_low_block = root["low_block"].GetInt64();
		_low_elem = root["low_elem"].GetInt64();
		_high_block = root["high_block"].GetInt64();
		_high_elem = root["high_elem"].GetInt64();
	}

	return true;
}

// create new queue
void queue_t::new_id(ioremap::elliptics::session *client, int id) {
	(void)client;
	_id = id;
}

// restoring state of the already existing queue
bool queue_t::existing_id(ioremap::elliptics::session *client, int id) {
	_id = id;

	LOG_INFO("trying to pick state for id '%s'...", _id);
	bool result = _state_restore(client);
	if (_state_restore(client)) {
		//TODO: dump state additionally
		LOG_INFO("found, restoring action as '%s'", _id);
	} else {
		LOG_INFO("not found, starting as '%s' from scratch", _id);
	}

	//TODO: check that queue blocks are really exist?

	return result;
}

void queue_t::push(ioremap::elliptics::session *client, const ioremap::elliptics::data_pointer &d) {
	//NOTE: добавить сохранение локально

	LOG_INFO("push: entering with d: %p", d.data());

	// is it time to switch to the new block?
	bool new_block_needed = ((_high_elem % _block_depth) == 0);

	if (new_block_needed) {
		++_high_block;
		LOG_INFO("switching to new high block: %d", _high_block);
	}

	LOG_INFO("high block id: %s", _make_block_key(_high_block).c_str());

	ioremap::elliptics::key block_id(_make_block_key(_high_block));

	// write or append
	block_t block;
	block.append((const char *)d.data(), d.size());

	LOG_INFO("block %d, %s", _high_block, (new_block_needed ? "write" : "append"));
	ioremap::elliptics::data_pointer writep = ioremap::elliptics::data_pointer::from_raw(block._bytes, block._size);
	bool append = !new_block_needed;
	if (!_lowlevel_write(client, block_id, writep, append)) {
		//FIXME: log error and do something
		LOG_ERROR("block write failed");
		return;
	}

	++_high_elem;

	_state_save(client);

	LOG_INFO("push: leaving with d: %p", d.data());
}

void queue_t::pop(ioremap::elliptics::session *client, ioremap::elliptics::data_pointer *elem_data, size_t *elem_size) {
	if (_low_block >= _high_block && _low_elem == _high_elem) {
		LOG_INFO("queue empty, nothing to pop");
		return;
	}

	ioremap::elliptics::key block_id(_make_block_key(_low_block));
	int start_index = (_low_elem % _block_depth);

	// read
	if (start_index >= _low_block_cached._item_count) {
		LOG_INFO("reading block %d", _low_block);

		if (!_lowlevel_read(client, block_id, &_low_block_data)) {
			//FIXME: log error and do something
			LOG_ERROR("read block %d failed", _low_block);
			return;
		}

		LOG_INFO("read block %d, size %ld", _low_block, _low_block_data.size());
		// std::string tmp;
		// LOG_INFO("block data %s", hex(&tmp, (char*)readp.data(), readp.size()));
		int items = _low_block_cached.set(_low_block_data.data<char>(), _low_block_data.size());
		if (items < 0) {
			// error with block format
			//FIXME: log error and do something
			LOG_ERROR("block data error: bad format (%d items parsed)", -items);
			return;
		}

	}

	// pop
	//NOTE: ASSERT((_low_elem / _block_depth) == _low_block);
	if (start_index >= _low_block_cached._item_count) {
		LOG_ERROR("block inconsistency: requesting element at %d, block size %ld", start_index, _low_block_cached._item_count);
		return;
	}

	LOG_INFO("popping item at %d", start_index);

	const block_t::item *elem = _low_block_cached.at(start_index);
	size_t elem_offset = elem->data - _low_block_cached._bytes;

	*elem_data = _low_block_data.skip(elem_offset);
	*elem_size = elem->size;

	// LOG_INFO("popped data(%d): %s", elem->size, hex(&tmp, (char*)elem_data->data(), elem->size));

	++_low_elem;

	// is it time to drop consumed block?
	bool block_consumed = ((_low_elem % _block_depth) == 0);
	if (block_consumed) {
		LOG_INFO("retire another low block: %d", _low_block);

		_low_block_data = ioremap::elliptics::data_pointer();
		_low_block_cached.set(NULL, 0);

		//FIXME: catch failure exception on remove op
		client->remove(block_id);

		++_low_block;
	}

	_state_save(client);
}

void queue_t::dump_state(std::string *textp) {
	rapidjson::StringBuffer stream;
	rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(stream);
	rapidjson::Document root;

	root.AddMember("id", _id, root.GetAllocator());
	root.AddMember("prefix", _prefix.c_str(), root.GetAllocator());
	root.AddMember("block_size", _block_depth, root.GetAllocator());

	root.AddMember("low_block", _low_block, root.GetAllocator());
	root.AddMember("low_elem", _low_elem, root.GetAllocator());
	root.AddMember("high_block", _high_block, root.GetAllocator());
	root.AddMember("high_elem", _high_elem, root.GetAllocator());

	root.Accept(writer);
	std::string text;
	text.assign(stream.GetString(), stream.GetSize());

	*textp = text;
}

std::string queue_t::_make_block_key(uint64_t block) {
	return make_block_key(_prefix.c_str(), _id, block);
}
std::string queue_t::_make_queue_key(int id) {
	return make_queue_key(_prefix.c_str(), id);
}
