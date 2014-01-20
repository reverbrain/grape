#include <unordered_set>

#include <cocaine/framework/logging.hpp>

#include "grape/rapidjson/document.h"
#include "grape/rapidjson/prettywriter.h"
#include "grape/rapidjson/stringbuffer.h"
#include "grape/rapidjson/filestream.h"

#include "queue.hpp"

namespace {
	//DEBUG: only for debug ioremap::elliptics::sessions
	std::shared_ptr<cocaine::framework::logger_t> __grape_queue_private_log;
	#define LOG_INFO(...) COCAINE_LOG_INFO(__grape_queue_private_log, __VA_ARGS__)
	#define LOG_ERROR(...) COCAINE_LOG_ERROR(__grape_queue_private_log, __VA_ARGS__)
	#define LOG_DEBUG(...) COCAINE_LOG_DEBUG(__grape_queue_private_log, __VA_ARGS__)
}

// externally accessible function to set cocaine logger object
void grape_queue_module_set_logger(std::shared_ptr<cocaine::framework::logger_t> logger) {
	__grape_queue_private_log = logger;
}

// externally accessible function to get cocaine logger object
std::shared_ptr<cocaine::framework::logger_t> grape_queue_module_get_logger() {
	return __grape_queue_private_log;
}

namespace ioremap { namespace grape {

namespace defaults {
	const int MAX_CHUNK_SIZE = 10000;
	const uint64_t ACK_WAIT_TIMEOUT = 5 * 1000000; // microseconds
	const uint64_t TIMEOUT_CHECK_PERIOD = 1 * 1000000; // microseconds
}

queue::queue(const std::string &queue_id)
	: m_chunk_max(defaults::MAX_CHUNK_SIZE)
	, m_ack_wait_timeout(defaults::ACK_WAIT_TIMEOUT)
	, m_timeout_check_period(defaults::TIMEOUT_CHECK_PERIOD)
	, m_queue_id(queue_id)
	, m_queue_state_id(m_queue_id + ".state")
	, m_last_timeout_check_time(0)
{
}

void queue::initialize(const std::string &config)
{
	memset(&m_statistics, 0, sizeof(m_statistics));

	rapidjson::Document doc;
	m_client_proto = elliptics_client_state::create(config, doc);

	m_reply_client = std::make_shared<elliptics::session>(m_client_proto.create_session());
	m_data_client = std::make_shared<elliptics::session>(m_reply_client->clone());
	m_data_client->set_ioflags(DNET_IO_FLAGS_NOCSUM | DNET_IO_FLAGS_OVERWRITE);

	std::vector<int> storage_groups;
	read_groups_array(&storage_groups, "storage-groups", doc);
	if (!storage_groups.empty()) {
		m_data_client->set_groups(storage_groups);
	}

	if (doc.HasMember("wait-timeout")) {
		int timeout = doc["wait-timeout"].GetInt();
		m_reply_client->set_timeout(timeout);
		m_data_client->set_timeout(timeout);
	}

	LOG_INFO("%s, init: elliptics client created", m_queue_id.c_str());

	if (doc.HasMember("chunk-max-size")) {
		m_chunk_max = doc["chunk-max-size"].GetInt();
	}

	if (doc.HasMember("ack-wait-timeout")) {
		// config value in seconds
		m_ack_wait_timeout = 1000000 * doc["ack-wait-timeout"].GetInt();
	}

	if (doc.HasMember("timeout-check-period")) {
		// config value in seconds
		m_timeout_check_period = 1000000 * doc["timeout-check-period"].GetInt();
	}

	memset(&m_state, 0, sizeof(m_state));

	try {
		ioremap::elliptics::data_pointer d = m_data_client->read_data(m_queue_state_id, 0, 0).get_one().file();
		auto *state = d.data<queue_state>();

		m_state.chunk_id_push = state->chunk_id_push;
		m_state.chunk_id_ack = state->chunk_id_ack;

		LOG_INFO("%s, init: queue meta found: chunk_id_ack %d, chunk_id_push %d",
				m_queue_id.c_str(),
				m_state.chunk_id_ack, m_state.chunk_id_push
				);

	} catch (const ioremap::elliptics::not_found_error &) {
		LOG_INFO("%s, init: no queue meta found, starting in pristine state", m_queue_id.c_str());
	}

	// load metadata of existing chunk into memory
	for (int i = m_state.chunk_id_ack; i <= m_state.chunk_id_push; ++i) {
		auto p = std::make_shared<chunk>(*m_data_client.get(), m_queue_id, i, m_chunk_max);
		m_chunks.insert(std::make_pair(i, p));
		if (!p->load_meta() && i < m_state.chunk_id_push) {
			LOG_ERROR("%s, init: failed to read middle chunk meta data, can't proceed, exiting", m_queue_id.c_str());
			ioremap::elliptics::throw_error(-ENODATA, "middle chunk %d meta data is missing", i);
		}
	}

	LOG_INFO("%s, init: queue started", m_queue_id.c_str());
}

void queue::write_state()
{
	m_data_client->write_data(m_queue_state_id,
			ioremap::elliptics::data_pointer::from_raw(&m_state, sizeof(queue_state)),
			0);

	m_statistics.state_write_count++;
}

void queue::clear()
{
	LOG_INFO("%s, clearing queue", m_queue_id.c_str());

	LOG_INFO("%s, dropping statistics", m_queue_id.c_str());
	clear_counters();

	LOG_INFO("%s, erasing state", m_queue_id.c_str());
	queue_state state = m_state;
	memset(&m_state, 0, sizeof(m_state));
	write_state();

	LOG_INFO("%s, removing chunks, from %d to %d", m_queue_id.c_str(), state.chunk_id_ack, state.chunk_id_push);
	std::map<int, shared_chunk> remove_list;
	remove_list.swap(m_chunks);
	remove_list.insert(m_wait_ack.cbegin(), m_wait_ack.cend());
	m_wait_ack.clear();

	for (auto i = remove_list.cbegin(); i != remove_list.cend(); ++i) {
		auto chunk = i->second;
		chunk->remove();
	}

	remove_list.clear();

	LOG_INFO("%s, queue cleared", m_queue_id.c_str());
}

void queue::push(const ioremap::elliptics::data_pointer &d)
{
	auto found = m_chunks.find(m_state.chunk_id_push);
	if (found == m_chunks.end()) {
		// create new empty chunk
		auto p = std::make_shared<chunk>(*m_data_client.get(), m_queue_id, m_state.chunk_id_push, m_chunk_max);
		auto inserted = m_chunks.insert(std::make_pair(m_state.chunk_id_push, p));

		found = inserted.first;
	}

	int chunk_id = found->first;
	auto chunk = found->second;

	if (chunk->push(d)) {
		LOG_INFO("%s, chunk %d filled", m_queue_id.c_str(), chunk_id);

		++m_state.chunk_id_push;
		write_state();

		chunk->add(&m_statistics.chunks_pushed);
	}

	++m_statistics.push_count;
}

ioremap::elliptics::data_pointer queue::peek(entry_id *entry_id)
{
	check_timeouts();

	ioremap::elliptics::data_pointer d;
	*entry_id = {-1, -1};

	while (true) {
		auto found = m_chunks.begin();
		if (found == m_chunks.end()) {
			break;
		}

		int chunk_id = found->first;
		auto chunk = found->second;

		entry_id->chunk = chunk_id;
		d = chunk->pop(&entry_id->pos);

		LOG_INFO("%s, popping entry %d-%d (%ld)'%s'", m_queue_id.c_str(), entry_id->chunk, entry_id->pos, d.size(), d.to_string());
		if (!d.empty()) {
			m_statistics.pop_count++;

			// set or reset timeout timer for the chunk
			update_chunk_timeout(chunk_id, chunk);

		}

		if (chunk_id == m_state.chunk_id_push) {
			break;
		}

		if (chunk->expect_no_more()) {
			//FIXME: this could happen to be called many times for the same chunk
			// (between queue restarts or because of chunk replaying)
			chunk->add(&m_statistics.chunks_popped);

			LOG_INFO("%s, chunk %d exhausted, dropped from the popping line", m_queue_id.c_str(), chunk_id);

			// drop chunk from the pop list
			m_chunks.erase(found);
		} else if (d.empty()) {
			// this is error condition: middle chunk must give some data but gives none
			break;
		}

		break;
	}

	return d;
}

void queue::update_chunk_timeout(int chunk_id, shared_chunk chunk)
{
	// add chunk to the waiting list and postpone its deadline time
	m_wait_ack.insert({chunk_id, chunk});
	//TODO: make acking timeout value configurable
	chunk->reset_time(m_ack_wait_timeout);
}

void queue::check_timeouts()
{
	uint64_t now = microseconds_now();

	// check no more then once in 1 second
	//TODO: make timeout check interval configurable
	if ((now - m_last_timeout_check_time) < m_timeout_check_period) {
		return;
	}
	m_last_timeout_check_time = now;

	LOG_INFO("%s, checking timeouts: %ld waiting chunks", m_queue_id.c_str(), m_wait_ack.size());

	auto i = m_wait_ack.begin();
	while (i != m_wait_ack.end()) {
		int chunk_id = i->first;
		auto chunk = i->second;

		if (now < chunk->get_time()) {
			++i;
			continue;
		}

		// Time passed but chunk still is not complete
		// so we must replay unacked items from it.
		LOG_ERROR("%s, chunk %d timed out, returning back to the popping line (due time was %ldus ago)",
				m_queue_id.c_str(),
				chunk_id,
				now - chunk->get_time()
				);

		// There are two cases when chunk can still be in m_chunks
		// while experiencing a timeout:
		// 1) if it's a single chunk in the queue (and serves both
		//    as a push and a pop/ack target)
		// 2) if iteration of this chunk was preempted by other timed out chunk
		//    and then later this chunk timed out in its turn
		// Timeout require switching chunk's iteration into the replay mode.

		auto inserted = m_chunks.insert({chunk_id, chunk});
		if (!inserted.second) {
			LOG_INFO("%s, chunk %d is already in popping line", m_queue_id.c_str(), chunk_id);
		} else {
			LOG_INFO("%s, chunk %d inserted back to the popping line anew", m_queue_id.c_str(), chunk_id);
		}
		chunk->reset_iteration();

		auto hold = i;
		++i;
		m_wait_ack.erase(hold);

		// number of popped but still unacked entries
		m_statistics.timeout_count += (chunk->meta().low_mark() - chunk->meta().acked());
	}
}

void queue::ack(const entry_id id)
{
	auto found = m_wait_ack.find(id.chunk);
	if (found == m_wait_ack.end()) {
		LOG_ERROR("%s, ack for chunk %d (pos %d) which is not in waiting list", m_queue_id.c_str(), id.chunk, id.pos);
		return;
	}

	auto chunk = found->second;
	chunk->ack(id.pos, true);
	if (chunk->meta().acked() == chunk->meta().low_mark()) {
		// Real end of the chunk's lifespan, all popped entries are acked

		m_wait_ack.erase(found);

		chunk->add(&m_statistics.chunks_popped);

		// Chunk would be uncomplete here only if its the only chunk in the queue
		// (filled partially and serving both as a push and a pop/ack target)
		if (chunk->meta().complete()) {
			chunk->remove();
			LOG_INFO("%s, ack, chunk %d complete", m_queue_id.c_str(), id.chunk);
		}

		// Set chunk_id_ack to the lowest active chunk
		//NOTE: its important to have m_chunks and m_wait_ack both sorted
		m_state.chunk_id_ack = m_state.chunk_id_push;
		if (!m_chunks.empty()) {
			m_state.chunk_id_ack = std::min(m_state.chunk_id_ack, m_chunks.begin()->first);
		}
		if (!m_wait_ack.empty()) {
			m_state.chunk_id_ack = std::min(m_state.chunk_id_ack, m_wait_ack.begin()->first);
		}

		write_state();
	}

	++m_statistics.ack_count;
}

ioremap::elliptics::data_pointer queue::pop()
{
	entry_id id;
	ioremap::elliptics::data_pointer d = peek(&id);
	if (!d.empty()) {
		ack(id);
	}
	return d;
}

data_array queue::pop(int num)
{
	data_array d = peek(num);
	if (!d.empty()) {
		ack(d.ids());
	}
	return d;
}

data_array queue::peek(int num)
{
	check_timeouts();

	data_array ret;

	while (num > 0) {
		auto found = m_chunks.begin();
		if (found == m_chunks.end()) {
			break;
		}

		int chunk_id = found->first;
		auto chunk = found->second;

		data_array d = chunk->pop(num);
		LOG_INFO("%s, chunk %d, popping %d entries", m_queue_id.c_str(), chunk_id, d.sizes().size());
		//DEBUG
		for (const auto &i : d.ids()) {
			LOG_INFO("%s, chunk %d, pos %d, state %d", m_queue_id.c_str(), i.chunk, i.pos, chunk->meta()[i.pos].state);
		}

		if (!d.empty()) {
			m_statistics.pop_count += d.sizes().size();

			ret.extend(d);

			// set or reset timeout timer for the chunk
			update_chunk_timeout(chunk_id, chunk);

		}

		if (chunk_id == m_state.chunk_id_push) {
			break;
		}

		if (chunk->expect_no_more()) {
			//FIXME: this could happen to be called many times for the same chunk
			// (between queue restarts or because of chunk replaying)
			chunk->add(&m_statistics.chunks_popped);

			LOG_INFO("%s, chunk %d exhausted, dropped from the popping line", m_queue_id.c_str(), chunk_id);

			// drop chunk from the pop list
			m_chunks.erase(found);

		} else if (d.empty()) {
			// this is error condition: middle chunk must give some data but gives none
			break;
		}

		num -= d.sizes().size();
	}

	return ret;
}

void queue::ack(const std::vector<entry_id> &ids)
{
	// Collect affected chunk set
	std::unordered_set<shared_chunk> affected_chunks;
	auto found = m_wait_ack.end();

	for (const auto &id : ids) {
		// sequential case optimization
		if (found == m_wait_ack.end() || found->second->id() != id.chunk) {
			found = m_wait_ack.find(id.chunk);
			if (found == m_wait_ack.end()) {
				LOG_ERROR("%s, ack for chunk %d (pos %d) which is not in waiting list", m_queue_id.c_str(), id.chunk, id.pos);
				continue;
			}
		}
		auto chunk = found->second;

		affected_chunks.insert(chunk);

		// ack without saving meta
		LOG_INFO("%s, ack, chunk %d, pos %d, state %d", m_queue_id.c_str(), id.chunk, id.pos, chunk->meta()[id.pos].state);
		chunk->ack(id.pos, false);

		if (chunk->meta().acked() == chunk->meta().low_mark()) {
			// Real end of the chunk's lifespan, all popped entries are acked

			m_wait_ack.erase(found);
			found = m_wait_ack.end();

			// Set chunk_id_ack to the lowest active chunk
			//NOTE: its important to have m_chunks and m_wait_ack both sorted
			m_state.chunk_id_ack = m_state.chunk_id_push;
			if (!m_chunks.empty()) {
				m_state.chunk_id_ack = std::min(m_state.chunk_id_ack, m_chunks.begin()->first);
			}
			if (!m_wait_ack.empty()) {
				m_state.chunk_id_ack = std::min(m_state.chunk_id_ack, m_wait_ack.begin()->first);
			}

			// writing state before any other external actions to ensure state correctness
			write_state();

			// Chunk would be uncomplete here only if its the only chunk in the queue
			// (filled partially and serving both as a push and a pop/ack target)
			if (chunk->meta().complete()) {
				chunk->remove();
				affected_chunks.erase(chunk);
				LOG_INFO("%s, ack, chunk %d complete", m_queue_id.c_str(), id.chunk);
			}

			chunk->add(&m_statistics.chunks_popped);
		}

		affected_chunks.insert(chunk);

		++m_statistics.ack_count;
	}

	// then save meta for all affected chunks
	for (const auto &chunk : affected_chunks) {
		LOG_INFO("%s, ack, chunk %d, write meta, acked %d, low %d", m_queue_id.c_str(), chunk->id(), chunk->meta().acked(), chunk->meta().low_mark());
		chunk->write_meta();
	}
}

void queue::reply(cocaine::framework::response_ptr response, const ioremap::elliptics::exec_context &context,
		const ioremap::elliptics::argument_data &d, ioremap::elliptics::exec_context::final_state state)
{
	m_reply_client->reply(context, d, state).connect(
		ioremap::elliptics::async_reply_result::result_function(),
		[this, response] (const ioremap::elliptics::error_info &error) {
			if (error) {
				LOG_ERROR("%s, reply error: %s", m_queue_id.c_str(), error.message().c_str());
			} else {
				//LOG_INFO("%s, replied", m_queue_id.c_str());
			}
		}
	);
}

void queue::final(cocaine::framework::response_ptr response, const ioremap::elliptics::exec_context &context, const ioremap::elliptics::argument_data &d)
{
	reply(response, context, d, ioremap::elliptics::exec_context::final);
}

const std::string &queue::queue_id() const
{
	return m_queue_id;
}

const queue_state &queue::state()
{
	return m_state;
}

const queue_statistics &queue::statistics()
{
	return m_statistics;
}

void queue::clear_counters()
{
	memset(&m_statistics, 0, sizeof(m_statistics));
}

}} // namespace ioremap::grape
