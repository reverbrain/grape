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

const int DEFAULT_MAX_CHUNK_SIZE = 10000;

queue::queue(const std::string &queue_id)
	: m_chunk_max(DEFAULT_MAX_CHUNK_SIZE)
	, m_queue_id(queue_id)
	, m_queue_state_id(m_queue_id + ".state")
	, m_last_timeout_check_time(0)
{
}

void queue::initialize(const std::string &config)
{
	memset(&m_statistics, 0, sizeof(m_statistics));

	rapidjson::Document doc;
	m_client = elliptics_client_state::create(config, doc);

	LOG_INFO("init: elliptics client created");

	if (doc.HasMember("chunk-max-size"))
		m_chunk_max = doc["chunk-max-size"].GetInt();

	memset(&m_state, 0, sizeof(m_state));

	try {
		ioremap::elliptics::data_pointer d = m_client.create_session().read_data(m_queue_state_id, 0, 0).get_one().file();
		auto *state = d.data<queue_state>();
	
		m_state.chunk_id_push = state->chunk_id_push;
		m_state.chunk_id_ack = state->chunk_id_ack;

		LOG_INFO("init: queue meta found: chunk_id_ack %d, chunk_id_push %d",
				m_state.chunk_id_ack, m_state.chunk_id_push
				);

	} catch (const ioremap::elliptics::not_found_error &) {
		LOG_INFO("init: no queue meta found, starting in pristine state");
	}

	// load metadata of existing chunk into memory
	ioremap::elliptics::session tmp = m_client.create_session();
	for (int i = m_state.chunk_id_ack; i <= m_state.chunk_id_push; ++i) {
		auto p = std::make_shared<chunk>(tmp, m_queue_id, i, m_chunk_max);
		m_chunks.insert(std::make_pair(i, p));
		if (!p->load_meta() && i < m_state.chunk_id_push) {
			LOG_ERROR("init: failed to read middle chunk meta data, can't proceed, exiting");
			ioremap::elliptics::throw_error(-ENODATA, "middle chunk %d meta data is missing", i);
		}
	}

	LOG_INFO("init: queue started");
}

void queue::write_state()
{
	m_client.create_session().write_data(m_queue_state_id,
			ioremap::elliptics::data_pointer::from_raw(&m_state, sizeof(queue_state)),
			0).wait();

	m_statistics.state_write_count++;
}

void queue::clear()
{
	LOG_INFO("clearing queue");

	LOG_INFO("erasing state");
	queue_state state = m_state;
	memset(&m_state, 0, sizeof(m_state));
	write_state();

	LOG_INFO("removing chunks, from %d to %d", state.chunk_id_ack, state.chunk_id_push);
	std::map<int, shared_chunk> remove_list;
	remove_list.swap(m_chunks);
	remove_list.insert(m_wait_ack.cbegin(), m_wait_ack.cend());
	m_wait_ack.clear();

	for (auto i = remove_list.cbegin(); i != remove_list.cend(); ++i) {
		int chunk_id = i->first;
		auto chunk = i->second;
		chunk->remove();
	}

	remove_list.clear();

	LOG_INFO("dropping statistics");
	clear_counters();

	LOG_INFO("queue cleared");
}

void queue::push(const ioremap::elliptics::data_pointer &d)
{
	auto found = m_chunks.find(m_state.chunk_id_push);
	if (found == m_chunks.end()) {
		// create new empty chunk
		ioremap::elliptics::session tmp = m_client.create_session();
		auto p = std::make_shared<chunk>(tmp, m_queue_id, m_state.chunk_id_push, m_chunk_max);
		auto inserted = m_chunks.insert(std::make_pair(m_state.chunk_id_push, p));

		found = inserted.first;
	}

	int chunk_id = found->first;
	auto chunk = found->second;

	if (chunk->push(d)) {
		LOG_INFO("chunk %d filled", chunk_id);

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

		LOG_INFO("popping entry %d-%d (%ld)'%s'", entry_id->chunk, entry_id->pos, d.size(), d.to_string());
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

			LOG_INFO("chunk %d exhausted, dropped from the popping line", chunk_id);

			// drop chunk from the pop list
			m_chunks.erase(found);
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
	chunk->reset_time(5.0);	
}

void queue::check_timeouts()
{
	struct timeval tv;
	gettimeofday(&tv, NULL);

	// check no more then once in 1 second
	//TODO: make timeout check interval configurable
	if ((tv.tv_sec - m_last_timeout_check_time) < 1) {
		return;
	}
	m_last_timeout_check_time = tv.tv_sec;

	LOG_INFO("checking timeouts: %ld waiting chunks", m_wait_ack.size());

	auto i = m_wait_ack.begin();
	while (i != m_wait_ack.end()) {
		int chunk_id = i->first;
		auto chunk = i->second;

		if (chunk->get_time() > tv.tv_sec) {
			++i;
			continue;
		}

		// Time passed but chunk still is not complete
		// so we must replay unacked items from it.
		LOG_ERROR("chunk %d timed out, returning back to the popping line, current time: %ld, chunk expiration time: %f",
				chunk_id, tv.tv_sec, chunk->get_time());

		// There are two cases when chunk can still be in m_chunks
		// while experiencing a timeout:
		// 1) if it's a single chunk in the queue (and serves both
		//    as a push and a pop/ack target)
		// 2) if iteration of this chunk was preempted by other timed out chunk
		//    and then later this chunk timed out in its turn
		// Timeout requires switch chunk's iteration into the replay mode.

		auto inserted = m_chunks.insert({chunk_id, chunk});
		if (!inserted.second) {
			LOG_INFO("chunk %d is already in popping line", chunk_id);
		} else {
			LOG_INFO("chunk %d inserted back to the popping line anew", chunk_id);
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
		LOG_ERROR("ack for chunk %d (pos %d) which is not in waiting list", id.chunk, id.pos);
		return;
	}

	auto chunk = found->second;
	chunk->ack(id.pos);
	if (chunk->meta().acked() == chunk->meta().low_mark()) {
		// Real end of the chunk's lifespan, all popped entries are acked

		m_wait_ack.erase(found);

		chunk->add(&m_statistics.chunks_popped);

		// Chunk would be uncomplete here only if its the only chunk in the queue
		// (filled partially and serving both as a push and a pop/ack target)
		if (chunk->meta().complete()) {
			chunk->remove();
			LOG_INFO("chunk %d complete", id.chunk);
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
		LOG_INFO("chunk %d, popping %d entries", chunk_id, d.sizes().size());
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

			LOG_INFO("chunk %d exhausted, dropped from the popping line", chunk_id);

			// drop chunk from the pop list
			m_chunks.erase(found);
		}

		num -= d.sizes().size();
	}

	return ret;
}

void queue::ack(const std::vector<entry_id> &ids)
{
	for (auto i = ids.begin(); i != ids.end(); ++i) {
		const entry_id &id = *i;
		ack(id);
	}
}

void queue::reply(const ioremap::elliptics::exec_context &context,
		const ioremap::elliptics::data_pointer &d, ioremap::elliptics::exec_context::final_state state)
{
	m_client.create_session().reply(context, d, state);
}

void queue::final(const ioremap::elliptics::exec_context &context, const ioremap::elliptics::data_pointer &d)
{
	reply(context, d, ioremap::elliptics::exec_context::final);
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