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

const int DEFAULT_MAX_CHUNK_SIZE = 10000;

ioremap::grape::queue::queue(const std::string &queue_id, ev::loop_ref &event_loop)
	: m_chunk_max(DEFAULT_MAX_CHUNK_SIZE)
	, loop(event_loop)
	, m_queue_id(queue_id)
	, m_queue_stat_id(queue_id + ".stat")
{
}

void ioremap::grape::queue::initialize(const std::string &config)
{
	rapidjson::Document doc;
	m_client = elliptics_client_state::create(config, doc);

	LOG_INFO("init: elliptics client created");

	if (doc.HasMember("chunk-max-size"))
		m_chunk_max = doc["chunk-max-size"].GetInt();

	memset(&m_stat, 0, sizeof(struct queue_stat));

	try {
		ioremap::elliptics::data_pointer stat = m_client.create_session().read_data(m_queue_stat_id, 0, 0).get_one().file();
		ioremap::grape::queue_stat *st = stat.data<ioremap::grape::queue_stat>();

		m_stat.chunk_id_push = st->chunk_id_push;
		m_stat.chunk_id_ack = st->chunk_id_ack;
		LOG_INFO("init: queue meta found: chunk_id_ack %d, chunk_id_push %d",
				m_stat.chunk_id_ack, m_stat.chunk_id_push
				);
	} catch (const ioremap::elliptics::not_found_error &) {
		LOG_INFO("init: no queue meta found, starting in pristine state");
	}

	// load metadata of existing chunk into memory
	ioremap::elliptics::session tmp = m_client.create_session();
	for (int i = m_stat.chunk_id_ack; i <= m_stat.chunk_id_push; ++i) {
		auto p = std::make_shared<chunk>(tmp, m_queue_id, i, m_chunk_max);
		m_chunks.insert(std::make_pair(i, p));
		p->load_meta();
	}

	LOG_INFO("init: queue started");
}

void ioremap::grape::queue::push(const ioremap::elliptics::data_pointer &d)
{
	auto found = m_chunks.find(m_stat.chunk_id_push);
	if (found == m_chunks.end()) {
		// create new empty chunk
		ioremap::elliptics::session tmp = m_client.create_session();
		auto p = std::make_shared<chunk>(tmp, m_queue_id, m_stat.chunk_id_push, m_chunk_max);
		auto inserted = m_chunks.insert(std::make_pair(m_stat.chunk_id_push, p));

		found = inserted.first;
	}

	int chunk_id = found->first;
	auto chunk = found->second;

	if (chunk->push(d)) {
		m_stat.chunk_id_push++;
		chunk->add(&m_stat.chunks_pushed);
		update_indexes();
		LOG_INFO("chunk %d filled", chunk_id);
	}

	m_stat.push_count++;
}

ioremap::elliptics::data_pointer ioremap::grape::queue::peek(entry_id *entry_id)
{
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
			m_stat.pop_count++;

			// set or reset timeout timer for the chunk
			{
				auto inserted = m_wait_completion.insert(std::make_pair(chunk_id, wait_item(this, chunk_id, chunk)));
				if (inserted.second) {
					wait_item &w = inserted.first->second;
					w.timeout.reset(new ev::timer(loop));
					w.timeout->set(0.0, 5.0);
					w.timeout->set<ioremap::grape::queue::wait_item, &ioremap::grape::queue::wait_item::on_timeout>(&w);
				}
				inserted.first->second.timeout->again();
			}

			break;
		}

		if (chunk_id == m_stat.chunk_id_push) {
			break;
		}

		//FIXME: this could happen to be called many times for the same chunk
		// (between queue restarts or because of chunk replaying)
		chunk->add(&m_stat.chunks_popped);

		LOG_INFO("chunk %d exhausted, dropped from the popping line", chunk_id);

		// drop chunk from the pop list
		m_chunks.erase(found);

		update_indexes();
	}

	return d;
}

void ioremap::grape::queue::check_chunk_completion(int chunk_id)
{
	auto found = m_wait_completion.find(chunk_id);
	if (found == m_wait_completion.end()) {
		return;
	}

	// Time passed but chunk still is not complete
	// so we must replay unacked items from it.
	LOG_ERROR("chunk %d timed out, returning back to the popping line", chunk_id);

	const wait_item &w = found->second;
	w.timeout->stop();

	// There are two cases when chunk can still be in m_chunks
	// while experiencing a timeout:
	// 1) if it's a single chunk in the queue (and serves both
	//    as a push and a pop/ack target)
	// 2) if iteration of this chunk was preempted by other timed out chunk
	//    and then later this chunk timed out in its turn
	// Timeout requires switch chunk's iteration into the replay mode.

	auto inserted = m_chunks.insert({chunk_id, w.chunk});
	auto chunk = inserted.first->second;
	if (!inserted.second) {
		LOG_INFO("chunk %d is already in popping line", chunk_id);
	} else {
		LOG_INFO("chunk %d inserted back to the popping line anew", chunk_id);
	}
	chunk->reset_iteration();

	m_wait_completion.erase(found);

	// number of popped but still unacked entries
	m_stat.timeout_count += (chunk->meta().low_mark() - chunk->meta().acked());
}

void ioremap::grape::queue::ack(const entry_id id)
{
	auto found = m_wait_completion.find(id.chunk);
	if (found == m_wait_completion.end()) {
		return;
	}

	auto chunk = found->second.chunk;
	chunk->ack(id.pos);
	if (chunk->meta().acked() == chunk->meta().low_mark()) {
		// Real end of the chunk's lifespan, all popped entries are acked

		// Stop timeout timer for the chunk if its active,
		// and forget about chunk
		found->second.timeout->stop();
		m_wait_completion.erase(found);

		chunk->add(&m_stat.chunks_popped);

		// Chunk would be uncomplete here only if its the only chunk in the queue
		// (filled partially and serving both as a push and a pop/ack target)
		if (chunk->meta().complete()) {
			chunk->remove();
			LOG_INFO("chunk %d complete", id.chunk);
		}

		// Set chunk_id_ack to the lowest active chunk
		//NOTE: its important to have m_chunks and m_wait_completion both sorted
		m_stat.chunk_id_ack = m_stat.chunk_id_push;
		if (!m_chunks.empty()) {
			m_stat.chunk_id_ack = std::min(m_stat.chunk_id_ack, m_chunks.begin()->first);
		}
		if (!m_wait_completion.empty()) {
			m_stat.chunk_id_ack = std::min(m_stat.chunk_id_ack, m_wait_completion.begin()->first);	
		}

		update_indexes();
	}

	++m_stat.ack_count;
}

ioremap::elliptics::data_pointer ioremap::grape::queue::pop()
{
	entry_id id;
	ioremap::elliptics::data_pointer d = peek(&id);
	if (!d.empty()) {
		ack(id);
	}
	return d;
}

ioremap::grape::data_array ioremap::grape::queue::pop(int num)
{
	ioremap::grape::data_array d = peek(num);
	if (!d.empty()) {
		ack(d.ids());
	}
	return d;
}

ioremap::grape::data_array ioremap::grape::queue::peek(int num)
{
	ioremap::grape::data_array ret;

	while (num > 0) {
		auto found = m_chunks.begin();
		if (found == m_chunks.end()) {
			break;
		}

		int chunk_id = found->first;
		auto chunk = found->second;

		ioremap::grape::data_array d = chunk->pop(num);
		LOG_INFO("chunk %d, popping %d entries", chunk_id, d.sizes().size());
		if (!d.empty()) {
			m_stat.pop_count += d.sizes().size();

			ret.extend(d);

			// set or reset timeout timer for the chunk
			{
				auto inserted = m_wait_completion.insert(std::make_pair(chunk_id, wait_item(this, chunk_id, chunk)));
				if (inserted.second) {
					wait_item &w = inserted.first->second;
					w.timeout.reset(new ev::timer(loop));
					w.timeout->set(0.0, 5.0);
					w.timeout->set<ioremap::grape::queue::wait_item, &ioremap::grape::queue::wait_item::on_timeout>(&w);
				}
				inserted.first->second.timeout->again();
			}

			break;
		}

		if (chunk_id == m_stat.chunk_id_push) {
			break;
		}

		//FIXME: this could happen to be called many times for the same chunk
		// (between queue restarts or because of chunk replaying)
		chunk->add(&m_stat.chunks_popped);

		LOG_INFO("chunk %d exhausted, dropped from the popping line", chunk_id);

		// drop chunk from the pop list
		m_chunks.erase(found);

		update_indexes();
	}

	return ret;
}

void ioremap::grape::queue::ack(const std::vector<entry_id> &ids)
{
	for (auto i = ids.begin(); i != ids.end(); ++i) {
		const entry_id &id = *i;
		ack(id);
	}
}

ioremap::grape::queue_stat ioremap::grape::queue::stat()
{
	return m_stat;
}

void ioremap::grape::queue::reply(const ioremap::elliptics::exec_context &context,
		const ioremap::elliptics::data_pointer &d, ioremap::elliptics::exec_context::final_state state)
{
	m_client.create_session().reply(context, d, state);
}

void ioremap::grape::queue::final(const ioremap::elliptics::exec_context &context, const ioremap::elliptics::data_pointer &d)
{
	reply(context, d, ioremap::elliptics::exec_context::final);
}

void ioremap::grape::queue::update_indexes()
{
	m_client.create_session().write_data(m_queue_stat_id,
			ioremap::elliptics::data_pointer::from_raw(&m_stat, sizeof(struct ioremap::grape::queue_stat)),
			0);

	m_stat.update_indexes++;
}

const std::string ioremap::grape::queue::queue_id(void) const
{
	return m_queue_id;
}
