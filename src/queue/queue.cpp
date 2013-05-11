#include "queue.hpp"

#include "rapidjson/document.h"
#include "rapidjson/prettywriter.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/filestream.h"

#include <cocaine/json.hpp>
#include <cocaine/framework/logging.hpp>

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

ioremap::grape::queue::queue(const std::string &config, const std::string &queue_id, int max):
m_chunk_max(max),
m_queue_id(queue_id),
m_queue_stat_id(queue_id + ".stat")
{
	rapidjson::Document doc;
	m_client = elliptics_client_state::create(config, doc);

	memset(&m_stat, 0, sizeof(struct queue_stat));

	try {
		ioremap::elliptics::data_pointer stat = m_client.create_session().read_data(m_queue_stat_id, 0, 0).get_one().file();
		struct ioremap::grape::queue_stat *st = (struct ioremap::grape::queue_stat *)stat.data();

		m_stat.chunk_id_push = st->chunk_id_push;
		m_stat.chunk_id_pop = st->chunk_id_pop;
	} catch (const ioremap::elliptics::not_found_error &) {
	}

	//LOG_INFO("%s: chunk_id_push: %d, chunk_id_pop: %d\n", m_stat.chunk_id_push, m_stat.chunk_id_pop);

	ioremap::elliptics::session tmp = m_client.create_session();
	for (int i = m_stat.chunk_id_pop; i <= m_stat.chunk_id_push; ++i) {
		m_chunks.insert(std::make_pair(i, std::make_shared<chunk>(tmp, m_queue_id, i, m_chunk_max)));
	}
}

void ioremap::grape::queue::push(const ioremap::elliptics::data_pointer &d)
{
	auto ch = m_chunks.find(m_stat.chunk_id_push);
	if (ch == m_chunks.end()) {
		ioremap::elliptics::session tmp = m_client.create_session();
		auto epair = m_chunks.insert(std::make_pair(m_stat.chunk_id_push,
					std::make_shared<chunk>(tmp, m_queue_id, m_stat.chunk_id_push, m_chunk_max)));

		ch = epair.first;
	}

	if (ch->second->push(d)) {
		m_stat.chunk_id_push++;
		update_indexes();
	}

	m_stat.push_count++;
}

ioremap::elliptics::data_pointer ioremap::grape::queue::pop(void)
{
	ioremap::elliptics::data_pointer d;

	while (true) {
		auto ch = m_chunks.find(m_stat.chunk_id_pop);
		if (ch == m_chunks.end())
			break;

		d = ch->second->pop();
		if (!d.empty()) {
			m_stat.pop_count++;
			break;
		}

		if (m_stat.chunk_id_pop == m_stat.chunk_id_push)
			break;

		ch->second->remove();

		m_chunks.erase(ch);
		m_stat.chunk_id_pop++;

		update_indexes();
	}

	return d;
}

struct ioremap::grape::queue_stat &ioremap::grape::queue::stat(void)
{
	return m_stat;
}

void ioremap::grape::queue::reply(const ioremap::elliptics::exec_context &context, const ioremap::elliptics::data_pointer &d)
{
	m_client.create_session().reply(context, d, ioremap::elliptics::exec_context::final);
}

void ioremap::grape::queue::update_indexes(void)
{
	m_client.create_session().write_data(m_queue_stat_id,
			ioremap::elliptics::data_pointer::from_raw(&m_stat, sizeof(struct ioremap::grape::queue_stat)),
			0).wait();
}
