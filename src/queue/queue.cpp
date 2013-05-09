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

ioremap::grape::queue::queue(const std::string &config, const std::string &queue_id):
m_queue_id(queue_id),
m_chunk_id_push(0),
m_chunk_id_pop(0)
{
	rapidjson::Document doc;
	m_client = elliptics_client_state::create(config, doc);
}

void ioremap::grape::queue::push(const ioremap::elliptics::data_pointer &d)
{
	auto ch = m_chunks.find(m_chunk_id_push);
	if (ch == m_chunks.end()) {
		ioremap::elliptics::session tmp = m_client.create_session();
		auto epair = m_chunks.insert(std::make_pair(m_chunk_id_push,
					std::make_shared<chunk>(tmp, m_queue_id, m_chunk_id_push, 3)));

		ch = epair.first;
	}

	if (ch->second->push(d)) {
		m_chunk_id_push++;
	}
}

ioremap::elliptics::data_pointer ioremap::grape::queue::pop(void)
{
	ioremap::elliptics::data_pointer d;

	while (true) {
		auto ch = m_chunks.find(m_chunk_id_pop);
		if (ch == m_chunks.end())
			return d;

		d = ch->second->pop();
		if (!d.empty())
			return d;

		if (m_chunk_id_pop == m_chunk_id_push) {
			return d;
		}

		m_chunks.erase(ch);
		m_chunk_id_pop++;
	}

	return d;
}
