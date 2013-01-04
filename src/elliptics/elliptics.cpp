#include "json/reader.h"

#include "grape/elliptics.hpp"

using namespace ioremap::grape;

elliptics_node_t::elliptics_node_t(const std::string &config)
{
	Json::Reader reader;
	Json::Value root;

	reader.parse(config, root);

	m_elog.reset(new elliptics::file_logger(root["log"].asString().c_str(), root["log-level"].asInt()));

	m_node.reset(new elliptics::node(*m_elog));
	m_session.reset(new elliptics::session(*m_node));

	Json::Value groups(root["groups"]);
	if (groups.empty() || !groups.isArray())
		throw std::runtime_error("no groups has been specified");

	std::vector<int> group_numbers;
	std::transform(groups.begin(), groups.end(), std::back_inserter(group_numbers), json_digitizer());

	m_session->set_groups(group_numbers);

	if (m_session->get_groups().size() == 0)
		throw std::runtime_error("elliptics_topology_t: no groups added, exiting");

	Json::Value nodes(root["nodes"]);
	if (nodes.empty() || !nodes.isObject())
		throw std::runtime_error("no nodes has been specified");

	Json::Value::Members node_names(nodes.getMemberNames());

	for(Json::Value::Members::const_iterator it = node_names.begin(); it != node_names.end(); ++it) {
		try {
			m_node->add_remote(it->c_str(), nodes[*it].asInt());
		} catch(const std::runtime_error&) {
		}
	}

	if (m_session->state_num() == 0)
		throw std::runtime_error("elliptics_topology_t: no remote nodes added, exiting");
}

void elliptics_node_t::emit(const struct sph &sph, const std::string &key, const std::string &event, const std::string &data)
{
	struct dnet_id id;
	m_session->transform(key, id);
	id.group_id = 0;
	id.type = 0;

	char id_str[DNET_DUMP_NUM * 2 + 1];
	char sph_str[DNET_DUMP_NUM * 2 + 1];

	dnet_dump_id_len_raw(id.id, DNET_DUMP_NUM, id_str);
	dnet_dump_id_len_raw(sph.src.id, DNET_DUMP_NUM, sph_str);

	id_str[2 * DNET_DUMP_NUM] = '\0';
	sph_str[2 * DNET_DUMP_NUM] = '\0';

	xlog(__LOG_INFO, "%s: sph: %s: %s: grape::emit: data-size: %zd\n", id_str, sph_str, event.c_str(), data.size());

	std::string binary;
	m_session->push_unlocked(&id, sph, event, data, binary);
}

std::string elliptics_node_t::emit_blocked(const std::string &key, const std::string &event, const std::string &data)
{
	struct dnet_id id;
	m_session->transform(key, id);
	id.group_id = 0;
	id.type = 0;

	xlog(__LOG_NOTICE, "grape::emit_blocked: key: '%s', event: '%s', data-size: %zd\n",
			key.c_str(), event.c_str(), data.size());

	std::string binary;
	return m_session->exec_unlocked(&id, event, data, binary);
}

void elliptics_node_t::reply(const struct sph &orig_sph, const std::string &event, const std::string &data, bool finish)
{
	struct sph sph = orig_sph;

	sph.flags &= ~DNET_SPH_FLAGS_SRC_BLOCK;
	sph.flags |= DNET_SPH_FLAGS_REPLY;

	if (finish)
		sph.flags |= DNET_SPH_FLAGS_FINISH;

	std::string binary;

	return m_session->reply(sph, event, data, binary);
}

void elliptics_node_t::remove(const std::string &key)
{
	m_session->remove(key);
}

void elliptics_node_t::put(const std::string &key, const std::string &data)
{
	uint64_t remote_offset = 0;

	m_session->write_data_wait(key, data, remote_offset);
}

std::string elliptics_node_t::get(const std::string &key)
{
	uint64_t offset = 0;
	uint64_t size = 0;

	return m_session->read_data_wait(key, offset, size);
}

std::vector<std::string> elliptics_node_t::mget(const std::vector<std::string> &keys)
{
	return m_session->bulk_read(keys);
}

void elliptics_node_t::calculate_checksum(const std::string &data, struct dnet_id &id) {
	m_session->transform(data, id);
}

void elliptics_node_t::compare_and_swap(const std::string &key, const std::string &data, const struct dnet_id &old_csum) {
	uint64_t remote_offset = 0;

	m_session->write_cas(key, data, old_csum, remote_offset);
}

std::vector<std::string> elliptics_node_t::mget(const std::vector<struct dnet_io_attr> &keys)
{
	return m_session->bulk_read(keys);
}
