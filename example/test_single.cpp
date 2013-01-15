#include <stdlib.h>

#include <boost/lexical_cast.hpp>

#include <json/reader.h>

#include <grape/grape.hpp>
#include <grape/elliptics.hpp>

using namespace ioremap::grape;

namespace {
	enum test_write_type {
		TEST_WRITE_NONE = 0,
		TEST_WRITE_RANDOM,
	};
}

extern "C" {
	void *initialize(const char *config, const size_t size);
}

class test_node0_t : public elliptics_node_t {
	public:
		test_node0_t(const std::string &config) :
			elliptics_node_t(config), m_wtype(TEST_WRITE_NONE), m_cflags(0), m_ioflags(0), m_event_num(0) {
			struct timeval tv;

			gettimeofday(&tv, NULL);
			srand(tv.tv_sec + tv.tv_usec);

			Json::Reader reader;
			Json::Value root;

			reader.parse(config, root);

			try {
				std::string write_type = root["write-type"].asString();
				if (write_type == "random")
					m_wtype = TEST_WRITE_RANDOM;
			} catch (...) {
			}

			try {
				m_ioflags = root["ioflags"].asInt();
			} catch (...) {
			}

			try {
				m_cflags = root["cflags"].asInt64();
			} catch (...) {
			}

			Json::Value groups(root["groups"]);
			if (!groups.empty() && groups.isArray()) {
				std::transform(groups.begin(), groups.end(), std::back_inserter(m_groups), json_digitizer());
			}

			m_event_base = root["event-base"].asString();
			m_event_num = root["event-num"].asInt();
		}

		void handle(struct sph *sph) {
			char *payload = (char *)(sph + 1);

			char *real_data = payload + sph->event_size;
			std::string data(real_data, sph->data_size);

			struct sph orig_sph = *sph;

			std::string event = xget_event(sph, payload);
			std::string emit_event;

			bool final = false;

			if (event == m_event_base + "-start") {
				emit_event = m_event_base + boost::lexical_cast<std::string>(0);
			} else {
				int pos = 0;
				sscanf(event.c_str(), (m_event_base + "%d").c_str(), &pos);
				
				pos++;
				if (pos < m_event_num)
					emit_event = m_event_base + boost::lexical_cast<std::string>(pos);
				else
					final = true;
			}

			io();
			if (emit_event.size() > 0) {
				std::string rand_key = boost::lexical_cast<std::string>(rand()) + "test-single";
				emit(orig_sph, rand_key, emit_event, data);
			}

			if (final) {
				char date_str[64];
				struct tm tm;
				struct timeval tv;

				gettimeofday(&tv, NULL);
				localtime_r((time_t *)&tv.tv_sec, &tm);
				strftime(date_str, sizeof(date_str), "%F %R:%S", &tm);

				std::ostringstream reply_data;
				reply_data << date_str << "." << tv.tv_usec << ": " << event << ": " << data << "\n";
				/*
				 * Reply adds not only your data, but also the whole sph header to the original caller's waiting container
				 */
				reply(orig_sph, event, reply_data.str(), final);
			}

			xlog(__LOG_INFO, "%s: grape::test-node0: %s -> %s: data-size: %zd, binary-size: %zd, final: %d\n",
					dnet_dump_id_str(sph->src.id),
					event.c_str(), emit_event.c_str(), sph->data_size, sph->binary_size, final);
		}

	private:
		std::vector<int> m_groups;
		test_write_type m_wtype;
		uint64_t m_cflags;
		uint32_t m_ioflags;
		std::string m_event_base;
		int m_event_num;

		void io(void) {
			if (m_wtype == TEST_WRITE_NONE)
				return;

			if (m_groups.empty())
				return;

			std::ostringstream key;
			key << rand();

			std::string data;
			data.resize(100);

			ioremap::elliptics::session s(*m_node);

			s.set_groups(m_groups);
			s.set_ioflags(m_ioflags);
			s.set_cflags(m_cflags);

			try {
				s.read_data(key.str(), 0, 0);
			} catch (...) {
			}

			try {
				s.write_data(key.str(), data, 0);
			} catch (...) {
			}

			xlog(__LOG_NOTICE, "grape::test-node0::io: %s", key.str().c_str());
		}
};

void *initialize(const char *config, const size_t size)
{
	std::string cfg(config, size);

	Json::Reader reader;
	Json::Value root;

	reader.parse(config, root);

	/*
	 * Everything below is a proof-of-concept code
	 * Do not use it as C++ codying cook-book,
	 * but rather properly code exception handling
	 */

	topology_t *top = new topology_t(root["log"].asString().c_str(), root["log-level"].asInt());

	test_node0_t *node0 = new test_node0_t(cfg);
	std::string base = root["event-base"].asString();
	int num = root["event-num"].asInt();

	for (int i = 0; i < num; ++i)
		top->add_slot(base + boost::lexical_cast<std::string>(i), node0);
	
	top->add_slot(base + "-start", node0);

	return (void *)top;
}
