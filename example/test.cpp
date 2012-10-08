#include <boost/lexical_cast.hpp>

#include <json/reader.h>

#include <grape/grape.hpp>
#include <grape/elliptics.hpp>

using namespace ioremap::grape;

extern "C" {
	void *initialize(const char *config, const size_t size);
}

class test_node0_t : public elliptics_node_t {
	public:
		test_node0_t(const std::string &config) : elliptics_node_t(config) {}

		void handle(struct sph *sph) {
			char *payload = (char *)(sph + 1);
			char *real_data = payload + sph->event_size;

			struct sph orig_sph = *sph;

			std::string event = xget_event(sph, payload);

			xlog(__LOG_NOTICE, "node0::process: %s: data-size: %zd, binary-size: %zd, event-size: %d: data: %.*s\n",
					event.c_str(), sph->data_size, sph->binary_size, sph->event_size,
					(int)sph->data_size, real_data);

			if (event == "test-app@event0")
				emit(orig_sph, "1", "test-app@event1", std::string(real_data, sph->data_size) + "1");
			if (event == "test-app@event1")
				emit(orig_sph, "2", "test-app@event2", std::string(real_data, sph->data_size) + "2");
			if (event == "test-app@event2")
				emit(orig_sph, "finish", "test-app@finish", std::string(real_data, sph->data_size) + "3");

			if (event == "test-app@finish") {
				/*
				 * Block waiting for execution of the second application in a row
				 * This application has to be started already
				 */
				std::string second_result = emit_blocked("key", "test-app-second@event0", std::string(real_data, sph->data_size));

				/*
				 * Reply adds not only your data, but also the whole sph header to the original caller's waiting container
				 */
				reply(orig_sph, "test-app@finish", second_result, true);
			}
		}
};

class test_node1_t : public elliptics_node_t {
	public:
		test_node1_t(const std::string &config) : elliptics_node_t(config) {}

		void handle(struct sph *sph) {
			char *payload = (char *)(sph + 1);
			char *real_data = payload + sph->event_size;

			struct sph orig_sph = *sph;

			std::string event = xget_event(sph, payload);

			xlog(__LOG_NOTICE, "node1::process: %s: data-size: %zd, binary-size: %zd, event-size: %d: data: %.*s\n",
					event.c_str(), sph->data_size, sph->binary_size, sph->event_size,
					(int)sph->data_size, real_data);

			std::string data = std::string(real_data, sph->data_size) + event + "|";
			if (event == "test-app-second@event0")
				emit(orig_sph, "1", "test-app-second@event1", data);
			if (event == "test-app-second@event1")
				emit(orig_sph, "2", "test-app-second@event2", data);
			if (event == "test-app-second@event2")
				emit(orig_sph, "finish", "test-app-second@finish", data);

			if (event == "test-app-second@finish") {

				/*
				 * Reply adds not only your data, but also the whole sph header to the original caller's waiting container
				 */
				reply(orig_sph, "test-app-second@finish", data, true);
			}
		}
};

static void test_add_app0(topology_t *top, const std::string &cfg)
{
	test_node0_t *node0 = new test_node0_t(cfg);

	top->add_slot("test-app@event0", node0);
	top->add_slot("test-app@event1", node0);
	top->add_slot("test-app@event2", node0);
	top->add_slot("test-app@finish", node0);
}

static void test_add_app1(topology_t *top, const std::string &cfg)
{

	test_node1_t *node1 = new test_node1_t(cfg);

	top->add_slot("test-app-second@event0", node1);
	top->add_slot("test-app-second@event1", node1);
	top->add_slot("test-app-second@event2", node1);
	top->add_slot("test-app-second@finish", node1);
}

void *initialize(const char *config, const size_t size)
{
	Json::Reader reader;
	Json::Value root;

	reader.parse(config, root);
	std::string cfg(config, size);

	/*
	 * Everything below is a proof-of-concept code
	 * Do not use it as C++ codying cook-book,
	 * but rather properly code exception handling
	 */


	topology_t *top = new topology_t(root["log"].asString().c_str(), root["log-level"].asInt());

	test_add_app0(top, cfg);
	test_add_app1(top, cfg);

	return (void *)top;
}
