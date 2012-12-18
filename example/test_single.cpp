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

			/*
			 * Reply adds not only your data, but also the whole sph header to the original caller's waiting container
			 */
			reply(orig_sph, event, "This is single test event result", true);

			xlog(__LOG_NOTICE, "grape::test-node0: %s: data-size: %zd, binary-size: %zd, event-size: %d: data: '%.*s'\n",
					event.c_str(), sph->data_size, sph->binary_size, sph->event_size,
					(int)sph->data_size, real_data);

		}
};

static void test_add_app0(topology_t *top, const std::string &cfg)
{
	test_node0_t *node0 = new test_node0_t(cfg);

	top->add_slot("test-single@event", node0);
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

	return (void *)top;
}
