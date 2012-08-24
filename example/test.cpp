#include <boost/lexical_cast.hpp>

#include <grape/grape.hpp>
#include <grape/elliptics.hpp>

using namespace ioremap::grape;

extern "C" {
	void *initialize(const char *config, const size_t size);
}

class test_node0_t : public elliptics_node_t {
	public:
		test_node0_t(const std::string &config) : elliptics_node_t(config) {}

		virtual std::string handle(struct sph *sph) {
			char *payload = (char *)(sph + 1);
			char *real_data = payload + sph->event_size;

			std::string event = xget_event(sph, payload);

			xlog(__LOG_NOTICE, "node0::process: %s: data-size: %zd, binary-size: %zd, event-size: %d: data: %.*s\n",
					event.c_str(), sph->data_size, sph->binary_size, sph->event_size,
					(int)sph->data_size, real_data);

			if (event == "event0")
				emit(NULL, "test-key", "test-app@event1", std::string(real_data, sph->data_size) + "1");
			if (event == "event1")
				emit(NULL, "test-key", "test-app@event2", std::string(real_data, sph->data_size) + "2");
			if (event == "event2")
				emit(NULL, "test-key", "test-app@finish", std::string(real_data, sph->data_size) + "3");

			if (event == "finish")
				return std::string(real_data, sph->data_size);

			return std::string();
		}
};

void *initialize(const char *config, const size_t size)
{
	topology_t *top = new topology_t("/dev/stdout", __LOG_INFO);
	xlog(__LOG_INFO, "init: %s\n", config);
	/*
	 * Everything below is a proof-of-concept code
	 * Do not use it as C++ codying cook-book,
	 * but rather properly code exception handling
	 */

	std::string cfg(config, size);
	test_node0_t *node0 = new test_node0_t(cfg);

	top->add_slot("event0", node0);
	top->add_slot("event1", node0);
	top->add_slot("event2", node0);
	top->add_slot("finish", node0);

	return (void *)top;
}
