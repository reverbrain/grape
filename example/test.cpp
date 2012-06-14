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

		virtual std::string process(const std::string &event, const char *data, const size_t dsize) {
			struct sph *sph = (struct sph *)data;
			char *payload = (char *)(sph + 1);

			std::string real_event = xget_event(sph, payload);

			xlog(__LOG_NOTICE, "node0::procress: %s: total-data-size: %zd\n", real_event.c_str(), dsize);

			if (event == "event0")
				emit("test-key", "test-app@event1", std::string(data, dsize) + "1");
			if (event == "event1")
				emit("test-key", "test-app@event2", std::string(data, dsize) + "2");
			if (event == "event2")
				emit("test-key", "test-app@finish", std::string(data, dsize) + "3");

			return std::string();
		}
};

void *initialize(const char *config, const size_t size)
{
	topology_t *top = new topology_t("/dev/stdout", 15);
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
