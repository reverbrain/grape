#include <boost/lexical_cast.hpp>

#include <xroute/xroute.hpp>
#include <xroute/elliptics.hpp>

using namespace ioremap::xroute;

extern "C" {
	void init(topology_t &);
}

class test_node0_t : public elliptics_node_t {
	public:
		test_node0_t(topology_t &top) : elliptics_node_t(top) {}
		virtual std::string process(struct sph &header, const std::string &data) {
			xlog(__LOG_NOTICE, "node0::procress: '%s'\n", data.c_str());

			std::string ndata = data.substr(header.event_size) + "-" + boost::lexical_cast<std::string>(getpid()) + "-node0";

			emit("test-key", "libxroute_etest.so/event1", ndata);

			return std::string();
		}
};

class test_node1_t : public elliptics_node_t {
	public:
		test_node1_t(topology_t &top) : elliptics_node_t(top) {}
		virtual std::string process(struct sph &header, const std::string &data) {
			xlog(__LOG_NOTICE, "node1::procress: '%s'\n", data.c_str());
			std::string ndata = data.substr(header.event_size) + "-" + boost::lexical_cast<std::string>(getpid()) + "-node0";

			emit("test-key", "libxroute_etest.so/event2", ndata);

			return std::string();
		}
};

class test_node2_t : public elliptics_node_t {
	public:
		test_node2_t(topology_t &top) : elliptics_node_t(top) {}
		virtual std::string process(struct sph &header, const std::string &data) {
			std::string event = xget_event(header, data.data());

			xlog(__LOG_INFO, "received: event: '%s', worker-selection-key: %08x, data: '%s'\n",
				event.c_str(), header.key, data.c_str());

			return std::string();
		}
};

void init(class topology_t &top)
{
	/*
	 * Everything below is a proof-of-concept code
	 * Do not use it as C++ codying cook-book,
	 * but rather properly code exception handling
	 */

	test_node0_t *node0 = new test_node0_t(top);
	test_node1_t *node1 = new test_node1_t(top);
	test_node2_t *node2 = new test_node2_t(top);

	top.add_slot("event0", node0);
	top.add_slot("event1", node1);
	top.add_slot("event2", node2);

	std::string event = "libxroute_etest.so/event0";
	std::string data = "some data";

#if 0
	struct sph header;
	memset(&header, 0, sizeof(struct sph));
	header.event_size = event.size();
	header.data_size = data.size();

	top.run_slot(header, event + data);
#endif
}
