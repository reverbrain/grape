#ifndef __XROUTE_NODE_HPP
#define __XROUTE_NODE_HPP

#include <elliptics/srw.h>

#include <grape/logger.hpp>

namespace ioremap { namespace grape {

static inline std::string xget_event(const struct sph *header, const char *data) {
	std::string event;
	event.assign(data, header->event_size);

	return event;
}

class node_t {
	public:
		node_t() {
		}
		virtual ~node_t() {
			xlog(__LOG_DSA, "node is being destroyed\n");
		}

		virtual std::string process(const std::string &event, const char *data, const size_t dsize) = 0;
		virtual void emit(const std::string &key, const std::string &event, const std::string &data) = 0;
		virtual void put(const std::string &key, const std::string &data) = 0;
		virtual std::string get(const std::string &key) = 0;
};

}}

#endif /* __XROUTE_NODE_HPP */
