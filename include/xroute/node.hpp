#ifndef __XROUTE_NODE_HPP
#define __XROUTE_NODE_HPP

#include <elliptics/srw/base.h>

#include <xroute/logger.hpp>

namespace ioremap { namespace xroute {

static inline std::string xget_event(const struct sph &header, const char *data) {
	std::string event;
	event.assign(data, header.event_size);

	return event;
}

class node_t {
	public:
		node_t() {
			xlog(__LOG_NOTICE, "constructor: %p\n", this);
		}
		virtual ~node_t() {
			xlog(__LOG_NOTICE, "node is being destroyed\n");
		}

		virtual std::string process(struct sph &header, const std::string &data) {
			std::string event = xget_event(header, data.data());
			xlog(__LOG_NOTICE, "node::process: event: '%s', data-size: %zd\n", event.c_str(), data.size());
			return std::string();
		}
		virtual void emit(const std::string &key, const std::string &event, const std::string &data) = 0;
};

}}

#endif /* __XROUTE_NODE_HPP */
