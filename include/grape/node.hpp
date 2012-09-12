#ifndef __XROUTE_NODE_HPP
#define __XROUTE_NODE_HPP

#include <vector>
#include <string>

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
			xlog(__LOG_DEBUG, "node is being destroyed\n");
		}

		virtual void handle(struct sph *sph) = 0;

		virtual void emit(const struct sph &sph, const std::string &key, const std::string &event, const std::string &data) = 0;
		virtual void reply(const struct sph &sph, const std::string &event, const std::string &data, bool finish) = 0;

		virtual void put(const std::string &key, const std::string &data) = 0;
		virtual std::string get(const std::string &key) = 0;
		virtual std::vector<std::string> mget(const std::vector<std::string> &keys) = 0;
};

}}

#endif /* __XROUTE_NODE_HPP */
