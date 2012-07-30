#ifndef __XROUTE_HPP
#define __XROUTE_HPP

#include <dlfcn.h>

#include <boost/thread.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/algorithm/string.hpp>

#include <grape/node.hpp>
#include <grape/logger.hpp>

namespace ioremap { namespace grape {

class topology_t {
	public:
		topology_t(const char *log_path, int log_mask = __LOG_ERROR | __LOG_INFO | __LOG_NOTICE) {
			logger::instance()->init(std::string(log_path), log_mask, true);
		}
		virtual ~topology_t() {}

		void add_slot(const std::string &event, node_t *node) {
			boost::lock_guard<boost::mutex> lock(m_lock);
			m_slots.insert(std::make_pair(event, node));
		}

		std::string run_slot(struct sph *sph) {
			std::string event = xget_event(sph, (char *)(sph + 1));

			boost::lock_guard<boost::mutex> lock(m_lock);
			std::map<std::string, node_t *>::iterator it = m_slots.find(event);

			if (it == m_slots.end())
				throw std::runtime_error(event + ": no slot in topology");

			return it->second->handle(sph);
		}

	private:
		boost::mutex m_lock;
		std::map<std::string, node_t *> m_slots;
};

}}

#endif /* __XROUTE_HPP */
