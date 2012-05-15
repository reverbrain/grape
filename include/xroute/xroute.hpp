#ifndef __XROUTE_HPP
#define __XROUTE_HPP

#include <dlfcn.h>

#include <boost/thread.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/algorithm/string.hpp>

#include <xroute/node.hpp>
#include <xroute/logger.hpp>

namespace ioremap { namespace xroute {

class topology_t {
	public:
		topology_t(const std::string &name, void *base, void *handle) : m_name(name), m_base(base), m_handle(handle) {}
		~topology_t() {
			dlclose(m_handle);
		}

		void add_slot(const std::string &event, node_t *node) {
			boost::lock_guard<boost::mutex> lock(m_lock);
			m_slots.insert(std::make_pair(event, node));
		}

		std::string run_slot(struct sph &header, const std::string &data) {
			std::string event = xget_event(header, data.data());

			std::vector<std::string> strs;
			boost::split(strs, event, boost::is_any_of("/"));

			boost::lock_guard<boost::mutex> lock(m_lock);
			std::map<std::string, node_t *>::iterator it = m_slots.find(strs[1]);

			if (it == m_slots.end()) {
				std::ostringstream str;
				str << getpid() << ": " << event << ": " << m_name << ": no slot in topology";
				throw std::runtime_error(str.str());
			}

			std::string ret = it->second->process(header, data);
			xlog(__LOG_NOTICE, "topology::run_slot: event: '%s', data-size: %zd, return-data: '%s'\n",
					event.c_str(), data.size(), ret.c_str());
			return ret;
		}

		void *base(void) {
			return m_base;
		}

		const std::map<std::string, node_t *> &slots() const {
			return m_slots;
		}

	private:
		std::string m_name;
		void *m_base;
		void *m_handle;
		boost::mutex m_lock;
		std::map<std::string, node_t *> m_slots;
};

}}

#endif /* __XROUTE_HPP */
