#ifndef __XROUTE_ELLIPTICS_HPP
#define __XROUTE_ELLIPTICS_HPP

#include <elliptics/cppdef.h>
#include <elliptics/srw/shared.hpp>

#include <grape/node.hpp>
#include <grape/grape.hpp>

using namespace ioremap::srw;

namespace ioremap { namespace grape {

typedef void (* topology_init_t)(topology_t &top);

class elliptics_event_handler_t : public event_handler_t {
	public:
		elliptics_event_handler_t(const std::string &log, const std::string &config, shared *sh) :
		m_sh(sh),
		m_log(log.c_str(), std::ios::app),
		m_elog(log.c_str(), 0xff),
		m_n(m_elog, config)
		{
			if (m_n.get_groups().size() == 0) {
				std::ostringstream str;

				str << getpid() << ": elliptics_event_handler_t: no groups added, exiting";
				throw std::runtime_error(str.str());
			}

			if (m_n.state_num() == 0) {
				std::ostringstream str;

				str << getpid() << ": elliptics_event_handler_t: no remote nodes added, exiting";
				throw std::runtime_error(str.str());
			}
		}

		virtual std::string handle(struct sph &header, const std::string &data) {
			std::string event = xget_event(header, data.data());

			std::vector<std::string> strs;
			boost::split(strs, event, boost::is_any_of("/"));

			xlog(__LOG_NOTICE, "event: %s\n", event.c_str());

			if ((strs[0] == "new-task") && strs.size() == 2) {
				get_new_task(strs[1]);

				std::ostringstream str;
				str << "loaded in pid: " << getpid();
				return str.str();
			} else {
				return m_top->run_slot(header, data);
			}
		}

		void emit(const std::string &key, const std::string &event, const std::string &data) {
			struct dnet_id id;
			m_n.transform(key, id);
			id.group_id = 0;
			id.type = 0;

			xlog(__LOG_NOTICE, "elliptics::emit: key: '%s', event: '%s', data-size: %zd\n",
					key.c_str(), event.c_str(), data.size());

			std::string binary;
			m_n.push(&id, event, data, binary);
		}

	private:
		/*
		 * object of this class can only be created from init() call,
		 * which in turn only lives as part of the given ioremap::srw::shared object
		 */
		shared *m_sh;
		std::ofstream m_log;
		zbr::elliptics_log_file m_elog;
		zbr::elliptics_node m_n;
		std::auto_ptr<topology_t> m_top;

		void get_new_task(const std::string &name) {
			std::ostringstream out;
			out << "/tmp/" << name << "-" << getpid() << ".so";

			m_n.read_file(name, out.str(), 0, 0, 0);
			load(out.str(), name);

			for (std::map<std::string, node_t *>::const_iterator it = m_top->slots().begin(); it != m_top->slots().end(); ++it) {
				m_sh->add_handler(name + "/" + it->first, this);
			}
		}

		void load(const std::string &path, const std::string &name) {
			void *handle;
			char *error;

			handle = dlopen(path.c_str(), RTLD_NOW);
			if (!handle) {
				std::ostringstream str;
				str << "Could not open shared library " << path << ": " << dlerror();
				throw std::runtime_error(str.str());
			}

			topology_init_t init = (topology_init_t)dlsym(handle, "init");

			if ((error = dlerror()) != NULL) {
				std::ostringstream str;
				str << "Could not get 'init' from shared library " << path << ": " << dlerror();

				dlclose(handle);
				throw std::runtime_error(str.str());
			}

			m_top.reset(new topology_t(name, (void *)this, handle));
			init(*m_top);
		}
};

class elliptics_node_t : public node_t {
	public:
		elliptics_node_t(topology_t &top) : m_e((elliptics_event_handler_t *)top.base()) {}

		virtual void emit(const std::string &key, const std::string &event, const std::string &data) {
			m_e->emit(key, event, data);
		}

	private:
		elliptics_event_handler_t *m_e;
};

}}

#endif /* __XROUTE_ELLIPTICS_HPP */
