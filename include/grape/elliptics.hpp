#ifndef __GRAPE_ELLIPTICS_HPP
#define __GRAPE_ELLIPTICS_HPP

#include <elliptics/cppdef.h>

#include <json/value.h>

#include <grape/node.hpp>
#include <grape/grape.hpp>

namespace ioremap { namespace grape {

namespace {
	struct json_digitizer {
		template<class T>
		int operator()(const T& value) {
			return value.asInt();
		}
	};
}

class elliptics_node_t : public node_t {
	public:
		elliptics_node_t(const std::string &config);

		void emit(const struct sph &sph, const std::string &key, const std::string &event, const std::string &data);
		std::string emit_blocked(const std::string &key, const std::string &event, const std::string &data);
		void reply(const struct sph &sph, const std::string &event, const std::string &data, bool finish);

		void remove(const std::string &key);
		void put(const std::string &key, const std::string &data);
		std::string get(const std::string &key);

		std::vector<std::string> mget(const std::vector<std::string> &keys);
		std::vector<std::string> mget(const std::vector<struct dnet_io_attr> &keys);

	private:
		std::auto_ptr<elliptics::log_file> m_elog;
		std::auto_ptr<elliptics::node> m_node;
		std::auto_ptr<elliptics::session> m_session;
};

}}

#endif /* __GRAPE_ELLIPTICS_HPP */
