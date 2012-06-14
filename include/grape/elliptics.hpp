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

		virtual void emit(const std::string &key, const std::string &event, const std::string &data);
		virtual void put(const std::string &key, const std::string &data);
		virtual std::string get(const std::string &key);

	private:
		std::auto_ptr<elliptics::log_file> m_elog;
		std::auto_ptr<elliptics::node> m_node;
};

}}

#endif /* __GRAPE_ELLIPTICS_HPP */
