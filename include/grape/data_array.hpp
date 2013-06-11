#ifndef __GRAPE_DATA_ARRAY_HPP
#define __GRAPE_DATA_ARRAY_HPP

#include <vector>
#include <string>
#include <msgpack.hpp>

#include <elliptics/utils.hpp>
#include <grape/entry_id.hpp>

namespace ioremap { namespace grape {

class data_array {
	public:
		static data_array deserialize(const elliptics::data_pointer &d);

		void append(const char *data, size_t size, const entry_id &id);
		void append(const data_array &d);

		const std::vector<entry_id> &ids() const;
		const std::vector<int> &sizes() const;
		const std::string &data() const;

		bool empty() const;

		elliptics::data_pointer serialize();

		MSGPACK_DEFINE(m_id, m_size, m_data);
	private:
		std::vector<entry_id> m_id;
		std::vector<int> m_size;
		std::string m_data;
};

}}

#endif /* __GRAPE_DATA_ARRAY_HPP */
