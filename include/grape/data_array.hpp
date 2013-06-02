#ifndef __GRAPE_DATA_ARRAY_HPP
#define __GRAPE_DATA_ARRAY_HPP

#include <elliptics/utils.hpp>

#include <vector>
#include <string>

#include <msgpack.hpp>

namespace ioremap { namespace grape {

class data_array {
	public:
		static data_array deserialize(const elliptics::data_pointer &d);

		void append(const char *data, size_t size);
		void append(const data_array &d);

		const std::vector<int> &sizes(void) const;
		const std::string &data(void) const;

		bool empty(void) const;

		elliptics::data_pointer serialize(void);

		MSGPACK_DEFINE(m_size, m_data);
	private:
		std::vector<int>	m_size;
		std::string		m_data;
};

}}

#endif /* __GRAPE_DATA_ARRAY_HPP */
