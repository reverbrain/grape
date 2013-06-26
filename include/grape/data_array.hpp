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
		void append(const char *data, size_t size, const entry_id &id);
		void extend(const data_array &d);

		const std::vector<entry_id> &ids() const;
		const std::vector<int> &sizes() const;
		const std::string &data() const;

		bool empty() const;

		MSGPACK_DEFINE(m_id, m_size, m_data);
	private:
		std::vector<entry_id> m_id;
		std::vector<int> m_size;
		std::string m_data;
};

template <class T>
elliptics::data_pointer serialize(const T &obj) {
	msgpack::sbuffer sbuf;
	msgpack::pack(sbuf, obj);

	return elliptics::data_pointer::copy(sbuf.data(), sbuf.size());
}

template <class T>
T deserialize(const elliptics::data_pointer &d) {
	msgpack::unpacked msg;
	msgpack::unpack(&msg, (const char *)d.data(), d.size());

	msgpack::object obj = msg.get();

	T result;
	obj.convert(&result);

	return result;
}

}}

#endif /* __GRAPE_DATA_ARRAY_HPP */
