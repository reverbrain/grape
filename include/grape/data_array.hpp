#ifndef __GRAPE_DATA_ARRAY_HPP
#define __GRAPE_DATA_ARRAY_HPP

#include <vector>
#include <string>
#include <iterator>

#include <msgpack.hpp>

#include <elliptics/utils.hpp>
#include <grape/entry_id.hpp>

namespace ioremap { namespace grape {

class data_array
{
private:
	std::vector<entry_id> m_id;
	std::vector<int> m_size;
	std::string m_data;

public:
	// virtual view onto single array's item
	struct entry
	{
		const char *data;
		size_t size;
		grape::entry_id entry_id;
	};

	class iterator : public std::iterator<std::forward_iterator_tag, entry>
	{
	public:
		iterator(const iterator &other);

		iterator &operator =(const iterator &other);

		bool operator ==(const iterator &other) const;
		bool operator !=(const iterator &other) const;

		value_type operator *() const;
		value_type *operator ->() const;

		iterator &operator ++();
		iterator operator ++(int);

	private:
		iterator(const data_array *array, bool at_end);
	
		const data_array *array;
		int index;
		size_t offset;

		void prepare_value() const;
		mutable value_type value;

		friend class data_array;
	};

	void append(const char *data, size_t size, const entry_id &id);
	void append(const std::string &data, const entry_id &id);
	void append(const data_array::entry &entry);
	void extend(const data_array &d);

	bool empty() const;

	// "do whatever you want" interface
	const std::vector<entry_id> &ids() const;
	const std::vector<int> &sizes() const;
	const std::string &data() const;

	// iteration interface

	iterator begin() const;
	iterator end() const;

	MSGPACK_DEFINE(m_id, m_size, m_data);
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
