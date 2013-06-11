#include "grape/data_array.hpp"

using namespace ioremap;
using namespace ioremap::grape;

void data_array::append(const char *data, size_t size, const entry_id &id)
{
	size_t old_data_size = m_data.size();

	try {
		m_data.insert(m_data.end(), data, data + size);
		m_size.push_back(size);
		m_id.push_back(id);
	} catch (...) {
		m_data.resize(old_data_size);
		throw;
	}
}

void data_array::append(const data_array &d)
{
	size_t old_data_size = m_data.size();
	size_t old_sizes_size = m_size.size();
	size_t old_ids_size = m_id.size();

	try {
		m_data.insert(m_data.end(), d.data().begin(), d.data().end());
		m_size.insert(m_size.end(), d.sizes().begin(), d.sizes().end());
		m_id.insert(m_id.end(), d.ids().begin(), d.ids().end());
	} catch (...) {
		m_data.resize(old_data_size);
		m_size.resize(old_sizes_size);
		m_id.resize(old_ids_size);
		throw;
	}
}

const std::vector<entry_id> &data_array::ids(void) const
{
	return m_id;
}

const std::vector<int> &data_array::sizes(void) const
{
	return m_size;
}

const std::string &data_array::data(void) const
{
	return m_data;
}

bool data_array::empty(void) const
{
	return m_size.empty();
}

elliptics::data_pointer data_array::serialize(void)
{
	msgpack::sbuffer sbuf;
	msgpack::pack(sbuf, *this);

	return elliptics::data_pointer::copy(sbuf.data(), sbuf.size());
}

data_array data_array::deserialize(const elliptics::data_pointer &d)
{
	msgpack::unpacked msg;
	msgpack::unpack(&msg, (const char *)d.data(), d.size());

	msgpack::object obj = msg.get();
	data_array array;

	obj.convert(&array);

	return array;	
}
