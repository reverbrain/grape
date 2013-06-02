#include "queue.hpp"

using namespace ioremap;
using namespace ioremap::grape;

void data_array::append(const char *data, size_t size)
{
	size_t old_data_size = m_data.size();

	try {
		m_data.insert(m_data.end(), data, data + size);
		m_size.push_back(size);
	} catch (...) {
		m_data.resize(old_data_size);
		throw;
	}
}

void data_array::append(const data_array &d)
{
	size_t old_data_size = m_data.size();
	size_t old_sizes_size = m_size.size();

	try {
		m_data.insert(m_data.end(), d.data().begin(), d.data().end());
		m_size.insert(m_size.end(), d.sizes().begin(), d.sizes().end());
	} catch (...) {
		m_data.resize(old_data_size);
		m_size.resize(old_sizes_size);
		throw;
	}
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
