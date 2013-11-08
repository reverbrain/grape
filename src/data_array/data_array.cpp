#include "grape/data_array.hpp"

namespace ioremap { namespace grape {

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
void data_array::append(const data_array::entry &entry)
{
	append(entry.data, entry.size, entry.entry_id);
}

void data_array::extend(const data_array &d)
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

data_array::iterator::iterator(data_array &array, bool at_end)
	: array(array), index(0), offset(0)
{
	if (at_end) {
		index = array.sizes().size();
		offset = array.data().size();
	}
}

data_array::iterator::iterator(const iterator &other)
	: array(other.array), index(other.index), offset(other.offset)
{
}

data_array::iterator &data_array::iterator::operator =(const iterator &other)
{
	array = other.array;
	index = other.index;
	offset = other.offset;
	return *this;
}

bool data_array::iterator::operator ==(const iterator &other) const
{
	return &array == &other.array && index == other.index && offset == other.offset;
}

bool data_array::iterator::operator !=(const iterator &other) const
{
	return !operator ==(other);
}

void data_array::iterator::prepare_value() const
{
	value.data = (const char *)array.data().data() + offset;
	value.size = array.sizes()[index];
	value.entry_id = array.ids()[index];
}

data_array::iterator::value_type data_array::iterator::operator *() const
{
	prepare_value();
	return value;
}

data_array::iterator::value_type *data_array::iterator::operator ->() const
{
	prepare_value();
	return &value;
}

data_array::iterator &data_array::iterator::operator ++()
{
	int size = array.sizes()[index];
	offset += size;
	++index;
	return *this;
}

data_array::iterator data_array::iterator::operator ++(int)
{
	iterator tmp = *this;
	operator ++();
	return tmp;
}

data_array::iterator data_array::begin()
{
	return iterator(*this, false);
}

data_array::iterator data_array::end()
{
	return iterator(*this, true);
}

}}
