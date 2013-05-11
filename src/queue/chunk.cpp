#include "queue.hpp"

#include <iostream>

ioremap::grape::chunk_ctl::chunk_ctl(int max):
m_ptr(NULL)
{
	m_chunk.resize(sizeof(struct chunk_disk) + max * sizeof(struct chunk_entry));
	m_ptr = (struct chunk_disk *)m_chunk.data();
	
	m_ptr->max = max;

	//std::cout << "new chunk: max: " << max << std::endl;
}

bool ioremap::grape::chunk_ctl::push(int size)
{
	if (m_ptr->used >= m_ptr->max)
		ioremap::elliptics::throw_error(-ERANGE, "chunk is full: used: %d, max: %d", m_ptr->used, m_ptr->max);

	m_ptr->states[m_ptr->used].size = size;
	m_ptr->used++;
	return m_ptr->used == m_ptr->max;
}

int ioremap::grape::chunk_ctl::ack(int pos, int state)
{
	if (pos >= m_ptr->used) {
		ioremap::elliptics::throw_error(-ERANGE, "invalid ack: position can not be more than used: "
				"pos: %d, acked: %d, used: %d, max: %d",
				pos, m_ptr->acked, m_ptr->used, m_ptr->max);
	}

	if (pos >= m_ptr->max) {
		ioremap::elliptics::throw_error(-ERANGE, "invalid ack: position can not be more than maximum chunk size: "
				"pos: %d, acked: %d, used: %d, max: %d",
				pos, m_ptr->acked, m_ptr->used, m_ptr->max);
	}

	if (m_ptr->acked >= m_ptr->used) {
		ioremap::elliptics::throw_error(-ERANGE, "invalid ack: acked can not be more than used: "
				"pos: %d, acked: %d, used: %d, max: %d",
				pos, m_ptr->acked, m_ptr->used, m_ptr->max);
	}

	m_ptr->states[pos].state = state;
	m_ptr->acked++;
	return m_ptr->acked;
}

std::string &ioremap::grape::chunk_ctl::data(void)
{
	return m_chunk;
}

int ioremap::grape::chunk_ctl::used(void) const
{
	return m_ptr->used;
}

int ioremap::grape::chunk_ctl::acked(void) const
{
	return m_ptr->acked;
}

void ioremap::grape::chunk_ctl::assign(char *data, int size)
{
	if (size != m_chunk.size())
		ioremap::elliptics::throw_error(-ERANGE, "chunk assignment with invalid size: current: %zd, want-to-assign: %d",
				m_chunk.size(), size);

	m_chunk.assign(data, size);
	m_ptr = (struct chunk_disk *)m_chunk.data();
}

struct ioremap::grape::chunk_entry ioremap::grape::chunk_ctl::operator[] (int pos)
{
	if (pos > m_ptr->used)
		ioremap::elliptics::throw_error(-ERANGE, "invalid entry access: pos: %d, used: %d, max: %d",
				pos, m_ptr->used, m_ptr->max);

	return m_ptr->states[pos];
}

ioremap::grape::chunk::chunk(ioremap::elliptics::session &session, const std::string &queue_id, int chunk_id, int max):
m_chunk_id(chunk_id),
m_queue_id(queue_id),
m_data_key(queue_id + ".chunk." + lexical_cast(chunk_id)),
m_ctl_key(queue_id + ".chunk." + lexical_cast(chunk_id) + ".meta"),
m_session_data(session.clone()),
m_session_ctl(session.clone()),
m_pop_position(0),
m_chunk(max)
{
	m_session_data.set_ioflags(DNET_IO_FLAGS_APPEND | DNET_IO_FLAGS_NOCSUM);
	m_session_ctl.set_ioflags(DNET_IO_FLAGS_NOCSUM | DNET_IO_FLAGS_OVERWRITE);

	try {
		ioremap::elliptics::data_pointer d = m_session_ctl.read_data(m_ctl_key, 0, 0).get_one().file();
		m_chunk.assign((char *)d.data(), d.size());
#if 0
		std::cout << "constructor: chunk read: data-key: " << m_data_key.to_string() <<
			", ctl-key: " << m_ctl_key.to_string() <<
			", chunk-size: " << m_chunk_data.size() <<
			", used: " << m_chunk.used() <<
			", acked: " << m_chunk.acked() <<
			std::endl;
#endif
		for (int i = 0; i < m_chunk.acked(); ++i)
			m_pop_position += m_chunk[i].size;
	} catch (const ioremap::elliptics::not_found_error &) {
		// ignore not-found exception - create empty chunk
	}
}

ioremap::grape::chunk::~chunk()
{
}

void ioremap::grape::chunk::write_chunk(void)
{
	m_session_ctl.write_data(m_ctl_key, ioremap::elliptics::data_pointer::from_raw(m_chunk.data()), 0);
}

void ioremap::grape::chunk::remove(void)
{
	m_session_ctl.remove(m_ctl_key);
}

bool ioremap::grape::chunk::push(const ioremap::elliptics::data_pointer &d)
{
	auto async_data = m_session_data.write_data(m_data_key, d, 0);

	bool full = m_chunk.push(d.size());
	write_chunk();

	//async_data.wait();

	return full;
}

ioremap::elliptics::data_pointer ioremap::grape::chunk::pop(void)
{
	ioremap::elliptics::data_pointer d;

	if (m_pop_position >= m_chunk_data.size()) {
		m_chunk_data = m_session_data.read_data(m_data_key, 0, 0).get_one().file();
		if (m_chunk.used() == 0) {
			ioremap::elliptics::data_pointer d = m_session_ctl.read_data(m_ctl_key, 0, 0).get_one().file();
			m_chunk.assign((char *)d.data(), d.size());
		}

		m_pop_position = 0;
		for (int i = 0; i < m_chunk.acked(); ++i)
			m_pop_position += m_chunk[i].size;
#if 0
		std::cout << "chunk read: data-key: " << m_data_key.to_string() <<
			", ctl-key: " << m_ctl_key.to_string() <<
			", chunk-size: " << m_chunk_data.size() <<
			", used: " << m_chunk.used() <<
			", acked: " << m_chunk.acked() <<
			", m_pop_position: " << m_pop_position <<
			std::endl;
#endif
	}

	if (m_chunk.acked() < m_chunk.used() && m_pop_position < m_chunk_data.size()) {
		int size = m_chunk[m_chunk.acked()].size;
		d = ioremap::elliptics::data_pointer::copy((char *)m_chunk_data.data() + m_pop_position, size);

		m_chunk.ack(m_chunk.acked(), 1);
		write_chunk();

		m_pop_position += size;
	}

	return d;
}
