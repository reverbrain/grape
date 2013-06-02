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

void ioremap::grape::chunk_ctl::assign(char *data, size_t size)
{
	if (size != m_chunk.size())
		ioremap::elliptics::throw_error(-ERANGE, "chunk assignment with invalid size: current: %zd, want-to-assign: %zd",
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
m_data_key(queue_id + ".chunk." + lexical_cast(chunk_id)),
m_ctl_key(queue_id + ".chunk." + lexical_cast(chunk_id) + ".meta"),
m_session_data(session.clone()),
m_session_ctl(session.clone()),
m_pop_position(0),
m_chunk(max)
{
	m_session_data.set_ioflags(DNET_IO_FLAGS_APPEND | DNET_IO_FLAGS_NOCSUM);
	m_session_ctl.set_ioflags(DNET_IO_FLAGS_NOCSUM | DNET_IO_FLAGS_OVERWRITE);

	memset(&m_stat, 0, sizeof(struct chunk_stat));

	try {
		ioremap::elliptics::data_pointer d = m_session_ctl.read_data(m_ctl_key, 0, 0).get_one().file();
		m_chunk.assign((char *)d.data(), d.size());

		m_stat.read++;
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
	m_session_ctl.write_data(m_ctl_key, ioremap::elliptics::data_pointer::from_raw(m_chunk.data()), 0).wait();
	m_stat.write_ctl_async++;
}

void ioremap::grape::chunk::remove(void)
{
	m_session_ctl.remove(m_ctl_key);
	m_stat.remove++;
}

bool ioremap::grape::chunk::push(const ioremap::elliptics::data_pointer &d)
{
	auto async_data = m_session_data.write_data(m_data_key, d, 0);
	m_stat.write_data_async++;

	bool full = m_chunk.push(d.size());
	write_chunk();

	m_stat.push++;
	return full;
}

ioremap::grape::data_array ioremap::grape::chunk::pop(int num)
{
	ioremap::grape::data_array ret;

	try {
		while (num > 0) {
			if (m_pop_position >= m_chunk_data.size()) {
				m_chunk_data = m_session_data.read_data(m_data_key, 0, 0).get_one().file();
				if (m_chunk.used() == 0) {
					ioremap::elliptics::data_pointer d = m_session_ctl.read_data(m_ctl_key, 0, 0).get_one().file();
					m_chunk.assign((char *)d.data(), d.size());

					m_stat.read++;
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
					": " << m_chunk_data.skip(m_pop_position).to_string() <<
					std::endl;
#endif
			}

			// chunk has been completely sucked out
			if ((m_chunk.acked() >= m_chunk.used()) || (m_pop_position >= m_chunk_data.size()))
				break;

			int size = m_chunk[m_chunk.acked()].size;

			ret.append((char *)m_chunk_data.data() + m_pop_position, size);

			num--;

			m_chunk.ack(m_chunk.acked(), 1);
			m_stat.ack++;

			m_stat.pop++;

			write_chunk();

			m_pop_position += size;
		}
	} catch (const ioremap::elliptics::not_found_error &err) {
		/*
		 * Do not explode on not-found-error, return empty data pointer
		 */
	} catch (const ioremap::elliptics::timeout_error &err) {
		/*
		 * Do not explode on timeout-error, return empty data pointer
		 */
	}

	return ret;
}

struct ioremap::grape::chunk_stat ioremap::grape::chunk::stat(void)
{
	return m_stat;
}

void ioremap::grape::chunk::add(struct ioremap::grape::chunk_stat *st)
{
#define SUM(member)	st->member += m_stat.member
	SUM(write_data_async);
	SUM(write_data_sync);
	SUM(write_ctl_async);
	SUM(write_ctl_sync);
	SUM(read);
	SUM(remove);
	SUM(push);
	SUM(pop);
	SUM(ack);
}
