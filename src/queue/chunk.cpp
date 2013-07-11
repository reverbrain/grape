#include <iostream>

#include "queue.hpp"

extern std::shared_ptr<cocaine::framework::logger_t> grape_queue_module_get_logger();
#define LOG_INFO(...) COCAINE_LOG_INFO(grape_queue_module_get_logger(), __VA_ARGS__)
#define LOG_ERROR(...) COCAINE_LOG_ERROR(grape_queue_module_get_logger(), __VA_ARGS__)
#define LOG_DEBUG(...) COCAINE_LOG_DEBUG(grape_queue_module_get_logger(), __VA_ARGS__)

ioremap::grape::chunk_meta::chunk_meta(int max)
	: m_ptr(NULL)
{
	m_data.resize(sizeof(struct chunk_disk) + max * sizeof(struct chunk_entry));
	m_ptr = (struct chunk_disk *)m_data.data();
	
	m_ptr->max = max;

}

bool ioremap::grape::chunk_meta::push(int size)
{
	if (m_ptr->high >= m_ptr->max)
		ioremap::elliptics::throw_error(-ERANGE, "chunk is full: high: %d, max: %d", m_ptr->high, m_ptr->max);

	m_ptr->entries[m_ptr->high].size = size;
	m_ptr->high++;

	LOG_DEBUG("\tmeta.push: acked: %d, low: %d, high: %d, max: %d", m_ptr->acked, m_ptr->low, m_ptr->high, m_ptr->max);

	return full();
}

void ioremap::grape::chunk_meta::pop()
{
	if (m_ptr->high > m_ptr->max) {
		ioremap::elliptics::throw_error(-ERANGE, "invalid pop: high mark can not be more than maximum chunk size: "
				"low: %d, high: %d, max: %d",
				m_ptr->low, m_ptr->high, m_ptr->max);
	}

	if (m_ptr->low >= m_ptr->high) {
		ioremap::elliptics::throw_error(-ERANGE, "invalid pop: position can not be more than high mark: "
				"low: %d, high: %d, max: %d",
				m_ptr->low, m_ptr->high, m_ptr->max);
	}

	m_ptr->low++;

	LOG_DEBUG("\tmeta.pop: acked: %d, low: %d, high: %d, max: %d", m_ptr->acked, m_ptr->low, m_ptr->high, m_ptr->max);
}

bool ioremap::grape::chunk_meta::ack(int32_t pos, int state)
{
	if (pos >= m_ptr->high) {
		ioremap::elliptics::throw_error(-ERANGE, "invalid ack: position can not be more than high mark: "
				"pos: %d, acked: %d, high: %d, max: %d",
				pos, m_ptr->acked, m_ptr->high, m_ptr->max);
	}

	if (pos >= m_ptr->max) {
		ioremap::elliptics::throw_error(-ERANGE, "invalid ack: position can not be more than maximum chunk size: "
				"pos: %d, acked: %d, high: %d, max: %d",
				pos, m_ptr->acked, m_ptr->high, m_ptr->max);
	}

	if (m_ptr->acked >= m_ptr->high) {
		ioremap::elliptics::throw_error(-ERANGE, "invalid ack: acked can not be more than high mark: "
				"pos: %d, acked: %d, high: %d, max: %d",
				pos, m_ptr->acked, m_ptr->high, m_ptr->max);
	}

	m_ptr->entries[pos].state = state;
	m_ptr->acked++;
	LOG_DEBUG("\tmeta.ack: pos: %d, acked: %d, low: %d, high: %d, max: %d", pos, m_ptr->acked, m_ptr->low, m_ptr->high, m_ptr->max);

	return complete();
}

std::string &ioremap::grape::chunk_meta::data()
{
	return m_data;
}

int ioremap::grape::chunk_meta::low_mark() const
{
	return m_ptr->low;
}

int ioremap::grape::chunk_meta::high_mark() const
{
	return m_ptr->high;
}

int ioremap::grape::chunk_meta::acked() const
{
	return m_ptr->acked;
}

bool ioremap::grape::chunk_meta::full() const
{
	return m_ptr->high == m_ptr->max;
}

bool ioremap::grape::chunk_meta::exhausted() const
{
	return m_ptr->low == m_ptr->max;
}

bool ioremap::grape::chunk_meta::complete() const
{
	return m_ptr->acked == m_ptr->max;
}
void ioremap::grape::chunk_meta::assign(char *data, size_t size)
{
	if (size != m_data.size()) {
		ioremap::elliptics::throw_error(-ERANGE, "chunk meta assignment with invalid size: current: %ld, want-to-assign: %ld",
				m_data.size(), size);
	}

	m_data.assign(data, size);
	m_ptr = (struct chunk_disk *)m_data.data();
}

ioremap::grape::chunk_entry ioremap::grape::chunk_meta::operator[] (int32_t pos) const
{
	if (pos > m_ptr->high) {
		ioremap::elliptics::throw_error(-ERANGE, "invalid entry access: pos: %d, high: %d, max: %d",
				pos, m_ptr->high, m_ptr->max);
	}

	return m_ptr->entries[pos];
}

uint64_t ioremap::grape::chunk_meta::byte_offset(int32_t pos) const
{
	if (pos > m_ptr->high) {
		ioremap::elliptics::throw_error(-ERANGE, "invalid entry access: pos: %d, high: %d, max: %d",
				pos, m_ptr->high, m_ptr->max);
	}

	uint64_t offset = 0;
	for (int i = 0; i < pos; ++i) {
		offset += m_ptr->entries[i].size;
	}

	return offset;
}

ioremap::grape::chunk::chunk(ioremap::elliptics::session &session, const std::string &queue_id, int chunk_id, int max)
	: m_chunk_id(chunk_id)
	, m_data_key(queue_id + ".chunk." + std::to_string(chunk_id))
	, m_meta_key(queue_id + ".chunk." + std::to_string(chunk_id) + ".meta")
	, m_session_data(session.clone())
	, m_session_meta(session.clone())
	, m_meta(max)
	, m_fire_time(0)
{
	m_session_data.set_ioflags(DNET_IO_FLAGS_APPEND | DNET_IO_FLAGS_NOCSUM);
	m_session_meta.set_ioflags(DNET_IO_FLAGS_NOCSUM | DNET_IO_FLAGS_OVERWRITE);

	memset(&m_stat, 0, sizeof(struct chunk_stat));
}

ioremap::grape::chunk::~chunk()
{
	//XXX: is it really needed?
	//write_meta();
}

void ioremap::grape::chunk::load_meta()
{
	try {
		ioremap::elliptics::data_pointer d = m_session_meta.read_data(m_meta_key, 0, 0).get_one().file();
		m_meta.assign((char *)d.data(), d.size());
		++m_stat.read;
		reset_iteration_mode();

	} catch (const ioremap::elliptics::not_found_error &e) {
		// ignore not-found exception - create empty chunk
		LOG_ERROR("chunk %d, load_meta, ERROR: meta not found: %s", m_chunk_id, e.what());

	} catch (const ioremap::elliptics::error &e) {
		// special case to ignore bad chunk meta format error
		// (raised by chunk_clt::assign())
		LOG_ERROR("chunk %d, load_meta, ERROR: error reading meta: %s", m_chunk_id, e.what());
		if (e.error_code() != -ERANGE) {
			throw;
		}
	}
}

bool ioremap::grape::chunk::expect_no_more()
{
	return m_meta.full() && iter->at_end();
}

void ioremap::grape::chunk::prepare_iteration()
{
	try {
		// Actual data is read on first pop() and also reread on chunk's exhaustion
		// (for data could be updated by push).
		// Metadata is read only at start (as it resides in memory and properly updated by push).
		//
		if (iteration_state.byte_offset >= m_data.size()) {
			LOG_INFO("chunk %d, prepare_iteration, (re)reading data, iteration.byte_offset %lld, m_data.size() %ld", m_chunk_id, iteration_state.byte_offset, m_data.size());
			
			m_data = m_session_data.read_data(m_data_key, 0, 0).get_one().file();
		}

		if (!iter) {
			LOG_INFO("chunk %d, prepare_iteration, initializing iterator", m_chunk_id);
			reset_iteration_mode();
		}

	} catch (const ioremap::elliptics::not_found_error &e) {
		// Do not explode on not-found-error, return empty data pointer
		LOG_ERROR("chunk %d, prepare_iteration, ERROR: %s", m_chunk_id, e.what());

	} catch (const ioremap::elliptics::timeout_error &e) {
		// Do not explode on timeout-error, return empty data pointer
		LOG_ERROR("chunk %d, prepare_iteration, ERROR: %s", m_chunk_id, e.what());

		//XXX: this means we silently ignore unreachable data,
		// and it can't be good

	} catch (const ioremap::elliptics::error &e) {
		// Special case to ignore bad chunk meta format error
		// (raised by chunk_clt::assign())
		LOG_ERROR("chunk %d, prepare_iteration, ERROR: %s", m_chunk_id, e.what());
		if (e.error_code() != -ERANGE) {
			throw;
		}
	}
}

ioremap::elliptics::data_pointer ioremap::grape::chunk::pop(int *pos)
{
	ioremap::elliptics::data_pointer d;
	*pos = -1;

	// Fast track for the case when chunk is empty (not exist).
	// It valid to check only metadata as push() updates metadata in memory
	if (m_meta.high_mark() == 0) {
		LOG_INFO("chunk %d, pop-single, empty", m_chunk_id);
		return d;
	}

	prepare_iteration();

	LOG_INFO("chunk %d, pop-single, iter: mode %d, index %d, offset %lld", m_chunk_id, iter->mode, iteration_state.entry_index, iteration_state.byte_offset);

	if (iter->mode == iterator::REPLAY && iter->at_end()) {
		LOG_INFO("chunk %d, pop-single, iter: mode %d, index %d, offset %lld, switching to mode %d", m_chunk_id, iter->mode, iteration_state.entry_index, iteration_state.byte_offset, iterator::FORWARD);

		iter.reset(new forward_iterator(iteration_state, m_meta));
		iter->begin();
	}

	if (!iter->at_end()) {
		int size = m_meta[iteration_state.entry_index].size;
		d = ioremap::elliptics::data_pointer::copy((char *)m_data.data() + iteration_state.byte_offset, size);
		*pos = iteration_state.entry_index;

		iter->advance();

	} else {
		LOG_INFO("chunk %d, pop-single, iter: mode %d, index %d, offset %lld, is at end", m_chunk_id, iter->mode, iteration_state.entry_index, iteration_state.byte_offset);
	}

	++m_stat.pop;
	return d;
}

ioremap::grape::data_array ioremap::grape::chunk::pop(int num)
{
	ioremap::grape::data_array ret;

	// Fast track for the case when chunk is empty (not exist).
	// Its valid to check only metadata as push() updates metadata in memory
	if (m_meta.high_mark() == 0) {
		LOG_INFO("chunk %d, pop, empty", m_chunk_id);
		return ret;
	}

	prepare_iteration();

	entry_id entry_id;
	entry_id.chunk = m_chunk_id;

	while(num > 0) {
		if (iter->mode == iterator::REPLAY && iter->at_end()) {
			
			LOG_INFO("chunk %d, pop, iter: mode %d, index %d, offset %lld, switching to mode %d", m_chunk_id, iter->mode, iteration_state.entry_index, iteration_state.byte_offset, iterator::FORWARD);

			iter.reset(new forward_iterator(iteration_state, m_meta));
			iter->begin();
		}

		if (iter->at_end()) {
			LOG_INFO("chunk %d, pop, iter: mode %d, index %d, offset %lld, is at end", m_chunk_id, iter->mode, iteration_state.entry_index, iteration_state.byte_offset);
			break;
		}

		int size = m_meta[iteration_state.entry_index].size;
		entry_id.pos = iteration_state.entry_index;
		ret.append((char *)m_data.data() + iteration_state.byte_offset, size, entry_id);
		
		iter->advance();

		++m_stat.pop;
		--num;
	}

	return ret;
}

const ioremap::grape::chunk_meta &ioremap::grape::chunk::meta()
{
	return m_meta;
}

void ioremap::grape::chunk::write_meta()
{
	m_session_meta.write_data(m_meta_key, ioremap::elliptics::data_pointer::from_raw(m_meta.data()), 0);
	++m_stat.write_meta;
}

void ioremap::grape::chunk::remove()
{
	m_session_meta.remove(m_meta_key);
	m_session_data.remove(m_data_key);
	++m_stat.remove;
}

bool ioremap::grape::chunk::push(const ioremap::elliptics::data_pointer &d)
{
	LOG_INFO("chunk %d, push-single, index %d", m_chunk_id, m_meta.high_mark());

	// if given chunk already has some cached data, update it too
	if (m_data.size()) {
		std::string tmp = m_data.to_string();
		tmp += d.to_string();

		m_data = ioremap::elliptics::data_pointer::copy(tmp.data(), tmp.size());
	}

	m_session_data.write_data(m_data_key, d, 0);
	++m_stat.write_data;
	//XXX: not going to wait for completion? what if write happen to be unsuccessfull?

	m_meta.push(d.size());
	if (m_meta.full()) {
		//XXX: is it good to write meta only for full chunks?
		write_meta();
	}

	++m_stat.push;
	return m_meta.full();
}

bool ioremap::grape::chunk::ack(int pos)
{
	//FIXME: check if pos < low < high 
	m_meta.ack(pos, 1);
	write_meta();

	++m_stat.ack;

	return m_meta.complete();
}

void ioremap::grape::chunk::reset_iteration()
{
	iteration_state = iteration();
	reset_iteration_mode();
}

void ioremap::grape::chunk::reset_iteration_mode()
{
	iter.reset(new replay_iterator(iteration_state, m_meta));
	iter->begin();
	if (iter->at_end()) {
		iter.reset(new forward_iterator(iteration_state, m_meta));
		iter->begin();
	}
}

struct ioremap::grape::chunk_stat ioremap::grape::chunk::stat(void)
{
	return m_stat;
}

void ioremap::grape::chunk::add(struct ioremap::grape::chunk_stat *st)
{
#define SUM(member)	st->member += m_stat.member
	SUM(write_data);
	SUM(write_meta);
	SUM(read);
	SUM(remove);
	SUM(push);
	SUM(pop);
	SUM(ack);
#undef SUM
}

int ioremap::grape::chunk::id(void) const
{
	return m_chunk_id;
}

void ioremap::grape::chunk::reset_time(double timeout)
{
	struct timeval tv;

	gettimeofday(&tv, NULL);
	m_fire_time = tv.tv_sec + timeout;
}

double ioremap::grape::chunk::get_time(void)
{
	return m_fire_time;
}
