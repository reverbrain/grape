#include <fstream>

#include <cocaine/format.hpp>
#include <cocaine/framework/logging.hpp>
#include <cocaine/framework/dispatch.hpp>

#include "queue.hpp"

namespace {

uint64_t microseconds_now() {
	timespec t;
	clock_gettime(CLOCK_MONOTONIC_RAW, &t);
	return t.tv_sec * 1000000 + t.tv_nsec / 1000;
}

template <unsigned N>
double modified_moving_average(double avg, double input) {
	avg -= avg / N;
	avg += input / N;
	return avg;
}

double exponential_moving_average(double avg, double input, double alpha) {
	return alpha * input + (1.0 - alpha) * avg;
}

struct rate_stat
{
	uint64_t last_update; // in microseconds
	double avg;

	rate_stat() : last_update(microseconds_now()), avg(0.0) {}

	void update(size_t num) {
		uint64_t now = microseconds_now();
		double elapsed = double(now - last_update) / 1000000; // in seconds
		double alpha = (elapsed > 1.0) ? 1.0 : elapsed;
		avg = exponential_moving_average(avg, ((double)num / elapsed), alpha);
		last_update = now;
	}

	double get() {
		return avg;
	}
};

struct time_stat
{
	uint64_t start_time; // in microseconds
	double avg;

	time_stat() : avg(0.0) {}

	void start() {
		start_time = microseconds_now();
	}
	void stop() {
		avg = modified_moving_average<19>(avg, microseconds_now() - start_time);
	}

	double get() {
		return avg;
	}
};

}

class queue_app_context {
	public:
		queue_app_context(cocaine::framework::dispatch_t& dispatch);
		virtual ~queue_app_context();

		void process(const std::string &cocaine_event, const std::vector<std::string> &chunks, cocaine::framework::response_ptr response);

	private:
		typedef ioremap::grape::data_array peek_multi_type;
		typedef std::vector<ioremap::grape::entry_id> ack_multi_type;

		std::string m_id;
		std::shared_ptr<cocaine::framework::logger_t> m_log;
		std::shared_ptr<ioremap::grape::queue> m_queue;

		rate_stat m_push_rate;
		rate_stat m_pop_rate;
		rate_stat m_ack_rate;

		time_stat m_push_time;
		time_stat m_pop_time;
		time_stat m_ack_time;
};

queue_app_context::queue_app_context(cocaine::framework::dispatch_t& dispatch)
    : m_id(dispatch.id())
	, m_log(dispatch.service_manager()->get_system_logger())
{
	//FIXME: pass logger explicitly everywhere
	extern void grape_queue_module_set_logger(std::shared_ptr<cocaine::framework::logger_t>);
	grape_queue_module_set_logger(m_log);
	
    m_queue.reset(new ioremap::grape::queue(m_id));
	m_queue->initialize("queue.conf");
	COCAINE_LOG_INFO(m_log, "%s: queue has been successfully configured", m_id.c_str());

	// register event handlers
	dispatch.on("queue@ping", this, &queue_app_context::process);
	dispatch.on("queue@push", this, &queue_app_context::process);
	dispatch.on("queue@pop-multi", this, &queue_app_context::process);
	dispatch.on("queue@pop-multiple-string", this, &queue_app_context::process);
	dispatch.on("queue@pop", this, &queue_app_context::process);
	dispatch.on("queue@peek", this, &queue_app_context::process);
	dispatch.on("queue@peek-multi", this, &queue_app_context::process);
	dispatch.on("queue@ack", this, &queue_app_context::process);
	dispatch.on("queue@ack-multi", this, &queue_app_context::process);
	dispatch.on("queue@clear", this, &queue_app_context::process);
	dispatch.on("queue@stats-clear", this, &queue_app_context::process);
	dispatch.on("queue@stats", this, &queue_app_context::process);
}

queue_app_context::~queue_app_context()
{
}

void queue_app_context::process(const std::string &cocaine_event, const std::vector<std::string> &chunks, cocaine::framework::response_ptr response)
{
	ioremap::elliptics::exec_context context = ioremap::elliptics::exec_context::from_raw(chunks[0].c_str(), chunks[0].size());

	std::string app;
	std::string event;
	{
		char *p = strchr((char*)context.event().c_str(), '@');
		app.assign(context.event().c_str(), p - context.event().c_str());
		event.assign(p + 1);
	}

	const std::string action_id = cocaine::format("%s %d, %s", dnet_dump_id_str(context.src_id()->id), context.src_key(), m_id.c_str());

	COCAINE_LOG_INFO(m_log, "%s, event: %s, data-size: %ld",
			action_id.c_str(),
			event.c_str(), context.data().size()
			);

	if (event == "ping") {
		m_queue->final(context, std::string("ok"));

	} else if (event == "push") {
		ioremap::elliptics::data_pointer d = context.data();
		// skip adding zero length data, because there is no value in that
		// queue has no method to request size and we can use zero reply in pop
		// to indicate queue emptiness
		if (!d.empty()) {
			m_push_time.start();
			m_queue->push(d);
			m_push_time.stop();
			COCAINE_LOG_INFO(m_log, "push time %ld", microseconds_now() - m_push_time.start_time);
			m_push_rate.update(1);
		}
		m_queue->final(context, ioremap::elliptics::data_pointer());

	} else if (event == "pop-multi" || event == "pop-multiple-string") {
		int num = stoi(context.data().to_string());

		m_pop_time.start();
		m_ack_time.start();
		peek_multi_type d = m_queue->pop(num);
		m_ack_time.stop();
		m_pop_time.stop();
		if (!d.empty()) {
			m_queue->final(context, ioremap::grape::serialize(d));
			m_pop_rate.update(d.sizes().size());
			m_ack_rate.update(d.sizes().size());
		} else {
			m_queue->final(context, ioremap::elliptics::data_pointer());
		}

		COCAINE_LOG_INFO(m_log, "%s, completed event: %s, size: %ld, popped: %d/%d (multiple: '%s')",
				action_id.c_str(),
				event.c_str(), context.data().size(),
				d.sizes().size(), num, context.data().to_string().c_str()
				);

	} else if (event == "pop") {
		m_pop_time.start();
		m_ack_time.start();
		m_queue->final(context, m_queue->pop());
		m_ack_time.stop();
		m_pop_time.stop();
		m_pop_rate.update(1);
		m_ack_rate.update(1);

	} else if (event == "peek") {
		m_pop_time.start();
		ioremap::grape::entry_id entry_id;
		ioremap::elliptics::data_pointer d = m_queue->peek(&entry_id);

		COCAINE_LOG_INFO(m_log, "%s, peeked entry: %d-%d: (%ld)'%s'",
				action_id.c_str(),
				entry_id.chunk, entry_id.pos, d.size(), d.to_string()
				);

		// embed entry id directly into sph of the reply
		dnet_raw_id *src = context.src_id();
		memcpy(&src->id[DNET_ID_SIZE - sizeof(entry_id)], &entry_id, sizeof(entry_id));

		m_queue->final(context, d);

		m_pop_time.stop();
		m_pop_rate.update(1);

	} else if (event == "peek-multi") {
		m_pop_time.start();
		int num = stoi(context.data().to_string());

		peek_multi_type d = m_queue->peek(num);

		if (!d.empty()) {
			m_queue->final(context, ioremap::grape::serialize(d));
			m_pop_rate.update(d.sizes().size());
		} else {
			m_queue->final(context, ioremap::elliptics::data_pointer());
		}

		m_pop_time.stop();

		COCAINE_LOG_INFO(m_log, "%s, peeked %ld entries (asked %d)",
				action_id.c_str(),
				d.sizes().size(), num
				);

	} else if (event == "ack") {
		m_ack_time.start();
		ioremap::elliptics::data_pointer d = context.data();
		ioremap::grape::entry_id entry_id = ioremap::grape::entry_id::from_dnet_raw_id(context.src_id());

		m_queue->ack(entry_id);
		m_queue->final(context, ioremap::elliptics::data_pointer());

		m_ack_time.stop();
		m_ack_rate.update(1);

		COCAINE_LOG_INFO(m_log, "%s, acked entry %d-%d",
				action_id.c_str(),
				entry_id.chunk, entry_id.pos
				);

	} else if (event == "ack-multi") {
		m_ack_time.start();
		auto d = ioremap::grape::deserialize<ack_multi_type>(context.data());

		m_queue->ack(d);
		m_queue->final(context, ioremap::elliptics::data_pointer());

		m_ack_time.stop();
		m_ack_rate.update(d.size());

		COCAINE_LOG_INFO(m_log, "%s, acked %ld entries",
				action_id.c_str(),
				d.size()
				);

	} else if (event == "clear") {
		// clear queue content
		m_queue->clear();
		m_queue->final(context, ioremap::elliptics::data_pointer("ok"));

	} else if (event == "stats-clear") {
		m_queue->clear_counters();
		m_queue->final(context, ioremap::elliptics::data_pointer("ok"));

	} else if (event == "stats") {
		rapidjson::StringBuffer stream;
		rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(stream);
		rapidjson::Document root;

		root.SetObject();

		rapidjson::Value name;
		std::string qname = m_queue->queue_id();
		name.SetString(qname.c_str(), qname.size());

		ioremap::grape::queue_state state = m_queue->state();
		ioremap::grape::queue_statistics st = m_queue->statistics();

		root.AddMember("queue_id", name, root.GetAllocator());

		root.AddMember("high-id", state.chunk_id_push, root.GetAllocator());
		root.AddMember("low-id", state.chunk_id_ack, root.GetAllocator());

		root.AddMember("push.count", st.push_count, root.GetAllocator());
		root.AddMember("push.rate", m_push_rate.get(), root.GetAllocator());
		root.AddMember("push.time", m_push_time.get(), root.GetAllocator());
		root.AddMember("pop.count", st.pop_count, root.GetAllocator());
		root.AddMember("pop.rate", m_pop_rate.get(), root.GetAllocator());
		root.AddMember("pop.time", m_pop_time.get(), root.GetAllocator());
		root.AddMember("ack.count", st.ack_count, root.GetAllocator());
		root.AddMember("ack.rate", m_ack_rate.get(), root.GetAllocator());
		root.AddMember("ack.time", m_ack_time.get(), root.GetAllocator());
		root.AddMember("timeout.count", st.timeout_count, root.GetAllocator());
		root.AddMember("state.write_count", st.state_write_count, root.GetAllocator());

		root.AddMember("chunks_popped.write_data", st.chunks_popped.write_data, root.GetAllocator());
		root.AddMember("chunks_popped.write_meta", st.chunks_popped.write_meta, root.GetAllocator());
		root.AddMember("chunks_popped.read", st.chunks_popped.read, root.GetAllocator());
		root.AddMember("chunks_popped.remove", st.chunks_popped.remove, root.GetAllocator());
		root.AddMember("chunks_popped.push", st.chunks_popped.push, root.GetAllocator());
		root.AddMember("chunks_popped.pop", st.chunks_popped.pop, root.GetAllocator());
		root.AddMember("chunks_popped.ack", st.chunks_popped.ack, root.GetAllocator());

		root.AddMember("chunks_pushed.write_data", st.chunks_pushed.write_data, root.GetAllocator());
		root.AddMember("chunks_pushed.write_meta", st.chunks_pushed.write_meta, root.GetAllocator());
		root.AddMember("chunks_pushed.read", st.chunks_pushed.read, root.GetAllocator());
		root.AddMember("chunks_pushed.remove", st.chunks_pushed.remove, root.GetAllocator());
		root.AddMember("chunks_pushed.push", st.chunks_pushed.push, root.GetAllocator());
		root.AddMember("chunks_pushed.pop", st.chunks_pushed.pop, root.GetAllocator());
		root.AddMember("chunks_pushed.ack", st.chunks_pushed.ack, root.GetAllocator());

		root.Accept(writer);

		std::string text;
		text.assign(stream.GetString(), stream.GetSize());

		m_queue->final(context, text);

	} else {
		std::string msg = event + ": unknown event";
		m_queue->final(context, msg);
	}

	COCAINE_LOG_INFO(m_log, "%s, event: %s, data-size: %ld, completed",
			action_id.c_str(),
			event.c_str(), context.data().size()
			);
}

int main(int argc, char **argv)
{
	try {
		return cocaine::framework::run<queue_app_context>(argc, argv);
	} catch (const std::exception &e) {
		std::ofstream tmp("/tmp/queue.out");

		std::ostringstream out;
		out << "queue failed: " << e.what();

		tmp.write(out.str().c_str(), out.str().size());
	}
}
