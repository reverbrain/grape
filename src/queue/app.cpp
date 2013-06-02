#include "queue.hpp"

#include <cocaine/framework/logging.hpp>
#include <cocaine/framework/application.hpp>
#include <cocaine/framework/worker.hpp>

#include <fstream>

namespace {

template <unsigned N>
double approx_moving_average(double avg, double input) {
	avg -= avg/N;
	avg += input/N;
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

	uint64_t microseconds_now() {
		timespec t;
		clock_gettime(CLOCK_MONOTONIC_RAW, &t);
		return t.tv_sec * 1000000 + t.tv_nsec / 1000;
	}

	void update(size_t num) {
		for (size_t i = 0; i < num; ++i) {
			uint64_t now = microseconds_now();
			double elapsed = double(now - last_update) / 1000000; // in seconds
			double alpha = (elapsed > 1.0) ? 1.0 : elapsed;
			avg = exponential_moving_average(avg, (1.0 / elapsed), alpha);
			last_update = now;
		}
	}

	double get() {
		return avg;
	}
};

}

class queue_app_context : public cocaine::framework::application<queue_app_context>
{
	public:
		queue_app_context(const std::string &id, std::shared_ptr<cocaine::framework::service_manager_t> service_manager);
		virtual ~queue_app_context();

		void initialize();

		std::string process(const std::string &cocaine_event, const std::vector<std::string> &chunks);

	private:
		std::string m_id;
		std::shared_ptr<cocaine::framework::logger_t> m_log;
		std::shared_ptr<ioremap::grape::queue> m_queue;

		rate_stat m_rate_push;
		rate_stat m_rate_pop;
};

queue_app_context::queue_app_context(const std::string &id, std::shared_ptr<cocaine::framework::service_manager_t> service_manager):
application<queue_app_context>(id, service_manager),
m_id(id),
m_log(service_manager->get_system_logger())
{
}
queue_app_context::~queue_app_context()
{
}

void queue_app_context::initialize()
{
	on_unregistered(&queue_app_context::process);
}

std::string queue_app_context::process(const std::string &cocaine_event, const std::vector<std::string> &chunks)
{
	ioremap::elliptics::exec_context context = ioremap::elliptics::exec_context::from_raw(chunks[0].c_str(), chunks[0].size());

	std::string app;
	std::string event;
	{
		char *p = strchr((char*)context.event().c_str(), '@');
		app.assign(context.event().c_str(), p - context.event().c_str());
		event.assign(p + 1);
	}

	sph *s = (sph *)chunks[0].c_str();

	COCAINE_LOG_INFO(m_log, "%s: %s: event: %s, size: %ld",
			m_id.c_str(), dnet_dump_id_str(s->src.id), event.c_str(), context.data().size());

	if (!m_queue && event != "configure")
		ioremap::elliptics::throw_error(-EINVAL, "Worker '%s' is not configured", m_id.c_str());

	if (event == "ping") {
		m_queue->final(context, std::string("ok"));
	} else if (event == "configure") {
		if (!m_queue) {
			m_id = context.data().to_string() + "-" + m_id;
			m_queue.reset(new ioremap::grape::queue("queue.conf", m_id));
			m_queue->final(context, std::string(m_id + ": configured"));
			COCAINE_LOG_INFO(m_log, "%s: queue has been successfully configured", m_id.c_str());
		} else {
			m_queue->final(context, std::string(m_id + ": is already configured"));
			COCAINE_LOG_INFO(m_log, "%s: queue is already configured", m_id.c_str());
		}
	} else if (event == "push") {
		ioremap::elliptics::data_pointer d = context.data();
		// skip adding zero length data, because there is no value in that
		// queue has no method to request size and we can use zero reply in pop
		// to indicate queue emptiness
		if (d.size()) {
			m_queue->push(d);
			m_rate_push.update(1);
		}

		m_queue->final(context, std::string(m_id + ": ack"));
	} else if ((event == "pop") || (event == "pop-multiple-string")) {
		int num = 1;

		if (event == "pop-multiple-string") {
			num = atoi(context.data().to_string().c_str());
		}

		ioremap::grape::data_array d = m_queue->pop(num);
		m_rate_pop.update(d.sizes().size());

		if (d.empty()) {
			m_queue->final(context, ioremap::elliptics::data_pointer());
		} else {
			m_queue->final(context, d.serialize());
		}

		COCAINE_LOG_INFO(m_log, "%s: %s: completed event: %s, size: %ld, popped: %d/%d (multiple: '%s')",
				m_id.c_str(), dnet_dump_id_str(s->src.id), event.c_str(), context.data().size(), d.sizes().size(), num,
				context.data().to_string().c_str());
	} else if (event == "stats") {
		rapidjson::StringBuffer stream;
		rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(stream);
		rapidjson::Document root;

		root.SetObject();

		rapidjson::Value name;
		std::string qname = m_queue->queue_id();
		name.SetString(qname.c_str(), qname.size());

		struct ioremap::grape::queue_stat st = m_queue->stat();

		root.AddMember("queue_id", name, root.GetAllocator());
		root.AddMember("ack.count", st.ack_count, root.GetAllocator());
		root.AddMember("fail.count", st.fail_count, root.GetAllocator());
		root.AddMember("pop.count", st.pop_count, root.GetAllocator());
		root.AddMember("pop.rate", m_rate_pop.get(), root.GetAllocator());
		root.AddMember("push.count", st.push_count, root.GetAllocator());
		root.AddMember("push.rate", m_rate_push.get(), root.GetAllocator());
		root.AddMember("push-id", st.chunk_id_push, root.GetAllocator());
		root.AddMember("pop-id", st.chunk_id_pop, root.GetAllocator());
		root.AddMember("update_indexes", st.update_indexes, root.GetAllocator());

		root.AddMember("chunks_popped.write_data_async", st.chunks_popped.write_data_async, root.GetAllocator());
		root.AddMember("chunks_popped.write_data_sync", st.chunks_popped.write_data_sync, root.GetAllocator());
		root.AddMember("chunks_popped.write_ctl_async", st.chunks_popped.write_ctl_async, root.GetAllocator());
		root.AddMember("chunks_popped.write_ctl_sync", st.chunks_popped.write_ctl_sync, root.GetAllocator());
		root.AddMember("chunks_popped.read", st.chunks_popped.read, root.GetAllocator());
		root.AddMember("chunks_popped.remove", st.chunks_popped.remove, root.GetAllocator());
		root.AddMember("chunks_popped.push", st.chunks_popped.push, root.GetAllocator());
		root.AddMember("chunks_popped.pop", st.chunks_popped.pop, root.GetAllocator());
		root.AddMember("chunks_popped.ack", st.chunks_popped.ack, root.GetAllocator());

		root.AddMember("chunks_pushed.write_data_async", st.chunks_pushed.write_data_async, root.GetAllocator());
		root.AddMember("chunks_pushed.write_data_sync", st.chunks_pushed.write_data_sync, root.GetAllocator());
		root.AddMember("chunks_pushed.write_ctl_async", st.chunks_pushed.write_ctl_async, root.GetAllocator());
		root.AddMember("chunks_pushed.write_ctl_sync", st.chunks_pushed.write_ctl_sync, root.GetAllocator());
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

	COCAINE_LOG_INFO(m_log, "%s: %s: completed event: %s, size: %ld",
			m_id.c_str(), dnet_dump_id_str(s->src.id), event.c_str(), context.data().size());

	return "";
}

int main(int argc, char **argv)
{
	try {
		return cocaine::framework::worker_t::run<queue_app_context>(argc, argv);
	} catch (const std::exception &e) {
		std::ofstream tmp("/tmp/queue.out");

		std::ostringstream out;
		out << "queue failed: " << e.what();

		tmp.write(out.str().c_str(), out.str().size());
	}
}
