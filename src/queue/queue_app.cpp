#include "grape/elliptics_client_state.hpp"
#include "queue.hpp"

#include <cocaine/framework/logging.hpp>
#include <cocaine/framework/application.hpp>
#include <cocaine/framework/worker.hpp>

#include <unistd.h>
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

	void update() {
		uint64_t now = microseconds_now();
		double elapsed = double(now - last_update) / 1000000; // in seconds
		double alpha = (elapsed > 1.0) ? 1.0 : elapsed;
		avg = exponential_moving_average(avg, (1.0 / elapsed), alpha);
		last_update = now;
	}

	double get() {
		return avg;
	}
};

}

class queue_app_context : public cocaine::framework::application<app_context>
{
public:
	queue_t _queue;

	// proxy to the logging service
	std::shared_ptr<cocaine::framework::logger_t> _log;

	// elliptics client generator
	elliptics_client_state _elliptics_client_state;

	long _ack_count;
	long _fail_count;
	long _push_count;
	long _pop_count;

	app_context(std::shared_ptr<cocaine::framework::service_manager_t> service_manager);
	void initialize();

	std::string process(const std::string &cocaine_event, const std::vector<std::string> &chunks);
};

app_context::app_context(std::shared_ptr<cocaine::framework::service_manager_t> service_manager)
	: application<app_context>(service_manager)
	, _ack_count(0)
	, _fail_count(0)
	, _push_count(0)
	, _pop_count(0)
{
	// first of all obtain logging facility
	_log = service_manager->get_system_logger();

	//FIXME: pass logger explicitly everywhere
	extern void _queue_module_set_logger(std::shared_ptr<cocaine::framework::logger_t>);
	_queue_module_set_logger(_log);
}

void app_context::initialize()
{
	// configure
	//FIXME: replace this with config storage service when it's done
	{
		rapidjson::Document doc;
		_elliptics_client_state = elliptics_client_state::create("queue.conf", doc);
	}

	// register event handlers
	//FIXME: all at once for now
	on_unregistered(&app_context::process);
}

std::string app_context::process(const std::string &cocaine_event, const std::vector<std::string> &chunks)
{
	ioremap::elliptics::session client = _elliptics_client_state.create_session();

	ioremap::elliptics::exec_context context = ioremap::elliptics::exec_context::from_raw(chunks[0].c_str(), chunks[0].size());

	auto reply_ack = [&client, &context] () {
		client.reply(context, std::string("queue@ack: final"), ioremap::elliptics::exec_context::final);
	};
	auto reply_error = [&client, &context] (const char *msg) {
		client.reply(context, std::string(msg), ioremap::elliptics::exec_context::final);
	};
	auto reply = [&client, &context] (ioremap::elliptics::data_pointer d) {
		client.reply(context, d, ioremap::elliptics::exec_context::final);
	};

	std::string app;
	std::string event;
	{
		char *p = strchr((char*)context.event().c_str(), '@');
		app.assign(context.event().c_str(), p - context.event().c_str());
		event.assign(p + 1);
	}

	COCAINE_LOG_INFO(_log, "event: %s", event.c_str());

	if (event == "ping") {
		reply(std::string("ok"));

	} else if (event == "push") {
		COCAINE_LOG_INFO(_log, "queue id: %d", _queue._id);

		ioremap::elliptics::data_pointer d = context.data();
		// skip adding zero length data, because there is no value in that
		// queue has no method to request size and we can use zero reply in pop
		// to indicate queue emptiness
		if (d.size()) {
			COCAINE_LOG_INFO(_log, "push data: %s", d.to_string().c_str());
			_queue.push(&client, d);
		} else {
			COCAINE_LOG_INFO(_log, "skipping empty push");
		}
		reply_ack();

		++_push_count;

	} else if (event == "pop") {
		COCAINE_LOG_INFO(_log, "queue id: %d", _queue._id);

		ioremap::elliptics::data_pointer d;
		size_t size = 0;
		_queue.pop(&client, &d, &size);
		// zero length item mean that queue is empty and there is nothing to pop
		// also pop is idempotent
		if (size) {
			COCAINE_LOG_INFO(_log, "returning item: %s", d.to_string().c_str());
			reply(ioremap::elliptics::data_pointer::copy(d.data(), size));
		} else {
			reply_ack();
		}

		++_pop_count;

/*    } else if (event == "peek") {
		COCAINE_LOG_INFO(_log, "queue id: %d", _queue._id);

		data_pointer d;
		size_t size = 0;
		_queue.pop(&client, &d, &size);
		// zero length item mean that queue is empty and there is nothing to pop
		// also pop is idempotent
		if (size) {
			COCAINE_LOG_INFO(_log, "returning item: %s", d.to_string().c_str());
			reply(data_pointer::copy(d.data(), size));
		} else {
			reply_ack();
		}
*/
	} else if (event == "ack") {
		reply_ack();

		++_ack_count;

	} else if (event == "fail") {
		reply_ack();

		++_fail_count;

	} else if (event == "new-id") {
		int id = std::stoi(context.data().to_string());
		if (id < 0) {
			reply_error("new-id: queue id must be positive integer");
			return "";
		}
		_queue.new_id(&client, id);
		reply(std::to_string(id));

	} else if (event == "existing-id") {
		int id = std::stoi(context.data().to_string());
		if (id < 0) {
			reply_error("existing-id: queue id must be positive interger");
			return "";
		}
		_queue.existing_id(&client, id);
		reply(std::to_string(id));

	} else if (event == "state") {
		std::string text;
		_queue.dump_state(&text);
		reply(text);

	} else if (event == "stats") {
		std::string text;
		{
			rapidjson::StringBuffer stream;
			rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(stream);
			rapidjson::Document root;

			root.SetObject();
			root.AddMember("ack.count", _ack_count, root.GetAllocator());
			root.AddMember("fail.count", _fail_count, root.GetAllocator());
			root.AddMember("pop.count", _pop_count, root.GetAllocator());
			root.AddMember("push.count", _push_count, root.GetAllocator());

			root.Accept(writer);
			text.assign(stream.GetString(), stream.GetSize());
		}
		reply(text);

	} else {
		std::string msg = "unknown event name: ";
		msg += event;
		reply_error(msg.c_str());
	}

	return "";
}

int main(int argc, char **argv)
{
	return cocaine::framework::worker_t::run<app_context>(argc, argv);
}
