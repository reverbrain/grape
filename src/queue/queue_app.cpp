#include <unistd.h>
#include <json/json.h>
#include <cocaine/framework/logging.hpp>
#include <cocaine/framework/application.hpp>
#include <cocaine/framework/worker.hpp>
#include <elliptics/cppdef.h>

#include <grape/elliptics_client_state.hpp>
#include "queue.hpp"

#include <fstream>

using namespace ioremap::elliptics;

namespace {

void print_cwd(int line) {
	std::ofstream f;
	f.open("/home/ijon/proj/launchpad/cwd.log", std::ios_base::app);
	char CWD[1024];
	getcwd(CWD, 1023);
	f << line << " " << std::string(CWD) << std::endl;
	f.flush();
	f.close();
}

#include <stdarg.h>
#define debug_log(...) _debug_log(__LINE__, __VA_ARGS__)
void _debug_log(int line, const char *format, ...) {
	FILE *f = fopen("/home/ijon/proj/launchpad/debug.log", "a");
	fprintf(f, "%d ", line);
	va_list a;
	va_start(a, format);
	vfprintf(f, format, a);
	va_end(a);
	fprintf(f, "\n");
	fflush(f);
	fclose(f);
}

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

class app_context : public cocaine::framework::application<app_context>
{
public:
	queue_t _queue;
	std::mutex _queue_mutex;

	// proxy to the logging service
	std::shared_ptr<cocaine::framework::logger_t> _log;

	// elliptics client generator
	elliptics_client_state _elliptics_client_state;

	int64_t _ack_count;
	rate_stat _ack_rate;
	int64_t _fail_count;
	rate_stat _fail_rate;
	int64_t _push_count;
	rate_stat _push_rate;
	int64_t _pop_count;
	rate_stat _pop_rate;

	app_context(std::shared_ptr<cocaine::framework::service_manager_t> service_manager);
	void initialize();

	std::string process(const std::string &cocaine_event, const std::vector<std::string> &chunks);
};

app_context::app_context(std::shared_ptr<cocaine::framework::service_manager_t> service_manager)
	: application<app_context>(service_manager)
	, _ack_count(0)
	, _fail_count(0)
	, _pop_count(0)
	, _push_count(0)
{
	// first of all obtain logging facility
	_log = service_manager->get_system_logger();
	debug_log("_log: %p", _log.get());

	//FIXME: pass logger explicitly everywhere
	extern void _queue_module_set_logger(std::shared_ptr<cocaine::framework::logger_t>);
	_queue_module_set_logger(_log);
}

void app_context::initialize()
{
	try {
		// configure
		//FIXME: replace this with config storage service when it's done
		{
			const char CONFFILE[] = "queue.conf";
			std::ifstream config;
			config.open(CONFFILE);
			if (!config.is_open()) {
				COCAINE_LOG_INFO(_log, "failed to open config file %s", CONFFILE);
				debug_log("failed to open config file");
				throw configuration_error_t("failed to open config file");
			}
 
			COCAINE_LOG_INFO(_log, "parsing config file");
			debug_log("parsing config file");

			Json::Value args;
			Json::Reader reader;
			if (!reader.parse(config, args, false)) {
				COCAINE_LOG_INFO(_log, "can not parse config file %s", CONFFILE);
				debug_log("can not parse config file");
				throw configuration_error_t("can not parse config file");
			}

			COCAINE_LOG_INFO(_log, "creating elliptics client");
			debug_log("creating elliptics client");
			{
				Json::Value remotesArray = args.get("remotes", Json::arrayValue);
				for (Json::ArrayIndex i = 0; i < remotesArray.size(); ++i) {
					COCAINE_LOG_INFO(_log, "remote %s", remotesArray[i].asCString());
				}
			}

			_elliptics_client_state = elliptics_client_state::create(args);
		}

		COCAINE_LOG_INFO(_log, "registering event handlers");
		debug_log("registering event handlers");

		// register event handlers
		//FIXME: all at once for now
		on_unregistered(&app_context::process);

		COCAINE_LOG_INFO(_log, "app_context initialized");
	}
	catch (const std::exception &e) {
		COCAINE_LOG_ERROR(_log, "error in initialize: %s", e.what());
		throw;
	}
}

std::string app_context::process(const std::string &cocaine_event, const std::vector<std::string> &chunks)
{
	session client = _elliptics_client_state.create_session();

	exec_context context = exec_context::from_raw(chunks[0].c_str(), chunks[0].size());

	auto reply_ack = [&client, &context] () {
		client.reply(context, std::string(), exec_context::final);
	};
	auto reply_error = [&client, &context] (const char *msg) {
		client.reply(context, std::string(msg), exec_context::final);
	};
	auto reply = [&client, &context] (data_pointer d) {
		client.reply(context, d, exec_context::final);
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
		std::lock_guard<std::mutex> lock(_queue_mutex);

		COCAINE_LOG_INFO(_log, "queue id: %d", _queue._id);

		data_pointer d = context.data();
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
		_push_rate.update();

	} else if (event == "pop") {
		std::lock_guard<std::mutex> lock(_queue_mutex);

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

		++_pop_count;
		_pop_rate.update();

/*    } else if (event == "peek") {
		std::lock_guard<std::mutex> lock(_queue_mutex);

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
		//std::lock_guard<std::mutex> lock(_queue_mutex);

		reply_ack();

		++_ack_count;
		_ack_rate.update();

	} else if (event == "fail") {
		//std::lock_guard<std::mutex> lock(_queue_mutex);

		reply_ack();

		++_fail_count;
		_fail_rate.update();

	} else if (event == "new-id") {
		std::lock_guard<std::mutex> lock(_queue_mutex);

		int id = std::stoi(context.data().to_string());
		if (id < 0) {
			reply_error("new-id: queue id must be positive integer");
			return "";
		}
		_queue.new_id(&client, id);
		reply(std::to_string(id));

	} else if (event == "existing-id") {
		std::lock_guard<std::mutex> lock(_queue_mutex);

		int id = std::stoi(context.data().to_string());
		if (id < 0) {
			reply_error("existing-id: queue id must be positive interger");
			return "";
		}
		_queue.existing_id(&client, id);
		reply(std::to_string(id));

	} else if (event == "state") {
		std::lock_guard<std::mutex> lock(_queue_mutex);

		std::string text;
		_queue.dump_state(&text);
		reply(text);

	} else if (event == "stats") {
		std::lock_guard<std::mutex> lock(_queue_mutex);

		std::string text;
		{
			Json::StyledWriter writer;
			Json::Value root;

			root["ack.count"] = Json::Value::Int64(_ack_count);
			root["ack.rate"] = Json::Value(_ack_rate.get());
			root["fail.count"] = Json::Value::Int64(_fail_count);
			root["fail.rate"] = Json::Value(_fail_rate.get());
			root["pop.count"] = Json::Value::Int64(_pop_count);
			root["pop.rate"] = Json::Value(_pop_rate.get());
			root["push.count"] = Json::Value::Int64(_push_count);
			root["push.rate"] = Json::Value(_push_rate.get());

			text = writer.write(root);
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
