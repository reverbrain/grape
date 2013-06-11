#include <cocaine/framework/logging.hpp>
#include <cocaine/framework/application.hpp>
#include <cocaine/framework/worker.hpp>

#include <grape/elliptics_client_state.hpp>
#include <grape/entry_id.hpp>

using namespace ioremap::elliptics;

class app_context : public cocaine::framework::application<app_context>
{
public:
	// proxy to the logging service
	std::shared_ptr<cocaine::framework::logger_t> m_log;

	// elliptics client generator
	elliptics_client_state _elliptics_client_state;
    
	// reply delay, in milliseconds
	int _delay;
	std::string _queue_ack_event;

	app_context(const std::string &id, std::shared_ptr<cocaine::framework::service_manager_t> service_manager);
	void initialize();

	std::string process(const std::string &cocaine_event, const std::vector<std::string> &chunks);
};

app_context::app_context(const std::string &id, std::shared_ptr<cocaine::framework::service_manager_t> service_manager)
	: application<app_context>(id, service_manager)
{
	// obtain logging facility
	m_log = service_manager->get_system_logger();
	COCAINE_LOG_INFO(m_log, "application start: %s", id.c_str());

	_delay = 0;
	_queue_ack_event = "queue@ack";
}

void app_context::initialize()
{
	// configure
	//FIXME: replace this with config storage service when it's done
	{
		rapidjson::Document doc;
		_elliptics_client_state = elliptics_client_state::create("testerhead-cpp.conf", doc);

		if (doc.HasMember("delay")) {
			_delay = doc["delay"].GetInt() * 1000;
		}
		if (doc.HasMember("queue-ack-event")) {
			_queue_ack_event = doc["queue-ack-event"].GetString();
		}
	}

	// register event handlers
	on_unregistered(&app_context::process);
}

std::string app_context::process(const std::string &cocaine_event, const std::vector<std::string> &chunks)
{
	session client = _elliptics_client_state.create_session();

	exec_context context = exec_context::from_raw(chunks[0].c_str(), chunks[0].size());

	// auto reply_ack = [&client, &context] () {
	//     client.reply(context, std::string(), exec_context::final);
	// };
	// auto reply_error = [&client, &context] (const char *msg) {
	//     client.reply(context, std::string(msg), exec_context::final);
	// };
	auto reply = [&client, &context] (data_pointer d) {
		client.reply(context, d, exec_context::final);
	};

	ioremap::grape::entry_id id = ioremap::grape::entry_id::from_dnet_raw_id(context.src_id());
	COCAINE_LOG_INFO(m_log, "event: '%s', entry: %d-%d, data: '%s'",
			cocaine_event.c_str(),
			id.chunk, id.pos,
			context.data().to_string().c_str()
			);

	// std::string app;
	// std::string event;
	// {
	//     char *p = strchr((char*)context.event().c_str(), '@');
	//     app.assign(context.event().c_str(), p - context.event().c_str());
	//     event.assign(p + 1);
	// }

	if (_delay) {
		usleep(_delay);
	}

	// acking success
//	if (m_ack_on_success) {
		client.set_exceptions_policy(session::no_exceptions);

		dnet_id queue_id;
		dnet_setup_id(&queue_id, 0, context.src_id()->id);
		queue_id.type = 0;

		client.exec(&queue_id, _queue_ack_event, data_pointer()).connect(
				async_result<exec_result_entry>::result_function(),
				[m_log, id] (const error_info &error) {
					if (error) {
						COCAINE_LOG_ERROR(m_log, "entry %d-%d not acked", id.chunk, id.pos);
					} else {
						COCAINE_LOG_INFO(m_log, "entry %d-%d acked", id.chunk, id.pos);
					}
				}
		);
//	}

	return "return: " + context.data().to_string();
}

int main(int argc, char **argv)
{
	return cocaine::framework::worker_t::run<app_context>(argc, argv);
}
