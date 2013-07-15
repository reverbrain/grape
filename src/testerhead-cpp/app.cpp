#include <cocaine/framework/dispatch.hpp>
#include <cocaine/framework/logging.hpp>

#include <grape/elliptics_client_state.hpp>
#include <grape/entry_id.hpp>
#include <grape/data_array.hpp>

using namespace ioremap::elliptics;

class app_context {
public:
	// proxy to the logging service
	std::shared_ptr<cocaine::framework::logger_t> m_log;

	// elliptics client generator
	elliptics_client_state _elliptics_client_state;
    
	// reply delay, in milliseconds
	int _delay;
	std::string _queue_ack_event;

	app_context(cocaine::framework::dispatch_t& dispatch);

	// void single_entry(const std::string &cocaine_event, const std::vector<std::string> &chunks, cocaine::framework::response_ptr response);
	// void multi_entry(const std::string &cocaine_event, const std::vector<std::string> &chunks, cocaine::framework::response_ptr response);
	void process(const std::string &cocaine_event, const std::vector<std::string> &chunks, cocaine::framework::response_ptr response);
};

app_context::app_context(cocaine::framework::dispatch_t& dispatch) {
	// obtain logging facility
	m_log = dispatch.service_manager()->get_system_logger();
	COCAINE_LOG_INFO(m_log, "application start: %s", dispatch.id().c_str());

	_delay = 0;
	_queue_ack_event = "queue@ack";

    // configure
	//FIXME: replace this with config storage service when it's done
	{
		rapidjson::Document doc;
		_elliptics_client_state = elliptics_client_state::create("testerhead-cpp.conf", doc);

		if (doc.HasMember("delay")) {
			_delay = doc["delay"].GetInt() * 1000;
		}
		if (doc.HasMember("source-queue-ack-event")) {
			_queue_ack_event = doc["source-queue-ack-event"].GetString();
		}
	}

	// register event handlers

	//XXX: it seems that "unregistered" entry has higher precedence over all
	// more specific ones: "unregistered" highjacks all events.
	// It's so counter intuitive, and most propably is a bug.
	// cocaine-framework-native version 0.10.5~prerelease~0.

	// on("single-entry", &app_context::single_entry);
	// on("multi-entry", &app_context::multi_entry);
	dispatch.on("testerhead-cpp@single-entry", this, &app_context::process);
	dispatch.on("testerhead-cpp@multi-entry", this, &app_context::process);
}

void app_context::process(const std::string &cocaine_event, const std::vector<std::string> &chunks, cocaine::framework::response_ptr response)
{
	session client = _elliptics_client_state.create_session();

	exec_context context = exec_context::from_raw(chunks[0].c_str(), chunks[0].size());

	// auto reply_ack = [&client, &context] () {
	//     client.reply(context, std::string(), exec_context::final);
	// };
	// auto reply_error = [&client, &context] (const char *msg) {
	//     client.reply(context, std::string(msg), exec_context::final);
	// };
	// auto reply = [&client, &context] (data_pointer d) {
	// 	client.reply(context, d, exec_context::final);
	// };

	std::string app;
	std::string event;
	{
		char *p = strchr((char*)cocaine_event.c_str(), '@');
		app.assign(cocaine_event.c_str(), p - cocaine_event.c_str());
		event.assign(p + 1);
	}

	const std::string action_id = cocaine::format("%s %d", dnet_dump_id_str(context.src_id()->id), context.src_key());

	COCAINE_LOG_INFO(m_log, "%s, event: %s, data-size: %ld",
			action_id.c_str(),
			event.c_str(), context.data().size()
			);

	if (_delay) {
		usleep(_delay);
	}

	if (event == "single-entry") {
		ioremap::grape::entry_id id = ioremap::grape::entry_id::from_dnet_raw_id(context.src_id());
		COCAINE_LOG_INFO(m_log, "received entry: %d-%d", id.chunk, id.pos);

		// acking success
		if (_queue_ack_event != "none") {
			client.set_exceptions_policy(session::no_exceptions);

			// dnet_id queue_id;
			// dnet_setup_id(&queue_id, 0, context.src_id()->id);

			COCAINE_LOG_INFO(m_log, "%s, acking entry %d-%d to queue %s",
					action_id.c_str(), id.chunk, id.pos, dnet_dump_id_str(context.src_id()->id));

			client.exec(context, _queue_ack_event, data_pointer()).connect(
					async_result<exec_result_entry>::result_function(),
					[this, action_id, id] (const error_info &error) {
						if (error) {
							COCAINE_LOG_ERROR(m_log, "%s, entry %d-%d not acked",
								action_id.c_str(), id.chunk, id.pos);
						} else {
							COCAINE_LOG_INFO(m_log, "%s, entry %d-%d acked",
								action_id.c_str(), id.chunk, id.pos);
						}
					}
			);
		}

	} else if (event == "multi-entry") {
		// acking success
		if (_queue_ack_event != "none") {
			client.set_exceptions_policy(session::no_exceptions);

			auto d = ioremap::grape::deserialize<ioremap::grape::data_array>(context.data());
			size_t count = d.ids().size();

			COCAINE_LOG_INFO(m_log, "%s, acking multi entry, size %ld, to queue %s",
					action_id.c_str(), count, dnet_dump_id_str(context.src_id()->id));

			// send back only a vector with ids
			client.exec(context, _queue_ack_event, ioremap::grape::serialize(d.ids())).connect(
					async_result<exec_result_entry>::result_function(),
					[this, action_id, count] (const error_info &error) {
						if (error) {
							COCAINE_LOG_ERROR(m_log, "%s, %ld entries not acked: %s",
								action_id.c_str(), count, error.message().c_str());
						} else {
							COCAINE_LOG_INFO(m_log, "%s, %ld entries acked", action_id.c_str(), count);
						}
					}
			);
		}
	} else if (event == "ping") {
		client.reply(context, std::string("ok"), ioremap::elliptics::exec_context::final);
	}
}
/*
void app_context::single_entry(const std::string &cocaine_event, const std::vector<std::string> &chunks, cocaine::framework::response_ptr response)
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

	if (_delay) {
		usleep(_delay);
	}

	// acking success
//	if (m_ack_on_success) {
		client.set_exceptions_policy(session::no_exceptions);

		dnet_id queue_id;
		dnet_setup_id(&queue_id, 0, context.src_id()->id);
		queue_id.type = 0;

		COCAINE_LOG_INFO(m_log, "acking entry %d-%d to queue %s", id.chunk, id.pos, dnet_dump_id_str(context.src_id()->id));

		client.exec(&queue_id, _queue_ack_event, data_pointer()).connect(
				async_result<exec_result_entry>::result_function(),
				[this, id] (const error_info &error) {
					if (error) {
						COCAINE_LOG_ERROR(m_log, "entry %d-%d not acked", id.chunk, id.pos);
					} else {
						COCAINE_LOG_INFO(m_log, "entry %d-%d acked", id.chunk, id.pos);
					}
				}
		);
//	}
}

void app_context::multi_entry(const std::string &cocaine_event, const std::vector<std::string> &chunks, cocaine::framework::response_ptr response)
{
	session client = _elliptics_client_state.create_session();

	exec_context context = exec_context::from_raw(chunks[0].c_str(), chunks[0].size());

	// auto reply_ack = [&client, &context] () {
	//     client.reply(context, std::string(), exec_context::final);
	// };
	// auto reply_error = [&client, &context] (const char *msg) {
	//     client.reply(context, std::string(msg), exec_context::final);
	// };
	// auto reply = [&client, &context] (data_pointer d) {
	// 	client.reply(context, d, exec_context::final);
	// };

	ioremap::grape::entry_id id = ioremap::grape::entry_id::from_dnet_raw_id(context.src_id());
	COCAINE_LOG_INFO(m_log, "event: '%s', entry: %d-%d, data: '%s'",
			cocaine_event.c_str(),
			id.chunk, id.pos,
			context.data().to_string().c_str()
			);

	if (_delay) {
		usleep(_delay);
	}

	// acking success
//	if (m_ack_on_success) {
		client.set_exceptions_policy(session::no_exceptions);

		dnet_id queue_id;
		dnet_setup_id(&queue_id, 0, context.src_id()->id);
		queue_id.type = 0;

		ioremap::grape::data_array d = ioremap::grape::data_array::deserialize(context.data());
		size_t count = d.ids().size();

		//FIXME: drop data, leave in the reply only ids 
		client.exec(&queue_id, _queue_ack_event, context.data()).connect(
				async_result<exec_result_entry>::result_function(),
				[this, count] (const error_info &error) {
					if (error) {
						COCAINE_LOG_ERROR(m_log, "%ld entries not acked", count);
					} else {
						COCAINE_LOG_INFO(m_log, "%ld entries acked", count);
					}
				}
		);
//	}
}
*/
int main(int argc, char **argv)
{
	return cocaine::framework::run<app_context>(argc, argv);
}
