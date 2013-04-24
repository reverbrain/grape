#include <boost/filesystem.hpp>
#include <jsoncpp/json.hpp>
#include <cocaine/framework/logging.hpp>
#include <cocaine/framework/application.hpp>
#include <cocaine/framework/worker.hpp>
#include <elliptics/cppdef.h>

#include <grape/elliptics_client_state.hpp>

using namespace ioremap::elliptics;

class app_context : public cocaine::framework::application<app_context>
{
public:
	// proxy to the logging service
	std::shared_ptr<cocaine::framework::logger_t> _log;

	// elliptics client generator
	elliptics_client_state _elliptics_client_state;
    
    // reply delay, in milliseconds
    int _delay;

	app_context(std::shared_ptr<cocaine::framework::service_manager_t> service_manager);
	void initialize();

	std::string process(const std::string &cocaine_event, const std::vector<std::string> &chunks);
};

app_context::app_context(std::shared_ptr<cocaine::framework::service_manager_t> service_manager)
	: application<app_context>(service_manager)
{
	// obtain logging facility
	_log = service_manager->get_system_logger();

    _delay = 0;
}

void app_context::initialize()
{
	try {
		// configure
		//FIXME: replace this with config storage service when it's done
		{
			const char CONFFILE[] = "testerhead-cpp.conf";
			std::ifstream config;
			config.open(CONFFILE);
			if (!config.is_open()) {
				COCAINE_LOG_INFO(_log, "failed to open config file %s", CONFFILE);
				throw configuration_error_t("failed to open config file");
			}

			COCAINE_LOG_INFO(_log, "parsing config file");

			Json::Value args;
			Json::Reader reader;
			if (!reader.parse(config, args, false)) {
				COCAINE_LOG_INFO(_log, "can not parse config file %s", CONFFILE);
				throw configuration_error_t("can not parse config file");
			}

			COCAINE_LOG_INFO(_log, "creating elliptics client");

			_elliptics_client_state = elliptics_client_state::create(args);

            _delay = args.get("delay", "0").asUInt();
            COCAINE_LOG_INFO(_log, "reply delay = %d", _delay);
            // actual delay must be in microseconds
            _delay *= 1000;
		}

		COCAINE_LOG_INFO(_log, "registering event handlers");

		// register event handlers
		on_unregistered(&app_context::process);
	}
	catch (const std::exception &e) {
		COCAINE_LOG_ERROR(_log, "error in app_context::initialize: %s", e.what());
		throw;
	}
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

	COCAINE_LOG_INFO(_log, "cocaine event: %s", cocaine_event.c_str());

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

	reply(cocaine_event);

	return "";
}

namespace fs = boost::filesystem;

int main(int argc, char **argv)
{
	{
		fs::path pathname(argv[0]);
		std::string dirname = pathname.parent_path().string();
		if (chdir(dirname.c_str())) {
			fprintf(stderr, "Can't set working directory to app's spool dir: %m");
		}
	}

	return cocaine::framework::worker_t::run<app_context>(argc, argv);
}
