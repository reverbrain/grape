#include <boost/program_options.hpp>
#include <boost/algorithm/string.hpp>

#include <cocaine/framework/logging.hpp>
#include <elliptics/cppdef.h>

#include <grape/elliptics_client_state.hpp>
#include "queue.hpp"

using namespace boost::program_options;
using namespace ioremap;

// Adaptor that converts elliptics::logger into cocaine::logging::logger_t
//NOTE: dnet_sink_t from srw module already does that, make it accessible here and drop this reimplementation
struct elliptics_logger_adaptor: public cocaine::framework::logger_t
{
	elliptics::logger &logger;
	std::string source;

	elliptics_logger_adaptor(elliptics::logger &logger, const std::string &source)
		: logger(logger), source(source)
	{}

	static cocaine::logging::priorities from_loglevel(int loglevel) {
		return cocaine::logging::priorities(loglevel);
	}
	static int from_priority(cocaine::logging::priorities priority) {
		return int(priority);
	}

	// cocaine::logging::logger_t
	virtual cocaine::logging::priorities verbosity() const {
		return from_loglevel(logger.get_log_level());
	}

	virtual void emit(cocaine::logging::priorities priority, const std::string& message) {
		std::string msg;
		msg += source;
		msg += ": ";
		msg += message;
		msg += "\n";
		logger.log(from_priority(priority), msg.c_str());
	}
};

int main(int argc, char** argv)
{
	options_description generic("Generic options");
	generic.add_options()
		("help", "help message")
		;

	options_description elliptics("Elliptics options");
	elliptics.add_options()
		("remote,r", value<std::string>(), "remote elliptics node addr to connect to")
		("group,g", value<std::vector<int>>()->multitoken(), "group(s) to connect to")
		("loglevel", value<int>()->default_value(0), "elliptics node loglevel")
		;

	options_description lowlevel("Queue data access and lowlevel actions");
	lowlevel.add_options()
		("id", value<int>(), "id of the queue to work with")
		("get-state", "find and show state record")
		("get-block", value<uint64_t>(), "find and show block record")
		//("get", value<int>(), "get item at specified index")
		;

	options_description highlevel("Queue API methods");
	highlevel.add_options()
		("new", "recreate queue before anything else")
		("dump-state", "call queue.dump_state")
		("push", value<std::vector<std::string>>()->implicit_value(std::vector<std::string>(1, "abcdf"), "abcdf"), "call queue.push")
		//NOTE: construct below is a hack to allow multiple occurencies of --pop and without any args
		("pop", value<std::vector<bool>>()->zero_tokens(), "call queue.pop")
		;

	options_description opts;
	opts.add(generic).add(elliptics).add(lowlevel).add(highlevel);

	parsed_options parsed_opts = parse_command_line(argc, argv, opts);
	variables_map args;
	store(parsed_opts, args);
	notify(args);

	if (args.count("help")) {
		std::cout << "Queue support utility." << "\n";
		std::cout << opts << "\n";
		return 1;
	}

	if (!args.count("id")) {
		std::cerr << "--id option required" << "\n";
		return 1;
	}
	int id(args["id"].as<int>());

	if (!args.count("remote"))
	{
		std::cerr << "--remote option required" << "\n";
		return 1;
	}

	if (!args.count("group")) {
		std::cerr << "--group option required" << "\n";
		return 1;
	}

	std::vector<std::string> remotes;
	remotes.push_back(args["remote"].as<std::string>());

	std::vector<int> groups = args["group"].as<std::vector<int>>();

	std::string logfile = "/dev/stderr";

	int loglevel = args["loglevel"].as<int>();

	elliptics_client_state clientlib = elliptics_client_state::create(remotes, groups, logfile, loglevel);
	elliptics::session client = clientlib.create_session();

	extern void _queue_module_set_logger(std::shared_ptr<cocaine::framework::logger_t>);
	std::shared_ptr<cocaine::framework::logger_t> log(new elliptics_logger_adaptor(*clientlib.logger, "queue-tool"));
	_queue_module_set_logger(log);

	queue_t queue;

	// lowlevel actions
	auto read = [&] (const std::string &name) {
		try {
			elliptics::key key(name);
			elliptics::sync_read_result result = client.read_latest(key, 0, 0);
			elliptics::data_pointer d = result[0].file();
			std::cout << d.to_string() << "\n";
		} catch (const ioremap::elliptics::not_found_error &e) {
			fprintf(stderr, "%s: not found\n", name.c_str());
		}
	};

	if (args.count("get-state")) {
		std::string name = make_queue_key("queue", id);
		read(name);
	}

	if (args.count("get-block")) {
		uint64_t block_id = args["get-block"].as<uint64_t>();
		std::string name = make_block_key("queue", id, block_id);
		read(name);
	}

	// highlevel actions (using queue methods)
	if (args.count("new")) {
		queue.new_id(&client, id);
	} else {
		queue.existing_id(&client, id);
	}

	if (args.count("dump-state")) {
		std::string text;
		queue.dump_state(&text);
		std::cout << text;
	}

	if (args.count("push")) {
		const std::vector<std::string> &values = args["push"].as<std::vector<std::string>>();
		std::for_each(values.begin(), values.end(),
			[&queue, &client] (const std::string &data) {
				queue.push(&client, data);
		});
	}

	if (args.count("pop")) {
		for (size_t i = 0; i < args.count("pop"); ++i) {
			ioremap::elliptics::data_pointer d;
			size_t size = 0;
			queue.pop(&client, &d, &size);
			// zero length item mean that queue is empty and there is nothing to pop
			// also pop is idempotent
			if (size) {
				std::cout << std::string(d.data<char>(), size) << "\n";
			}
		}
	}

/*
	std::vector<option> actions;
	for (const auto &i : parsed_opts.options) {
		std::cout << i.string_key << std::endl;
	}

	std::vector<option> actions;
	copy_if(parsed_opts.options.begin(), parsed_opts.options.end(), actions.begin(), [&highlevel] (const option &a) {
		return (highlevel.find_nothrow(a.string_key, true) != nullptr);
	});

	std::cout << "AAA" << std::endl;

	for (const auto &i : actions) {
		//NOTE: skipping 'new' option as its not really an action

		std::cout << i.string_key << std::endl;
		if (i.string_key == "dump-state") {
			std::string text;
			queue.dump_state(&text);
			std::cout << text;
		} else if (i.string_key == "push") {
			const std::vector<std::string> &values = args["push"].as<std::vector<std::string>>();
			std::for_each(values.begin(), values.end(),
				[&queue, &client] (const std::string &data) {
					queue.push(&client, data);
			});
		} else if (i.string_key == "pop") {
			data_pointer d;
			size_t size = 0;
			queue.pop(&client, &d, &size);
			// zero length item mean that queue is empty and there is nothing to pop
			// also pop is idempotent
			if (size) {
				std::cout << std::string(d.data<char>(), size) << "\n";
			}
		}
	}
*/
	return 0;
}
