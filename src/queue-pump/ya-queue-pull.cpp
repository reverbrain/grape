#include <boost/program_options.hpp>

#include <grape/elliptics_client_state.hpp>
#include <grape/concurrent-pump.hpp>

using namespace boost::program_options;
using namespace ioremap::grape;

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

	options_description other("Options");
	other.add_options()
		("concurrency,n", value<int>()->default_value(1), "concurrency limit")
		("request-size,s", value<int>()->default_value(100), "request size")
		;

	options_description opts;
	opts.add(generic).add(elliptics).add(other);

	parsed_options parsed_opts = parse_command_line(argc, argv, opts);
	variables_map args;
	store(parsed_opts, args);
	notify(args);

	if (args.count("help")) {
		std::cout << "Queue support utility." << "\n";
		std::cout << opts << "\n";
		return 1;
	}

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

	int concurrency = args["concurrency"].as<int>();
	int request_size = args["request-size"].as<int>();;

	auto clientlib = elliptics_client_state::create(remotes, groups, logfile, loglevel);

	const std::string queue_name("queue");

	// read queue indefinitely
	ya_concurrent_queue_reader pump(clientlib.create_session(), queue_name, request_size, concurrency);
	pump.run([] (ioremap::grape::entry_id entry_id, ioremap::elliptics::data_pointer data) -> int {
		fprintf(stderr, "entry %d-%d, byte size %ld\n", entry_id.chunk, entry_id.pos, data.size());
		return ya_concurrent_queue_reader::ACK_THIS;
	});

	return 0;
}
