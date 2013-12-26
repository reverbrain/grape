#include <boost/program_options.hpp>
#include <boost/algorithm/string.hpp>

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
		("remote,r", value<std::string>(), "remote addr to initially connect to")
		("group,g", value<std::vector<int>>()->multitoken(), "group(s) to connect to")
		("loglevel", value<int>()->default_value(0), "client loglevel")
		("net-thread-num", value<int>()->default_value(0), "client 'net thread' pool size")
		("io-thread-num", value<int>()->default_value(0), "client 'io thread' pool size")
		;

	options_description other("Options");
	elliptics.add_options()
		("concurrency,n", value<int>()->default_value(1), "concurrency limit")
		("limit,l", value<int>()->default_value(0), "upper limit")
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
	int net_thread_num = args["net-thread-num"].as<int>();
	int io_thread_num = args["io-thread-num"].as<int>();

	int concurrency = args["concurrency"].as<int>();
	int limit = args["limit"].as<int>();

	auto clientlib = elliptics_client_state::create(
			remotes, groups, logfile, loglevel,
			0, 0, net_thread_num, io_thread_num
			);

	const std::string queue_name("queue");

	// write queue indefinitely, with ever increasing number
	queue_writer pump(clientlib.create_session(), queue_name, concurrency);
	int counter = 0;
	pump.run([&counter, &limit] () {
		if (limit > 0 && counter >= limit) {
			return ioremap::elliptics::data_pointer();
		}
		return ioremap::elliptics::data_pointer::copy(std::to_string(counter++));
	});

	return 0;
}
