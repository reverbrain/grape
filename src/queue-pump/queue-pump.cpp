#include <atomic>
#include <iostream>
#include <mutex>
#include <condition_variable>

#include <boost/program_options.hpp>
#include <boost/algorithm/string.hpp>

#include <grape/elliptics_client_state.hpp>
#include <grape/data_array.hpp>
#include <grape/entry_id.hpp>

struct request {
	dnet_id id;
	int src_key;

	request() : src_key(0) {
		memset(&id, 0, sizeof(dnet_id));
	}
	request(int src_key) : src_key(src_key) {
		memset(&id, 0, sizeof(dnet_id));
	}
};

class queue_pump
{
public:
	typedef std::function<bool (ioremap::grape::entry_id, ioremap::elliptics::data_pointer data)> processing_function;

private:
	ioremap::elliptics::session client;
	const std::string queue_name;
	const int request_size;
	processing_function proc;

	std::atomic_int next_request_id;
	std::atomic_int running_requests;
	std::mutex mutex;
	std::condition_variable condition;

public:
	queue_pump(ioremap::elliptics::session client, const std::string &queue_name, int request_size)
		: client(client)
		, queue_name(queue_name)
		, request_size(request_size)
		, running_requests(0)
		, next_request_id(0)
	{}

	void run(processing_function func) {
		proc = func;

		srand(time(NULL));

		const int concurrency_limit = 1;

		while(1) {
			while(running_requests < concurrency_limit) {
				++running_requests;
				queue_peek(client, next_request_id++, request_size);
				//fprintf(stderr, "%d running_requests\n", (int)running_requests);
			}
			std::unique_lock<std::mutex> lock(mutex);
			condition.wait(lock, [this]{return running_requests < concurrency_limit;});
		}
	}

	void queue_peek(ioremap::elliptics::session client, int req_unique_id, int arg)
	{
		client.set_exceptions_policy(ioremap::elliptics::session::no_exceptions);

		std::string queue_key = std::to_string(req_unique_id) + std::to_string(rand());

		auto req = std::make_shared<request>(req_unique_id);
		client.transform(queue_key, req->id);

		client.exec(&req->id, req->src_key, "queue@peek-multi", std::to_string(arg))
			.connect(
				std::bind(&queue_pump::data_received, this, req, std::placeholders::_1),
				std::bind(&queue_pump::request_complete, this, req, std::placeholders::_1)
			);
	}

	void queue_ack(ioremap::elliptics::session client,
			std::shared_ptr<request> req,
			ioremap::elliptics::exec_context context,
			const std::vector<ioremap::grape::entry_id> &ids)
	{
		client.set_exceptions_policy(ioremap::elliptics::session::no_exceptions);

		size_t count = ids.size();
		client.exec(context, "queue@ack-multi", ioremap::grape::serialize(ids))
			.connect(
				ioremap::elliptics::async_result<ioremap::elliptics::exec_result_entry>::result_function(),
				[req, count] (const ioremap::elliptics::error_info &error) {
					if (error) {
						fprintf(stderr, "%s %d, %ld entries not acked: %s\n", dnet_dump_id(&req->id), req->src_key, count, error.message().c_str());
					} else {
						fprintf(stderr, "%s %d, %ld entries acked\n", dnet_dump_id(&req->id), req->src_key, count);
					}
				}
			);
	}

	void data_received(std::shared_ptr<request> req, const ioremap::elliptics::exec_result_entry &result)
	{
		if (result.error()) {
			fprintf(stderr, "%s %d: error: %s\n", dnet_dump_id(&req->id), req->src_key, result.error().message().c_str());
			return;
		}

		ioremap::elliptics::exec_context context = result.context();

		// queue.peek returns no data when queue is empty.
		if (context.data().empty()) {
			return;
		}

		// Received context must be used for acking to the same queue instance.
		//
		// But before that context.src_key must be restored back
		// to the original src_key used in the original request to the queue,
		// or else our worker's ack will not be routed to the exact same
		// queue worker that issued reply with this context.
		//
		// (src_key of the request gets replaced by job id server side,
		// so reply does not carries the same src_key as a request.
		// Which is unfortunate.)
		context.set_src_key(req->src_key);

		process_block(req, context);
	}

	void request_complete(std::shared_ptr<request> req, const ioremap::elliptics::error_info &error)
	{
		if (error) {
			fprintf(stderr, "%s %d: queue request completion error: %s\n", dnet_dump_id(&req->id), req->src_key, error.message().c_str());
		} else {
			//fprintf(stderr, "%s %d: queue request completed\n", dnet_dump_id(&req->id), req->src_key);
		}

		--running_requests;
		condition.notify_one();
	}

	void process_block(std::shared_ptr<request> req, const ioremap::elliptics::exec_context &context)
	{
		fprintf(stderr, "%s %d, received data, byte size %ld\n",
				dnet_dump_id_str(context.src_id()->id), context.src_key(),
				context.data().size()
				);

		auto array = ioremap::grape::deserialize<ioremap::grape::data_array>(context.data());
		ioremap::elliptics::data_pointer d = array.data();
		size_t count = array.sizes().size();

		// process entries
		fprintf(stderr, "%s %d, processing %ld entries\n",
				dnet_dump_id_str(context.src_id()->id), context.src_key(),
				count
				);

		size_t offset = 0;
		for (size_t i = 0; i < count; ++i) {
			const ioremap::grape::entry_id &entry_id = array.ids()[i];
			int bytesize = array.sizes()[i];

			//TODO: check result of the proc()
			proc(entry_id, d.slice(offset, bytesize));

			offset += bytesize;
		}

		// acknowledge entries
		fprintf(stderr, "%s %d, acking %ld entries\n",
				dnet_dump_id_str(context.src_id()->id), context.src_key(),
				count
				);

		queue_ack(client, req, context, array.ids());
	}

};

using namespace boost::program_options;

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

/*	options_description lowlevel("Queue data access and lowlevel actions");
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
*/
	options_description opts;
	opts.add(generic).add(elliptics);//.add(lowlevel).add(highlevel);

	parsed_options parsed_opts = parse_command_line(argc, argv, opts);
	variables_map args;
	store(parsed_opts, args);
	notify(args);

	if (args.count("help")) {
		std::cout << "Queue support utility." << "\n";
		std::cout << opts << "\n";
		return 1;
	}

	// if (!args.count("id")) {
	// 	std::cerr << "--id option required" << "\n";
	// 	return 1;
	// }
	// int id(args["id"].as<int>());

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

	auto clientlib = elliptics_client_state::create(remotes, groups, logfile, loglevel);

	const std::string queue_name("queue");
	const int request_size = 100;
	
	// read queue indefinitely
	queue_pump pump(clientlib.create_session(), queue_name, request_size);
	pump.run([] (ioremap::grape::entry_id entry_id, ioremap::elliptics::data_pointer data) -> bool {
		fprintf(stderr, "entry %d-%d, byte size %ld\n", entry_id.chunk, entry_id.pos, data.size());
	});

	return 0;
}
