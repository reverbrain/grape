#ifndef __CONCURRENT_HPP
#define __CONCURRENT_HPP

#include <atomic>
#include <mutex>
#include <condition_variable>

#include <elliptics/session.hpp>
#include <grape/data_array.hpp>
#include <grape/entry_id.hpp>

namespace ioremap { namespace grape {

class concurrent_pump
{
private:
	std::atomic_int running_requests;
	std::mutex mutex;
	std::condition_variable condition;
	std::atomic_bool cont;

public:
	int concurrency_limit;
	bool wait_on_exit;

	concurrent_pump()
		: running_requests(0)
		, cont(true)
		, concurrency_limit(1)
		, wait_on_exit(true)
	{}

	void run(std::function<void ()> make_request) {
		cont = true;
		while (true) {
			std::unique_lock<std::mutex> lock(mutex);
			condition.wait(lock, [this]{return running_requests < concurrency_limit;});
			if (!cont) {
				break;
			}
			++running_requests;
			lock.unlock();

			make_request();
		}

		if (wait_on_exit) {
			std::unique_lock<std::mutex> lock(mutex);
			condition.wait(lock, [this]{return running_requests == 0;});
		}
	}

	// Should be called when response on request is received
	void complete_request() {
		std::unique_lock<std::mutex> lock(mutex);
		--running_requests;
		condition.notify_one();
	}

	// Should be called if runloop must be stopped
	void stop() {
		cont = false;
	}
};

struct request {
	dnet_id id;
	int src_key;

	request() : src_key(-1) {
		memset(&id, 0, sizeof(dnet_id));
	}
	request(int src_key) : src_key(src_key) {
		memset(&id, 0, sizeof(dnet_id));
	}
};

template<class queue_reader_impl>
class base_queue_reader
{
protected:
	concurrent_pump runloop;

	ioremap::elliptics::session client;
	const std::string queue_name;
	const int request_size;
	std::atomic_int next_request_id;

	int concurrency_limit;

public:
	base_queue_reader(ioremap::elliptics::session client, const std::string &queue_name, int request_size, int concurrency_limit)
		: client(client)
		, queue_name(queue_name)
		, request_size(request_size)
		, next_request_id(0)
		, concurrency_limit(concurrency_limit)
	{
		srand(time(NULL));
	}

	void run() {
		runloop.concurrency_limit = concurrency_limit;
		runloop.run([this] () {
				base_queue_reader::queue_peek(client, next_request_id++, request_size);
		});
	}

	static void queue_ack(ioremap::elliptics::session client,
			const std::string &queue_name,
			std::shared_ptr<request> req,
			ioremap::elliptics::exec_context context,
			const std::vector<ioremap::grape::entry_id> &ids)
	{
		client.set_exceptions_policy(ioremap::elliptics::session::no_exceptions);

		size_t count = ids.size();
		client.exec(context, queue_name + "@ack-multi", ioremap::grape::serialize(ids))
				.connect(ioremap::elliptics::async_result<ioremap::elliptics::exec_result_entry>::result_function(),
					[req, count] (const ioremap::elliptics::error_info &error) {
						if (error) {
							fprintf(stderr, "%s %d, %ld entries not acked: %s\n", dnet_dump_id(&req->id), req->src_key, count, error.message().c_str());
						} else {
							fprintf(stderr, "%s %d, %ld entries acked\n", dnet_dump_id(&req->id), req->src_key, count);
						}
					}
				);
	}

	void queue_peek(ioremap::elliptics::session client, int req_unique_id, int arg)
	{
		client.set_exceptions_policy(ioremap::elliptics::session::no_exceptions);

		std::string queue_key = std::to_string(req_unique_id) + std::to_string(rand());

		auto req = std::make_shared<request>(req_unique_id);
		client.transform(queue_key, req->id);

		client.exec(&req->id, req->src_key, queue_name + "@peek-multi", std::to_string(arg))
				.connect(
					std::bind(&base_queue_reader::data_received, this, req, std::placeholders::_1),
					std::bind(&base_queue_reader::request_complete, this, req, std::placeholders::_1)
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

		fprintf(stderr, "%s %d, received data, byte size %ld\n",
				dnet_dump_id_str(context.src_id()->id), context.src_key(),
				context.data().size()
			   );

		auto array = ioremap::grape::deserialize<ioremap::grape::data_array>(context.data());

		ioremap::elliptics::data_pointer d = array.data();
		size_t count = array.sizes().size();
		fprintf(stderr, "%s %d, processing %ld entries\n",
				dnet_dump_id_str(context.src_id()->id), context.src_key(),
				count
				);
		fprintf(stderr, "array %p\n", d.data());

		static_cast<queue_reader_impl*>(this)->process_data_array(req, context, array);
	}

	void request_complete(std::shared_ptr<request> req, const ioremap::elliptics::error_info &error) {
		//TODO: add reaction to hard errors like No such device or address: -6
		if (error) {
			fprintf(stderr, "%s %d: queue request completion error: %s\n", dnet_dump_id(&req->id), req->src_key, error.message().c_str());
		} else {
			//fprintf(stderr, "%s %d: queue request completed\n", dnet_dump_id(&req->id), req->src_key);
		}

		runloop.complete_request();
	}
};

class queue_reader: public base_queue_reader<queue_reader>
{
public:
	typedef std::function<bool (ioremap::grape::entry_id, ioremap::elliptics::data_pointer data)> processing_function;
	processing_function proc;

	queue_reader(ioremap::elliptics::session client, const std::string &queue_name, int request_size, int concurrency_limit)
		: base_queue_reader(client, queue_name, request_size, concurrency_limit) 
	{}

	void run(processing_function func) {
		proc = func;
		base_queue_reader::run();
	}

	void process_data_array(std::shared_ptr<request> req, ioremap::elliptics::exec_context context, ioremap::grape::data_array &array) {
		ioremap::elliptics::data_pointer d = array.data();
		size_t count = array.sizes().size();

		std::vector<ioremap::grape::entry_id> ack_ids;

		size_t offset = 0;
		for (size_t i = 0; i < count; ++i) {
			const ioremap::grape::entry_id &entry_id = array.ids()[i];
			int bytesize = array.sizes()[i];

			//TODO: check result of the proc()
			if (proc(entry_id, d.slice(offset, bytesize))) {
				ack_ids.push_back(entry_id);
			}

			offset += bytesize;
		}

		// acknowledge entries
		fprintf(stderr, "%s %d, acking %ld entries\n",
				dnet_dump_id_str(context.src_id()->id), context.src_key(),
				ack_ids.size()
				);

		queue_ack(client, queue_name, req, context, ack_ids);
	}
};

class bulk_queue_reader: public base_queue_reader<bulk_queue_reader>
{
public:
	typedef std::function<int (ioremap::elliptics::exec_context, ioremap::grape::data_array)> processing_function;
	processing_function proc;

	static const int REQUEST_CONTINUE = 0;
	static const int REQUEST_ACK      = 1 << 0;
	static const int REQUEST_STOP     = 1 << 1;

	bulk_queue_reader(ioremap::elliptics::session client, const std::string &queue_name, int request_size, int concurrency_limit)
		: base_queue_reader(client, queue_name, request_size, concurrency_limit)
	{}

	void run(processing_function func) {
		proc = func;
		base_queue_reader::run();
	}

	void handle_process_result(int result, std::shared_ptr<request> req, ioremap::elliptics::exec_context context, ioremap::grape::data_array array) {
		if (result & REQUEST_ACK) {
			queue_ack(client, queue_name, req, context, array.ids());
		}
		if (result & REQUEST_STOP) {
			runloop.stop();
		}
	}

	void process_data_array(std::shared_ptr<request> req, ioremap::elliptics::exec_context context, ioremap::grape::data_array &array) {
		int proc_result = proc(context, array);
		handle_process_result(proc_result, req, context, array);
	}
};

class queue_writer
{
public:
	typedef ioremap::elliptics::data_pointer generator_result_type;
	typedef std::function<generator_result_type ()> generation_function;

private:
	concurrent_pump runloop;

	ioremap::elliptics::session client;
	const std::string queue_name;
	std::atomic_int next_request_id;

	int concurrency_limit;

	generation_function gen;

public:
	queue_writer(ioremap::elliptics::session client, const std::string &queue_name, int concurrency_limit = 1)
		: client(client)
		, queue_name(queue_name)
		, next_request_id(0)
		, concurrency_limit(concurrency_limit)
	{
		srand(time(NULL));
	}

	void run(generation_function func) {
		gen = func;
		runloop.concurrency_limit = concurrency_limit;
		runloop.run([this] () {
			generator_result_type d = gen();
			if (d.empty()) {
				runloop.stop();
			}
			queue_push(client, next_request_id++, d);
		});
	}

	void queue_push(ioremap::elliptics::session client, int req_unique_id, ioremap::elliptics::data_pointer d)
	{
		client.set_exceptions_policy(ioremap::elliptics::session::no_exceptions);

		std::string queue_key = std::to_string(req_unique_id) + std::to_string(rand());

		auto req = std::make_shared<request>(req_unique_id);
		client.transform(queue_key, req->id);

		client.exec(&req->id, req->src_key, "queue@push", d)
			.connect(
				ioremap::elliptics::async_result<ioremap::elliptics::exec_result_entry>::result_function(),
				std::bind(&queue_writer::request_complete, this, req, std::placeholders::_1)
			);
	}

	void request_complete(std::shared_ptr<request> req, const ioremap::elliptics::error_info &error)
	{
		if (error) {
			fprintf(stderr, "%s %d: queue request completion error: %s\n", dnet_dump_id(&req->id), req->src_key, error.message().c_str());
		} else {
			//fprintf(stderr, "%s %d: queue request completed\n", dnet_dump_id(&req->id), req->src_key);
		}

		runloop.complete_request();
	}
};

}} // namespace ioremap::grape

#endif //__CONCURRENT_HPP

