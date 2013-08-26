/*
	Copyright (c) 2013+ Ruslan Nigmatullin <euroelessar@yandex.ru>

	This file is part of Grape.

	Cocaine is free software; you can redistribute it and/or modify
	it under the terms of the GNU Lesser General Public License as published by
	the Free Software Foundation; either version 3 of the License, or
	(at your option) any later version.

	Cocaine is distributed in the hope that it will be useful,
	but WITHOUT ANY WARRANTY; without even the implied warranty of
	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
	GNU Lesser General Public License for more details.

	You should have received a copy of the GNU Lesser General Public License
	along with this program. If not, see <http://www.gnu.org/licenses/>.
*/

#ifndef __GRAPE_QUEUE_DRIVER_HPP
#define __GRAPE_QUEUE_DRIVER_HPP

#include <queue>
#include <mutex>
#include <atomic>

#include <cocaine/common.hpp>
#include <cocaine/api/driver.hpp>
#include <cocaine/api/stream.hpp>
#include <cocaine/asio/reactor.hpp>
#include <cocaine/app.hpp>
#include <cocaine/api/storage.hpp>

#include "grape/elliptics_client_state.hpp"

namespace cocaine { namespace driver {

class queue_driver: public api::driver_t {
	public:
		typedef api::driver_t category_type;

	public:
		struct downstream_t: public cocaine::api::stream_t {
			downstream_t(queue_driver *parent);
			~downstream_t();

			virtual void write(const char *data, size_t size);
			virtual void error(int code, const std::string &message);
			virtual void close();

			queue_driver *parent;
			uint64_t start_time;
		};

		struct queue_request {
			int num;
			dnet_id id;
			int src_key;

			queue_request(void) : num(0), src_key(0) {
				memset(&id, 0, sizeof(dnet_id));
			}
		};

		queue_driver(context_t& context, io::reactor_t& reactor, app_t& app, const std::string& name, const Json::Value& args);

		virtual ~queue_driver();

		virtual Json::Value info() const;

		void queue_dec(int num);
		void queue_inc(int num);

		void get_more_data();

	private:
		cocaine::context_t& m_context;
		cocaine::app_t& m_app;
		std::shared_ptr<cocaine::logging::log_t> m_log;

		elliptics_client_state m_client;
		std::vector<int> m_queue_groups;
		int m_wait_timeout;
		int m_check_timeout;
		int m_request_size;
		double m_high_rate_limit;

		void on_request_timer_event(ev::timer&, int);
		void on_rate_control_timer_event(ev::timer&, int);

		// request queue and callbacks
		void send_request();
		void on_queue_request_data(std::shared_ptr<queue_request> req, const ioremap::elliptics::exec_result_entry &result);
		void on_queue_request_complete(std::shared_ptr<queue_request> req, const ioremap::elliptics::error_info &error);

		bool enqueue_data(const ioremap::elliptics::exec_context &context);
		void on_worker_complete(uint64_t start_time, bool success);

	private:
		// std::queue<ioremap::elliptics::data_pointer> m_local_queue;
		// std::mutex m_local_queue_mutex;
		// std::mutex m_local_queue_processing_mutex;

		const std::string m_queue_name;
		std::string m_worker_event;
		const std::string m_queue_pop_event;

		const double m_timeout;
		const double m_deadline;

		std::atomic_int m_queue_length;
		std::atomic_int m_queue_length_max;

		ev::timer m_request_timer;
		ev::timer m_rate_control_timer;

		std::atomic<uint64_t> last_process_done_time; // in microseconds
		std::atomic<uint64_t> processed_time; // in microseconds
		std::atomic<int> process_count;
		std::atomic<int> receive_count;
		std::atomic<int> request_count;
		double rate_focus;
		int growth_step;
		double growth_time;

		uint64_t last_request_time; // in microseconds

		std::atomic_int m_queue_src_key;

		friend class downstream_t;
};

}}

#endif /* __GRAPE_QUEUE_DRIVER_HPP */
