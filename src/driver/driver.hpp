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

#include "grape/elliptics_client_state.hpp"

#include <queue>
#include <mutex>
#include <atomic>

#include <cocaine/common.hpp>
#include <cocaine/api/driver.hpp>
#include <cocaine/api/stream.hpp>
#include <cocaine/asio/reactor.hpp>
#include <cocaine/app.hpp>
#include <cocaine/api/storage.hpp>

namespace cocaine { namespace driver {

class queue_driver: public api::driver_t {
	public:
		typedef api::driver_t category_type;

	public:
		struct downstream_t: public cocaine::api::stream_t {
			downstream_t(queue_driver *queue, const ioremap::elliptics::data_pointer &d);
			~downstream_t();

			virtual void write(const char *data, size_t size);
			virtual void error(int, const std::string &message);
			virtual void close();

			queue_driver *m_queue;
			const ioremap::elliptics::data_pointer &m_data;

			int m_attempts;
		};

		struct queue_request {
			std::atomic_int total, success;
			dnet_id id;
		};

		queue_driver(context_t& context, io::reactor_t& reactor, app_t& app, const std::string& name, const Json::Value& args);

		virtual ~queue_driver();

		virtual Json::Value info() const;

		void queue_dec(int num);
		void queue_inc(int num);

		void get_more_data();

	private:
		std::vector<int> m_queue_groups;

		cocaine::context_t& m_context;
		cocaine::app_t& m_app;
		std::shared_ptr<cocaine::logging::log_t> m_log;

		elliptics_client_state m_client;
		std::atomic_int m_src_key;
		std::map<int, std::string> m_events;

		void on_idle_timer_event(ev::timer&, int);

		// requests to the queue callbacks
		void on_queue_request_data(std::shared_ptr<queue_request> req, const ioremap::elliptics::exec_result_entry &result);
		void on_queue_request_complete(std::shared_ptr<queue_request> req, const ioremap::elliptics::error_info &error);

		bool process_data(const ioremap::elliptics::data_pointer &data);

	private:
		struct data_pointer_comparator_t {
			bool operator() (const ioremap::elliptics::data_pointer &a, const ioremap::elliptics::data_pointer &b) const {
				return a.data() < b.data();
			}
		};

		ev::timer m_idle_timer;

		std::queue<ioremap::elliptics::data_pointer> m_local_queue;
		std::mutex m_local_queue_mutex;
		std::mutex m_local_queue_processing_mutex;

		const std::string m_worker_event;
		const std::string m_queue_name;
		const std::string m_queue_pop_event;
		const std::string m_queue_ack_event;

		const double m_timeout;
		const double m_deadline;

		std::atomic_int m_queue_length, m_queue_length_max;

		std::atomic_int m_queue_src_key;

		friend class downstream_t;
};

}}

#endif /* __GRAPE_QUEUE_DRIVER_HPP */
