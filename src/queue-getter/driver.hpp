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

#ifndef DRIVER_HPP__
#define DRIVER_HPP__

#include <queue>
#include <mutex>
#include <atomic>
#include <jsoncpp/json.hpp>
#include <cocaine/common.hpp>
#include <cocaine/api/driver.hpp>
#include <cocaine/api/stream.hpp>
#include <cocaine/asio/reactor.hpp>
#include <cocaine/app.hpp>
#include <elliptics/cppdef.h>

namespace cocaine { namespace driver {

class queue_t:
	public api::driver_t
{
	public:
		typedef api::driver_t category_type;

	public:
		struct scope_t
		{
			scope_t(ioremap::elliptics::data_pointer data)
				: data(data), try_count(0)
			{}

			ioremap::elliptics::data_pointer data;
			uint try_count;
			std::shared_ptr<api::stream_t> upstream;
			std::shared_ptr<api::stream_t> downstream;
		};

		struct downstream_t:
			public api::stream_t
		{
			downstream_t(queue_t *queue, scope_t *scope);

			virtual
			void
			write(const char *data, size_t size);

			virtual
			void
			error(cocaine::error_code, const std::string &message);

			virtual
			void
			close();

			queue_t *m_queue;
			scope_t *m_scope;
			bool m_finished;
		};

		queue_t(context_t& context,
				io::reactor_t& reactor,
				app_t& app,
			 const std::string& name,
			 const Json::Value& args);

		virtual
		~queue_t();

		virtual
		Json::Value
		info() const;

	private:
		ioremap::elliptics::session
		create_session();

		// idle timer callbacks 
		void
		on_idle_timer_event(ev::timer&, int);
		void
		on_idle_timer_async(ev::async&, int);

		// requests to the queue callbacks
		void
		on_queue_request_data(const ioremap::elliptics::exec_result_entry &result);
		void
		on_queue_request_complete(const ioremap::elliptics::error_info &error);

		void
		on_local_queue_async(ev::async&, int);
		bool
		process_data(const ioremap::elliptics::data_pointer &data);
		void
		get_more_data();
		void
		on_process_failed(const ioremap::elliptics::data_pointer &data);
		void
		on_process_successed(const ioremap::elliptics::data_pointer &data);
		void
		on_process_total_fail(const ioremap::elliptics::data_pointer &data);

		void
		process_local_queue();

	private:
		struct data_pointer_comparator_t
		{
			bool operator() (const ioremap::elliptics::data_pointer &a,
							 const ioremap::elliptics::data_pointer &b) const
			{
				return a.data() < b.data();
			}
		};

		context_t& m_context;
		app_t& m_app;
		std::shared_ptr<logging::log_t> m_log;
		std::unique_ptr<ioremap::elliptics::logger> m_logger;
		std::unique_ptr<ioremap::elliptics::node> m_node;
		std::vector<int> m_groups;

		ev::timer m_idle_timer;
		ev::async m_idle_timer_async;
		ev::async m_local_queue_async;

		std::atomic_uint m_current_exec_count;
		std::map<ioremap::elliptics::data_pointer, scope_t, data_pointer_comparator_t> m_scopes;

		std::queue<ioremap::elliptics::data_pointer> m_local_queue;
		std::mutex m_local_queue_mutex;
		std::mutex m_local_queue_processing_mutex;

		const std::string m_worker_event;
		const std::string m_queue_name;
		const std::string m_queue_id;
		const std::string m_queue_get_event;
		bool m_ack_on_success;
		const std::string m_queue_ack_event;
		bool m_return_on_total_fail;
		const std::string m_queue_return_event;

		const double m_timeout;
		const double m_deadline;

		friend class downstream_t;
};

}}

#endif
