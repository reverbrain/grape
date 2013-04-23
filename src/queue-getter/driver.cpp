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


#include <cocaine/context.hpp>
#include <cocaine/logging.hpp>
#include <cocaine/api/event.hpp>
#include "driver.hpp"

using namespace cocaine;
using namespace cocaine::driver;
using namespace cocaine::logging;
using namespace ioremap::elliptics;

// number of retries for data processing
const uint FAIL_LIMIT = 3;

queue_t::queue_t(context_t& context,
				 io::reactor_t &reactor,
				 app_t &app,
				 const std::string& name,
				 const Json::Value& args):
	category_type(context, reactor, app, name, args),
	m_context(context),
	m_app(app),
	m_log(new log_t(context, cocaine::format("driver/%s", name))),
	m_timer(reactor.native()),
	m_timer_async(reactor.native()),
	m_queue_async(reactor.native()),
	m_current_exec_count(0),
	m_worker_event(args.get("emit", name).asString()),
	m_queue_name(args.get("source-queue-app", "queue").asString()),
	//FIXME: queue id is important, make it a mandatory conf.value
	m_queue_id(args.get("source-queue-id", "5").asString()),
	m_queue_get_event(m_queue_name + "@pop"),
	m_ack_on_success(true),
	m_queue_ack_event(m_queue_name + "@ack"),
	m_return_on_total_fail(true),
	m_queue_return_event(m_queue_name + "@return"),
	m_timeout(args.get("timeout", 0.0f).asDouble()),
	m_deadline(args.get("deadline", 0.0f).asDouble())
{
	try {
		m_logger.reset(new file_logger(args.get("logfile", "/dev/stderr").asCString()));
		m_node.reset(new node(*m_logger));
	} catch (std::exception &e) {
		throw configuration_error_t(e.what());
	}

	Json::Value remotes = args.get("remotes", Json::arrayValue);
	if (remotes.size() == 0) {
		throw configuration_error_t("no remotes have been specified");
	}
	int remotes_added = 0;
	for (Json::ArrayIndex index = 0; index < remotes.size(); ++index) {
		try {
			m_node->add_remote(remotes[index].asCString());
			++remotes_added;
		} catch (...) {
			// We don't care, really
		}
	}
	if (remotes_added == 0) {
		throw configuration_error_t("no remotes were added successfully");
	}

	Json::Value groups = args.get("groups", Json::arrayValue);
	if (groups.size() == 0) {
		throw configuration_error_t("no groups have been specified");
	}
	std::transform(groups.begin(), groups.end(), std::back_inserter(m_groups),
		std::bind(&Json::Value::asInt, std::placeholders::_1));

	if (m_queue_name.empty()) {
		throw configuration_error_t("no queue name has been specified");
	}

	m_timer.set<queue_t, &queue_t::on_event>(this);
	m_timer.set(1.0f, 5.0f);
	m_timer.again();

	m_timer_async.set<queue_t, &queue_t::on_timer_async>(this);
	m_timer_async.start();

	m_queue_async.set<queue_t, &queue_t::on_queue_async>(this);
	m_queue_async.start();
}

queue_t::~queue_t() {
	m_timer.stop();
	m_node.reset();
}

Json::Value
queue_t::info() const {
	Json::Value result;

	result["type"] = "persistent-queue";
	result["name"] = m_queue_name;

	return result;
}

session queue_t::create_session()
{
	session sess(*m_node);
	sess.set_groups(m_groups);
	return sess;
}

void queue_t::on_result(const ioremap::elliptics::exec_result_entry &result)
{
	try {
		if (result.error()) {
			COCAINE_LOG_INFO(m_log, "got error from %s-%s: %d, %s", m_queue_name.c_str(), m_queue_id.c_str(), result.error().code(), result.error().message());
			return;
		}

		exec_context context = result.context();
		// check if queue is empty
		if (context.data().empty()) {
			return;
		}

		COCAINE_LOG_INFO(m_log, "got data %p from %s-%s", context.data().data(), m_queue_name.c_str(), m_queue_id.c_str());

		{
			std::lock_guard<std::mutex> lock(m_local_queue_mutex);
			m_local_queue.push(context.data());
		}
		m_queue_async.send();

	} catch(const std::exception& e) {
		COCAINE_LOG_ERROR(m_log, "unable to enqueue an event - %s", e.what());
	}
}

void queue_t::on_request_finished(const error_info &error)
{
	if (!error) {
		COCAINE_LOG_INFO(m_log, "request to %s-%s finished", m_queue_name.c_str(), m_queue_id.c_str());
	} else {
		COCAINE_LOG_ERROR(m_log, "request to %s-%s failed: %s", m_queue_name.c_str(), m_queue_id.c_str(), error.message().c_str());
	}
	--m_current_exec_count;
}

void queue_t::on_event(ev::timer &, int) {
	get_more_data();
}

void queue_t::on_timer_async(ev::async &, int)
{
	m_timer.again();
}

void queue_t::on_queue_async(ev::async &, int)
{
	process_queue();
}

bool queue_t::process_data(const data_pointer &data)
{
	if (data.empty()) {
		// queue.pop returns no data when queue has no data (queue is empty),
		// sending async to initiate "retry" timer
		COCAINE_LOG_INFO(m_log, "DEBUG: process_data() and data is empty");
		m_timer_async.send();
		return false;
	}

	COCAINE_LOG_INFO(m_log, "forwarding data %p to worker", data.data());

	auto result = m_scopes.insert(std::make_pair(data, scope_t(data)));
	scope_t &scope = result.first->second;
	if (result.second) {
		++scope.try_count;
		if (scope.try_count > FAIL_LIMIT) {
			m_scopes.erase(data);
			on_process_total_fail(data);
			return true;
		}
	}

	try {
		auto downstream = std::make_shared<downstream_t>(this, &scope);

		api::policy_t policy(false, m_timeout, m_deadline);
		auto upstream = m_app.enqueue(api::event_t(m_worker_event, policy), downstream);

		if (!downstream->m_finished) {
			upstream->write(data.data<char>(), data.size());
			scope.upstream = upstream;
			scope.downstream = downstream;

			get_more_data();

			return true;
		}
		COCAINE_LOG_ERROR(m_log, "downstream already finished, postpone data for later");

	} catch (const cocaine::error_t &e) {
		COCAINE_LOG_ERROR(m_log, "unable to enqueue an event - %s", e.what());
	}

	{
		std::lock_guard<std::mutex> lock(m_local_queue_mutex);
		m_local_queue.push(data);
	}
	return false;

	// scope_t &scope = m_scopes[data];
	// ++scope.count;
	// if (scope.count > FAIL_LIMIT) {
	//     m_scopes.erase(data);
	//     on_process_total_fail(data);
	//     return true;
	// }
/*
	try {
		api::policy_t policy(false, m_timeout, m_deadline);
		scope.downstream = std::make_shared<downstream_t>(this, &scope, data);
		scope.upstream = m_app.enqueue(api::event_t(m_worker_event, policy), scope.downstream);
		scope.upstream->write(data.data<char>(), data.size());

		get_more_data();

		return true;

	} catch (const cocaine::error_t &e) {
		COCAINE_LOG_ERROR(m_log, "unable to enqueue an event - %s", e.what());
		{
			std::lock_guard<std::mutex> lock(m_local_queue_mutex);
			m_local_queue.push(data);
		}
		return false;
	}
*/
}

void queue_t::get_more_data()
{
	session sess = create_session();

	++m_current_exec_count;

	dnet_id queue_id;
	queue_id.type = 0;
	queue_id.group_id = 0;
	sess.transform(m_queue_id, queue_id);

	sess.set_exceptions_policy(session::no_exceptions);
	sess.exec(&queue_id, m_queue_get_event, data_pointer()).connect(
		std::bind(&queue_t::on_result, this, std::placeholders::_1),
		std::bind(&queue_t::on_request_finished, this, std::placeholders::_1)
	);

	COCAINE_LOG_INFO(m_log, "sending %s to %s-%s", m_queue_get_event.c_str(), m_queue_name.c_str(), m_queue_id.c_str());
}

void queue_t::on_process_failed(const data_pointer &data)
{
	COCAINE_LOG_INFO(m_log, "failed to process data %p", data.data());
	// try to repeat processing
	process_data(data);
}

void queue_t::on_process_successed(const ioremap::elliptics::data_pointer &data)
{
	COCAINE_LOG_INFO(m_log, "data %p processed", data.data());
	//NOTE: it's unlikely that actual data would be in a text form
	COCAINE_LOG_DEBUG(m_log, "data %p content: %s", data.data(), data.to_string());
	const void *marker = data.data();

	m_scopes.erase(data);

	// acking success
	if (m_ack_on_success) {
		session sess = create_session();
		sess.set_exceptions_policy(session::no_exceptions);

		dnet_id queue_id;
		queue_id.type = 0;
		queue_id.group_id = 0;
		sess.transform(m_queue_id, queue_id);

		sess.exec(&queue_id, m_queue_ack_event, data_pointer()).connect(
			async_result<exec_result_entry>::result_function(),
			[m_log, marker] (const error_info &error) {
				if (error) {
					COCAINE_LOG_ERROR(m_log, "data %p not acked", marker);
				} else {
					COCAINE_LOG_INFO(m_log, "data %p acked", marker);
				}
			}
		);
	}

	if (m_local_queue.empty()) {
		get_more_data();
	} else {
		process_queue();
	}
}

void queue_t::on_process_total_fail(const data_pointer &data)
{
	COCAINE_LOG_ERROR(m_log, "unable to process data %p, fail count %d", data.data(), FAIL_LIMIT);
	//NOTE: it's unlikely that actual data would be in a text form
	COCAINE_LOG_DEBUG(m_log, "data content: %s", data.to_string());

/*    if (m_return_on_total_fail) {
		session sess = create_session();
		sess.set_exceptions_policy(session::no_exceptions);

		dnet_id queue_id;
		queue_id.type = 0;
		queue_id.group_id = 0;
		sess.transform(m_queue_id, queue_id);

		sess.exec(&queue_id, m_queue_ack_event, data_pointer()).connect(
			async_result<exec_result_entry>::result_function(),
			[m_log, marker] (const error_info &error) {
				if (error) {
					COCAINE_LOG_ERROR(m_log, "data %p not acked", marker);
				} else {
					COCAINE_LOG_INFO(m_log, "data %p acked", marker);
				}
			}
		);
	}
*/
}

void queue_t::process_queue()
{
	data_pointer data;
	for (;;) {
		{
			std::lock_guard<std::mutex> lock(m_local_queue_mutex);
			if (m_local_queue.empty())
				return;
			data = m_local_queue.front();
			m_local_queue.pop();
		}
		if (!process_data(data))
			return;
	}
}

queue_t::downstream_t::downstream_t(queue_t *queue, queue_t::scope_t *scope):
	m_queue(queue), m_scope(scope), m_finished(false)
{
}

void queue_t::downstream_t::write(const char *data, size_t size)
{
	std::string token(data, size);
	if (token == "done") {
		m_finished = true;
		m_queue->on_process_successed(m_scope->data);
	}
}

void queue_t::downstream_t::error(error_code code, const std::string &msg)
{
	COCAINE_LOG_ERROR(m_queue->m_log, "getting error from worker: %d, %s", code, msg.c_str());
	m_finished = true;
	m_queue->on_process_failed(m_scope->data);
}

void queue_t::downstream_t::close()
{
	m_queue->get_more_data();
	if (!m_finished) {
		m_queue->on_process_failed(m_scope->data);
	}
}
