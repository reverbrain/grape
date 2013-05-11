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

#include "driver.hpp"

#include <cocaine/context.hpp>
#include <cocaine/logging.hpp>
#include <cocaine/api/event.hpp>

#include "rapidjson/document.h"

// number of retries for data processing
const int FAIL_LIMIT = 3;

using namespace cocaine::driver;

queue_driver::queue_driver(cocaine::context_t& context, cocaine::io::reactor_t &reactor, cocaine::app_t &app,
		const std::string& name, const Json::Value& args):
category_type(context, reactor, app, name, args),
m_context(context),
m_app(app),
m_log(new cocaine::logging::log_t(context, cocaine::format("driver/%s", name))),
m_src_key(0),
m_idle_timer(reactor.native()),
m_worker_event(args.get("emit", name).asString()),
m_queue_name(args.get("source-queue-app", "queue").asString()),
//FIXME: queue id is important, make it a mandatory conf.value
m_queue_id(args.get("source-queue-id", "5").asString()),
m_queue_pop_event(m_queue_name + "@pop"),
m_queue_ack_event(m_queue_name + "@ack"),
m_timeout(args.get("timeout", 0.0f).asDouble()),
m_deadline(args.get("deadline", 0.0f).asDouble())
{
	COCAINE_LOG_INFO(m_log, "driver starts\n");

	try {
		std::string s = Json::FastWriter().write(args);

		rapidjson::Document doc;
		doc.Parse<0>(s.c_str());

		m_client = elliptics_client_state::create(doc);
	} catch (const std::exception &e) {
		COCAINE_LOG_INFO(m_log, "driver exception: %s\n", e.what());
		throw;
	}

	if (m_queue_name.empty())
		throw configuration_error_t("no queue name has been specified");

	m_idle_timer.set<queue_driver, &queue_driver::on_idle_timer_event>(this);
	m_idle_timer.set(1.0f, 5.0f);
	m_idle_timer.again();
}

queue_driver::~queue_driver()
{
	m_idle_timer.stop();
}

Json::Value queue_driver::info() const
{
	Json::Value result;

	result["type"] = "persistent-queue";
	result["name"] = m_queue_name;

	return result;
}

void queue_driver::on_idle_timer_event(ev::timer &, int)
{
	COCAINE_LOG_INFO(m_log, "timer: checking queue");
	get_more_data();
}

void queue_driver::get_more_data()
{
	ioremap::elliptics::session sess = m_client.create_session();

	dnet_id queue_id;
	queue_id.type = 0;
	queue_id.group_id = 0;
	sess.transform(m_queue_id, queue_id);

	sess.set_exceptions_policy(ioremap::elliptics::session::no_exceptions);
	sess.exec(&queue_id, m_queue_pop_event, ioremap::elliptics::data_pointer()).connect(
		std::bind(&queue_driver::on_queue_request_data, this, std::placeholders::_1),
		std::bind(&queue_driver::on_queue_request_complete, this, std::placeholders::_1)
	);

	COCAINE_LOG_INFO(m_log, "%s: request has been sent to queue %s-%s",
			m_queue_pop_event.c_str(), m_queue_name.c_str(), m_queue_id.c_str());
}

void queue_driver::on_queue_request_data(const ioremap::elliptics::exec_result_entry &result)
{
	try {
		if (result.error()) {
			COCAINE_LOG_INFO(m_log, "%s-%s: error: %d: %s",
					m_queue_name.c_str(), m_queue_id.c_str(),
					result.error().code(), result.error().message());
			return;
		}

		// queue.pop returns no data when queue is empty.
		// Idle timer handles queue emptiness firing periodically to check
		// if there is new data in the queue.
		//
		// But every time when we actually got data we have to postpone idle timer.

		ioremap::elliptics::exec_context context = result.context();
		if (!context.data().empty()) {
			COCAINE_LOG_INFO(m_log, "%s-%s: data: size: %d", m_queue_name.c_str(), m_queue_id.c_str(), context.data().size());

			bool processed_ok = process_data(context.data());

			COCAINE_LOG_INFO(m_log, "%s-%s: data: size: %d, processed-ok: %d",
					m_queue_name.c_str(), m_queue_id.c_str(), context.data().size(),
					processed_ok);
		}
	} catch(const std::exception &e) {
		COCAINE_LOG_ERROR(m_log, "%s-%s: exception: %s", m_queue_name.c_str(), m_queue_id.c_str(), e.what());
	}
}

void queue_driver::on_queue_request_complete(const ioremap::elliptics::error_info &error)
{
	if (!error) {
		COCAINE_LOG_INFO(m_log, "%s-%s: completed", m_queue_name.c_str(), m_queue_id.c_str());
	} else {
		COCAINE_LOG_ERROR(m_log, "%s-%s: completed error: %s", m_queue_name.c_str(), m_queue_id.c_str(), error.message().c_str());
	}
}

bool queue_driver::process_data(const ioremap::elliptics::data_pointer &data)
{
	// Pass data to the worker, return it back to the local queue if failed.

	try {
		auto downstream = std::make_shared<downstream_t>(this, data);

		std::string raw_data;
		raw_data.resize(sizeof(struct sph) + m_worker_event.size() + data.size());

		struct sph *sph = (struct sph *)raw_data.data();
		sph->flags = DNET_SPH_FLAGS_SRC_BLOCK;
		sph->src_key = m_src_key++;
		sph->event_size = m_worker_event.size();
		sph->data_size = data.size();

		memcpy(sph + 1, m_worker_event.data(), sph->event_size);
		memcpy(((char *)(sph + 1)) + sph->event_size, data.data(), sph->data_size);

		// this map should be used to store iteration counter
		//m_events.insert(std::make_pair(static_cast<const int>(sph->src_key), data.to_string()));

		api::policy_t policy(false, m_timeout, m_deadline);
		auto upstream = m_app.enqueue(api::event_t(m_worker_event, policy), downstream);
		upstream->write(raw_data.data(), raw_data.size());

		// get more data
		get_more_data();

		return true;
	} catch (const cocaine::error_t &e) {
		COCAINE_LOG_ERROR(m_log, "%s-%s: enqueue failed: %s", m_queue_name.c_str(), m_queue_id.c_str(), e.what());
	}

	return false;
}

void queue_driver::on_process_successed(const ioremap::elliptics::data_pointer &)
{
	ioremap::elliptics::session sess = m_client.create_session();
	sess.set_exceptions_policy(ioremap::elliptics::session::no_exceptions);

	dnet_id queue_id;
	queue_id.type = 0;
	queue_id.group_id = 0;
	sess.transform(m_queue_id, queue_id);

	sess.exec(&queue_id, m_queue_ack_event, ioremap::elliptics::data_pointer()).connect(
		ioremap::elliptics::async_result<ioremap::elliptics::exec_result_entry>::result_function(),
		[this] (const ioremap::elliptics::error_info &error) {
			if (error) {
				COCAINE_LOG_ERROR(m_log, "data not acked");
			} else {
				COCAINE_LOG_INFO(m_log, "data acked");
			}
		}
		);

	// Successful processing gives right to request more data,
	// gives support to inbound data rate
	get_more_data();
}

queue_driver::downstream_t::downstream_t(queue_driver *queue, const ioremap::elliptics::data_pointer &d)
	: m_queue(queue), m_data(d), m_attempts(0)
{
}

void queue_driver::downstream_t::write(const char *data, size_t size)
{
	std::string ret(data, size);

	COCAINE_LOG_INFO(m_queue->m_log, "%s-%s: from worker: received: '%s'",
			m_queue->m_queue_name.c_str(), m_queue->m_queue_id.c_str(), ret.c_str());
}

void queue_driver::downstream_t::error(cocaine::error_code code, const std::string &msg)
{
	++m_attempts;

	COCAINE_LOG_ERROR(m_queue->m_log, "%s-%s: from worker: error: attempts: %d/%d: %s [%d]",
			m_queue->m_queue_name.c_str(), m_queue->m_queue_id.c_str(), m_attempts, FAIL_LIMIT, msg.c_str(), code);

	// disable error processing for now, I'm not sure that downstream-out-of-downstream
	// creation will not lead to unlimited number of attempts for the same event
#if 0
	if (m_attempts < FAIL_LIMIT)
		m_queue->process_data(m_data);
#endif
}

void queue_driver::downstream_t::close()
{
}
