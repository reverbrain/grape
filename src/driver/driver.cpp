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

#include <cocaine/app.hpp>
#include <cocaine/exceptions.hpp>
#include <cocaine/api/event.hpp>
#include <cocaine/api/stream.hpp>
#include <cocaine/api/service.hpp>

#include "cocaine-json-trait.hpp"

#include "grape/rapidjson/document.h"

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
m_deadline(args.get("deadline", 0.0f).asDouble()),
m_queue_length(0),
m_queue_length_max(0),
m_no_data(false)
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

	char *ptr = strchr((char *)m_worker_event.c_str(), '@');
	if (!ptr)
		throw configuration_error_t("invalid worker event ('emit' config entry), it must contain @ sign");

	std::string app_name(m_worker_event.c_str(), ptr - m_worker_event.c_str());
	std::string event_name(ptr+1);

	auto storage = cocaine::api::storage(context, "core");
	Json::Value profile = storage->get<Json::Value>("profiles", app_name);
	int queue_limit = profile["queue-limit"].asInt();

	m_queue_length_max = queue_limit * 9 / 10;

	m_idle_timer.set<queue_driver, &queue_driver::on_idle_timer_event>(this);
	m_idle_timer.start(1.0f, 1.0f);
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
	result["queue-id"] = m_queue_id;
	result["queue-stats"]["inserted"] = (int)m_queue_length;
	result["queue-stats"]["max-length"] = (int)m_queue_length_max;

	return result;
}

void queue_driver::on_idle_timer_event(ev::timer &, int)
{
	COCAINE_LOG_ERROR(m_log, "timer: checking queue: length: %d, max: %d, no-data: %d",
			m_queue_length, m_queue_length_max, m_no_data);

	for (int i = m_queue_length; i < m_queue_length_max; ++i) {
		if (i > m_queue_length && m_no_data)
			break;

		get_more_data();
	}

	COCAINE_LOG_ERROR(m_log, "timer: checking queue completed: length: %d, max: %d, no-data: %d",
			m_queue_length, m_queue_length_max, m_no_data);
}

void queue_driver::get_more_data()
{
	COCAINE_LOG_INFO(m_log, "get_more_data: checking queue: length: %d, max: %d, no-data: %d",
			m_queue_length, m_queue_length_max, m_no_data);

	if (m_queue_length < m_queue_length_max) {

		ioremap::elliptics::session sess = m_client.create_session();

		dnet_id queue_id;
		queue_id.type = 0;
		queue_id.group_id = 0;
		sess.transform(m_queue_id, queue_id);

		queue_inc(1);

		sess.set_exceptions_policy(ioremap::elliptics::session::no_exceptions);
		sess.exec(&queue_id, m_queue_pop_event, ioremap::elliptics::data_pointer()).connect(
			std::bind(&queue_driver::on_queue_request_data, this, std::placeholders::_1),
			std::bind(&queue_driver::on_queue_request_complete, this, std::placeholders::_1)
		);

		COCAINE_LOG_INFO(m_log, "%s: request has been sent to queue %s-%s",
				m_queue_pop_event.c_str(), m_queue_name.c_str(), m_queue_id.c_str());
	}
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
		COCAINE_LOG_INFO(m_log, "%s-%s: data: size: %d", m_queue_name.c_str(), m_queue_id.c_str(), context.data().size());

		if (!context.data().empty()) {
			m_no_data = false;

			bool processed_ok = process_data(context.data());

			COCAINE_LOG_INFO(m_log, "%s-%s: data: size: %d, processed-ok: %d",
					m_queue_name.c_str(), m_queue_id.c_str(), context.data().size(),
					processed_ok);

			m_no_data = !processed_ok;
		} else {
			m_no_data = true;
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

	queue_dec(1);
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

		return true;
	} catch (const cocaine::error_t &e) {
		COCAINE_LOG_ERROR(m_log, "%s-%s: enqueue failed: %s: queue-len: %d/%d",
				m_queue_name.c_str(), m_queue_id.c_str(), e.what(),
				m_queue_length, m_queue_length_max);
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

void queue_driver::queue_dec(int num)
{
	m_queue_length -= num;
}

void queue_driver::queue_inc(int num)
{
	m_queue_length += num;
}

queue_driver::downstream_t::downstream_t(queue_driver *queue, const ioremap::elliptics::data_pointer &d)
	: m_queue(queue), m_data(d), m_attempts(0)
{
	m_queue->queue_inc(1);
}

queue_driver::downstream_t::~downstream_t()
{
	m_queue->queue_dec(1);

	COCAINE_LOG_INFO(m_queue->m_log, "%s-%s: ~downstream: attempts: %d\n",
			m_queue->m_queue_name.c_str(), m_queue->m_queue_id.c_str(), m_attempts);

	if (m_attempts == 0)
		m_queue->get_more_data();
}

void queue_driver::downstream_t::write(const char *data, size_t size)
{
	std::string ret(data, size);

	COCAINE_LOG_INFO(m_queue->m_log, "%s-%s: from worker: received: size: %zd, data: '%s'",
			m_queue->m_queue_name.c_str(), m_queue->m_queue_id.c_str(),
			ret.size(), ret.c_str());
}

void queue_driver::downstream_t::error(int code, const std::string &msg)
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
