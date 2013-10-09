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

#include <stdlib.h>
#include <time.h>
#include <cmath>

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

#include <grape/data_array.hpp>
#include <grape/entry_id.hpp>

#include "driver.hpp"

namespace {

const int MICROSECONDS_IN_SECOND = 1000000;

inline uint64_t microseconds(double seconds) {
	return uint64_t(seconds * MICROSECONDS_IN_SECOND);
} 

inline double seconds(uint64_t microseconds) {
	return double(microseconds) / MICROSECONDS_IN_SECOND;
} 

inline uint64_t microseconds_now() {
	timespec t;
	clock_gettime(CLOCK_MONOTONIC_RAW, &t);
	return t.tv_sec * MICROSECONDS_IN_SECOND + t.tv_nsec / 1000;
}

const double STAT_INTERVAL = 1.0; // in seconds

}

using namespace cocaine::driver;

queue_driver::queue_driver(cocaine::context_t& context, cocaine::io::reactor_t &reactor, cocaine::app_t &app,
		const std::string& name, const Json::Value& args)
	: category_type(context, reactor, app, name, args)
	, m_context(context)
	, m_app(app)
	, m_log(new cocaine::logging::log_t(context, cocaine::format("driver/%s", name)))
	// configuration
	, m_wait_timeout(args.get("wait-timeout", 60).asInt())
	, m_check_timeout(args.get("check-timeout", 30).asInt())
	, m_request_size(args.get("request-size", 100).asInt())
	, m_rate_upper_limit(args.get("rate-upper-limit", 0.0f).asDouble())
	, m_initial_rate_boost(args.get("initial-rate-boost", 0.0f).asDouble())
	, m_queue_name(args.get("source-queue-app", "queue").asString())
	, m_worker_event(args.get("worker-emit-event", "emit").asString())
	, m_queue_pop_event(m_queue_name + "@" + args.get("source-queue-pop-event", "pop-multiple-string").asString())
	, m_timeout(args.get("timeout", 0.0f).asDouble())
	, m_deadline(args.get("deadline", 0.0f).asDouble())
	// worker queue
	, m_queue_length(0)
	, m_queue_length_max(0)
	// rate control
	, m_request_timer(reactor.native())
	, m_rate_control_timer(reactor.native())
	, last_process_done_time(0)
	, processed_time(0)
	, process_count(0)
	, receive_count(0)
	, request_count(0)
	, rate_focus(0.0)
	, growth_step(0)
	, growth_time(0.0)
	, m_queue_src_key(0)
{
	COCAINE_LOG_INFO(m_log, "init: %s driver", m_queue_name.c_str());

	srand(time(NULL));

	try {
		std::string s = Json::FastWriter().write(args);

		rapidjson::Document doc;
		doc.Parse<0>(s.c_str());

		m_client = elliptics_client_state::create(doc);

		//TODO: remove extra code with queue-groups: driver doesn't need group set override
		// 
		std::string groups_key = "groups";
		if (doc.HasMember("queue-groups"))
			groups_key = "queue-groups";

		const rapidjson::Value &groupsArray = doc[groups_key.c_str()];
		std::transform(groupsArray.Begin(), groupsArray.End(), std::back_inserter(m_queue_groups),
				std::bind(&rapidjson::Value::GetInt, std::placeholders::_1));

		COCAINE_LOG_INFO(m_log, "init: elliptics client created");

	} catch (const std::exception &e) {
		COCAINE_LOG_ERROR(m_log, "init: %s driver constructor exception: %s", m_queue_name.c_str(), e.what());
		throw;
	}

	if (m_queue_name.empty())
		throw configuration_error("no queue name has been specified");

	// getting worker queue length from app profile
	{
		char *ptr = strchr((char *)m_worker_event.c_str(), '@');
		if (!ptr)
			throw configuration_error("worker-emit-event: must contain '@' character");

		std::string app_name(m_worker_event.c_str(), ptr - m_worker_event.c_str());
		std::string event_name(ptr+1);

		auto storage = cocaine::api::storage(context, "core");
		Json::Value profile = storage->get<Json::Value>("profiles", app_name);
		
		int queue_limit = profile["queue-limit"].asInt();

		m_queue_length_max = queue_limit * 9 / 10;
	}

	// Request timer will fire immediately
	m_request_timer.set<queue_driver, &queue_driver::on_request_timer_event>(this);
	last_request_time = microseconds_now();
	m_request_timer.set(0.0, (STAT_INTERVAL / 100));
	m_request_timer.start();

	// Rate control timer will fire after stat collection interval allowing
	// to accumulate initial receive/process statistics data.
	m_rate_control_timer.set<queue_driver, &queue_driver::on_rate_control_timer_event>(this);
	m_rate_control_timer.set(STAT_INTERVAL, STAT_INTERVAL);
	m_rate_control_timer.start();

	COCAINE_LOG_INFO(m_log, "init: %s driver started", m_queue_name.c_str());
}

queue_driver::~queue_driver()
{
	m_request_timer.stop();
	m_rate_control_timer.stop();
}

Json::Value queue_driver::info() const
{
	Json::Value result;

	result["type"] = "persistent-queue";
	result["name"] = m_queue_name;
	result["worker-queue"]["pending"] = (int)m_queue_length;
	result["worker-queue"]["max-length"] = (int)m_queue_length_max;

	return result;
}

void queue_driver::on_request_timer_event(ev::timer &, int)
{
	get_more_data();
}

void queue_driver::on_rate_control_timer_event(ev::timer &, int)
{
	int process_speed = 1;
	{
//		COCAINE_LOG_INFO(m_log, "values: process count %d, time %ld", process_count, processed_time);
		int count = std::atomic_exchange<int>(&process_count, 0);
		uint64_t time = std::atomic_exchange<uint64_t>(&processed_time, 0);
		COCAINE_LOG_INFO(m_log, "swapped: process count %d, time %ld (%d, %ld)", count, time, process_count, processed_time);
		if (count > 0) {
			process_speed = STAT_INTERVAL * count / seconds(time);
		}
	}
	int receive_speed = std::atomic_exchange<int>(&receive_count, 0);
	int observed_request_speed = std::atomic_exchange<int>(&request_count, 0);

	COCAINE_LOG_INFO(m_log, "%s: rate: request %d/s, receive %d/s, focus: %.3f, process %d/s",
			m_queue_name.c_str(), observed_request_speed, receive_speed, rate_focus, process_speed
			);

	double delta = double(receive_speed - observed_request_speed) / observed_request_speed;

	if (rate_focus == 0) {
		rate_focus = (m_initial_rate_boost > 0.0 ? m_initial_rate_boost : process_speed * 0.5);
		growth_time = 5.0;
	}

	const double C = 1.0; //0.4;

	double projected_request_speed = 1.0;
	if (delta > -0.05) {
		projected_request_speed = rate_focus + C * pow(growth_step - growth_time, 3);
		++growth_step;
	} else {
		growth_step = 0;
		if ((rate_focus - receive_speed) >= 0.0) {
			rate_focus = receive_speed * 0.9;
			growth_time = cbrt((receive_speed - rate_focus) / C);
		} else {
			rate_focus = std::min(receive_speed, process_speed);
			growth_time = 0.0;
		}
		projected_request_speed = receive_speed * 0.8;
	}

	projected_request_speed = std::max(1.0, projected_request_speed);
	if (m_rate_upper_limit > 0.0) {
		projected_request_speed = std::min(projected_request_speed, m_rate_upper_limit);
	}

	COCAINE_LOG_INFO(m_log, "%s: rate: delta: %.3f, focus: %.3f, growth_step: %d, growth_time: %.3f",
			m_queue_name.c_str(), delta, rate_focus, growth_step, growth_time
			);

	// Setting request timer to new interval.
	// Timer will fire first time immediately, it is important
	// so that even with borderline speed of 1 rps there will be at least
	// one request made between calibration points  
	double request_interval_seconds = STAT_INTERVAL / projected_request_speed;
	m_request_timer.stop();
	last_request_time = microseconds_now();
	m_request_timer.set(0.0f, request_interval_seconds);
	m_request_timer.start();

	COCAINE_LOG_INFO(m_log, "%s: rate: new request speed %.3f/s, interval %ld",
			m_queue_name.c_str(), projected_request_speed, microseconds(request_interval_seconds)
			);
}

void queue_driver::get_more_data()
{
	uint64_t now = microseconds_now();
	uint64_t elapsed = now - last_request_time;

	COCAINE_LOG_INFO(m_log, "%s: more-data: elapsed %ld, queue-len: %d/%d",
			m_queue_name.c_str(), elapsed, m_queue_length, m_queue_length_max);

	if (m_queue_length >= m_queue_length_max) {
		return;
	}

	send_request();

	queue_inc(1);

	++request_count;

	last_request_time = now;
}

void queue_driver::send_request()
{
	ioremap::elliptics::session sess = m_client.create_session();
	if (!m_wait_timeout) {
		sess.set_timeout(m_wait_timeout);
	}

	std::shared_ptr<queue_request> req = std::make_shared<queue_request>();
	//req->num = m_queue_length_max / step;
	req->num = m_request_size;
	req->src_key = m_queue_src_key;
	req->id.group_id = 0;

	std::string random_data = m_queue_name + std::to_string(req->src_key) + std::to_string(rand());
	sess.transform(random_data, req->id);

	sess.set_groups(m_queue_groups);

	sess.set_exceptions_policy(ioremap::elliptics::session::no_exceptions);
	sess.exec(&req->id, req->src_key, m_queue_pop_event, std::to_string(req->num)).connect(
		std::bind(&queue_driver::on_queue_request_data, this, req, std::placeholders::_1),
		std::bind(&queue_driver::on_queue_request_complete, this, req, std::placeholders::_1)
	);

	COCAINE_LOG_INFO(m_log, "%s: %s: pop request has been sent: requested number of events: %d, queue-len: %d/%d",
			m_queue_name.c_str(), dnet_dump_id(&req->id), req->num, m_queue_length, m_queue_length_max);

	++m_queue_src_key;
}

void queue_driver::on_queue_request_data(std::shared_ptr<queue_request> req, const ioremap::elliptics::exec_result_entry &result)
{
	try {
		if (result.error()) {
			COCAINE_LOG_ERROR(m_log, "%s: error: %d: %s",
				m_queue_name.c_str(), result.error().code(), result.error().message());

			// Normally queue counter decremented when worker completes item processing.
			// But if there is no data for the worker, we decrement counter here.
			queue_dec(1);

			return;
		}

		ioremap::elliptics::exec_context context = result.context();

		// Received context is passed directly to the worker, so that
		// worker could use it to continue talking to the same queue instance.
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

		COCAINE_LOG_INFO(m_log, "%s: %s: src-key: %d, data-size: %d",
				m_queue_name.c_str(), dnet_dump_id(&req->id),
				context.src_key(), context.data().size()
				);

		bool enqueued = false;

		// queue.pop returns no data when queue is empty.
		if (!context.data().empty()) {
			// ioremap::grape::entry_id entry_id = ioremap::grape::entry_id::from_dnet_raw_id(context.src_id());
			// COCAINE_LOG_INFO(m_log, "%s: %s: id: %d-%d, size: %d", m_queue_name.c_str(), dnet_dump_id(&req->id),
			// 		entry_id.chunk, entry_id.pos,
			// 		context.data().size()
			// 		);

			enqueued = enqueue_data(context);

			COCAINE_LOG_INFO(m_log, "%s: %s: src-key: %d, data-size: %d, enqueued %d",
					m_queue_name.c_str(), dnet_dump_id(&req->id),
					context.src_key(), context.data().size(),
					enqueued
					);

			++receive_count;

		} else {
			// Normally queue counter decremented when worker completes item processing.
			// But if there is no data for the worker, we decrement counter here.
			queue_dec(1);
		}

	} catch(const std::exception &e) {
		COCAINE_LOG_ERROR(m_log, "%s: %s: exception: %s", m_queue_name.c_str(), dnet_dump_id(&req->id), e.what());
	}
}

void queue_driver::on_queue_request_complete(std::shared_ptr<queue_request> req, const ioremap::elliptics::error_info &error)
{
	if (error) {
		COCAINE_LOG_ERROR(m_log, "%s: %s: queue request completion error: %s",
				m_queue_name.c_str(), dnet_dump_id(&req->id), error.message().c_str());

		// Normally queue counter decremented when worker completes item processing.
		// But if there is no data for the worker, we decrement counter here.
		queue_dec(1);

	} else {
		COCAINE_LOG_INFO(m_log, "%s: %s: queue request completed",
				m_queue_name.c_str(), dnet_dump_id(&req->id));
	}
}

bool queue_driver::enqueue_data(const ioremap::elliptics::exec_context &context)
{
	// Pass data to the worker.

	try {
		ioremap::elliptics::data_pointer packet = context.native_data();
		api::policy_t policy(false, m_timeout, m_deadline);
		auto downstream = std::make_shared<downstream_t>(this);
		auto upstream = m_app.enqueue(api::event_t(m_worker_event, policy), downstream);
		upstream->write((char *)packet.data(), packet.size());

		return true;

	} catch (const cocaine::error_t &e) {
		COCAINE_LOG_ERROR(m_log, "%s: enqueue failed: %s: queue-len: %d/%d",
				m_queue_name.c_str(), e.what(),
				m_queue_length, m_queue_length_max);
	}

	return false;
}

void queue_driver::on_worker_complete(uint64_t start_time, bool success)
{
	queue_dec(1);

	// Measure time between consecutive completions to get an estimation on
	// worker's possible processing speed.   
	// Take into account successful processings only, to slow things down
	// if there is something wrong between driver and a workers 

	if (success) {
		++process_count;

		uint64_t now = microseconds_now();
		uint64_t time_spent = now - start_time;

		uint64_t last_time = std::atomic_exchange<uint64_t>(&last_process_done_time, now);
		// skip it on the first run
		if (last_time != 0) {
			uint64_t time_from_previous = now - last_time;
			time_spent = std::min(time_spent, time_from_previous);
		}

		processed_time += time_spent;
	}
}

void queue_driver::queue_dec(int num)
{
	m_queue_length -= num;
}

void queue_driver::queue_inc(int num)
{
	m_queue_length += num;
}

queue_driver::downstream_t::downstream_t(queue_driver *parent)
	: parent(parent), success(true)
{
	start_time = microseconds_now();
}

queue_driver::downstream_t::~downstream_t()
{
}

void queue_driver::downstream_t::write(const char *data, size_t size)
{
	std::string ret(data, size);
	COCAINE_LOG_INFO(parent->m_log, "%s: from worker: received: size: %d, data: '%s'",
			parent->m_queue_name.c_str(), ret.size(), ret.c_str());
}

void queue_driver::downstream_t::error(int code, const std::string &msg)
{
	COCAINE_LOG_ERROR(parent->m_log, "%s: from worker: error: %s [%d]",
			parent->m_queue_name.c_str(), msg.c_str(), code);
	success = false;
}

void queue_driver::downstream_t::close()
{
	COCAINE_LOG_INFO(parent->m_log, "%s: from worker: %s", parent->m_queue_name.c_str(), (success ? "done" : "failed"));
	parent->on_worker_complete(start_time, success);
}
