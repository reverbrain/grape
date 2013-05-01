#include "forward.hpp"

#include <fstream>
#include <sstream>

#include <cocaine/framework/logging.hpp>
#include <cocaine/framework/application.hpp>
#include <cocaine/framework/worker.hpp>

#include "rapidjson/document.h"
#include "rapidjson/filestream.h"

namespace cocaine {
namespace worker {

using namespace ioremap::elliptics;
using namespace cocaine::framework;

executor::executor(std::shared_ptr<service_manager_t> service_manager)
	: application<executor>(service_manager)
{
	m_log = service_manager->get_system_logger();
}

void executor::initialize()
{
	FILE *cf = NULL;
	try {
		const char CONFFILE[] = "forward.conf";

		cf = fopen(CONFFILE, "r");
		if (!cf)
			throw configuration_error_t("failed to open config file");

		rapidjson::FileStream fs(cf);

		rapidjson::Document doc;
		doc.ParseStream<rapidjson::kParseDefaultFlags, rapidjson::UTF8<>, rapidjson::FileStream>(fs);
		if (doc.HasParseError()) {
			std::ostringstream out;
			out << "can not parse config file: " << doc.GetParseError();
			throw configuration_error_t(out.str().c_str());
		}

		std::string logfile = "/dev/stderr";
		int loglevel = DNET_LOG_INFO;

		if (doc.HasMember("logfile"))
			logfile = doc["logfile"].GetString();
		if (doc.HasMember("loglevel"))
			logfile = doc["loglevel"].GetInt();

		try {
			m_logger.reset(new file_logger(logfile.c_str(), loglevel));
			m_node.reset(new node(*m_logger));
		} catch (std::exception &e) {
			throw configuration_error_t(e.what());
		}

		if (!doc.HasMember("remotes"))
			throw configuration_error_t("no 'remotes' section found in config");

		const rapidjson::Value &remotesArray = doc["remotes"];
		int remotes_added = 0;
		for (rapidjson::Value::ConstValueIterator itr = remotesArray.Begin(); itr != remotesArray.End(); ++itr) {
			try {
				m_node->add_remote(itr->GetString());
				++remotes_added;
			} catch (...) {
				// We don't care, really
			}
		}
		if (remotes_added == 0)
			throw configuration_error_t("no remote nodes have been added");


		if (!doc.HasMember("groups"))
			throw configuration_error_t("no 'groups' section found in config");

		const rapidjson::Value &groupsArray = doc["groups"];
		std::transform(groupsArray.Begin(), groupsArray.End(),
			std::back_inserter(m_groups),
			std::bind(&rapidjson::Value::GetInt, std::placeholders::_1)
			);

		if (!doc.HasMember("forward-event"))
			throw configuration_error_t("no 'forward-event' section found in config");

		//FIXME: is there a reasonable default? or make it strongly required?
		m_forward_event.assign(doc["forward-event"].GetString());
	}
	catch (const std::exception &e) {
		if (cf)
			fclose(cf);
		COCAINE_LOG_ERROR(m_log, "failed to configure: %s", e.what());
		throw;
	}

	if (cf)
		fclose(cf);

	//FIXME: there we need to know names of the driver and the app in advance
	on<queue_handler>("forward/queue");
	on_unregistered(&executor::on_unexpected_event);
}

session executor::create_session()
{
	session sess(*m_node);
	sess.set_groups(m_groups);
	return sess;
}

std::string executor::on_unexpected_event(const std::string &event, const std::vector<std::string> &/*args*/)
{
	COCAINE_LOG_ERROR(m_log, "got unexpected event: %s", event.c_str());
	return "";
}

void executor::queue_handler::on_chunk(const char *data, size_t size)
{
	COCAINE_LOG_INFO(app()->m_log, "got chunk from the driver (%d): %s", size, data);

	try {
		data_pointer d = data_pointer::from_raw(const_cast<char*>(data), size);

		session sess = app()->create_session();
		sess.set_cflags(DNET_FLAGS_NOLOCK);

		dnet_id id;
		id.group_id = 0;
		id.type = 0;
		sess.transform(d, id);

		scope_t scope = { false, response() };
		session_handler handler = { std::make_shared<scope_t>(scope), app()->m_log, app()->m_forward_event };
		sess.set_filter(filters::all_with_ack);
		sess.set_exceptions_policy(session::no_exceptions);

		sess.exec(&id, app()->m_forward_event, d).connect(handler, handler);

		COCAINE_LOG_INFO(app()->m_log, "passing chunk to %s", app()->m_forward_event.c_str());

	} catch (std::exception &e) {
		COCAINE_LOG_ERROR(app()->m_log, "failed to pass chunk to %s: %s", app()->m_forward_event.c_str(), e.what());
		response()->error(resource_error, e.what());
	} catch (...) {
		COCAINE_LOG_ERROR(app()->m_log, "failed to pass chunk to %s", app()->m_forward_event.c_str());
		response()->error(resource_error, "unknown error");
	}
}

void executor::queue_handler::on_close()
{
}

void executor::queue_handler::on_error(int, const std::string&)
{
	//FIXME: and what to do now?
}

void executor::session_handler::operator ()(const exec_result_entry &result)
{
	if (scope->closed) {
		COCAINE_LOG_INFO(log, "got reply from %s, but stream is already closed", sent_event.c_str());
		return;
	}

	if (auto upstream = scope->upstream.lock()) {
		if (result.status() && !scope->closed) {
			COCAINE_LOG_ERROR(log, "got error from %s: %s", sent_event.c_str(), result.error().message().c_str());
			upstream->error(resource_error, result.error().message());
			scope->closed = true;
		} else if (!scope->closed) {
			COCAINE_LOG_INFO(log, "got reply from %s", sent_event.c_str());
			upstream->write("progress", 8);
		}
	}
}

void executor::session_handler::operator ()(const error_info &error)
{
	if (scope->closed)
		return;
	scope->closed = true;

	if (auto upstream = scope->upstream.lock()) {
		if (error) {
			COCAINE_LOG_ERROR(log, "got complete error from %s: %s", sent_event.c_str(), error.message().c_str());
			upstream->error(resource_error, error.message().c_str());
		} else {
			COCAINE_LOG_INFO(log, "got final from %s", sent_event.c_str());
			upstream->write("done", 4);
		}
	}
}

} // namespace worker
} // namespace cocaine

int main(int argc, char **argv)
{
	return cocaine::framework::worker_t::run<cocaine::worker::executor>(argc, argv);
}
