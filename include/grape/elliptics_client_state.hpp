#ifndef ELLIPTICS_CLIENT_STATE_HPP__
#define ELLIPTICS_CLIENT_STATE_HPP__

#include <algorithm>

#include "rapidjson/document.h"
#include "rapidjson/prettywriter.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/filestream.h"

#include <elliptics/error.hpp>
#include <elliptics/session.hpp>

#include <sstream>

using namespace ioremap;

class configuration_error : public elliptics::error
{
public:
	explicit configuration_error(const std::string &message) throw()
		: error(EINVAL, message)
	{}
};

static inline void read_groups_array(std::vector<int> *result, const char *name, const rapidjson::Value &value) {
	if (const auto *m = value.FindMember(name)) {
		if (m->value.IsArray()) {
			std::transform(m->value.Begin(), m->value.End(),
				std::back_inserter(*result),
				std::bind(&rapidjson::Value::GetInt, std::placeholders::_1)
				);
		} else {
			std::ostringstream str;
			str << name << "value must be of array type";
			throw configuration_error(str.str().c_str());
		}
	}
}

struct elliptics_client_state {
	std::shared_ptr<elliptics::file_logger> logger;
	std::shared_ptr<elliptics::node> node;
	std::vector<int> groups;

	elliptics::session create_session() {
		elliptics::session session(*node);
		session.set_groups(groups);
		return session;
	}

	// Configure from preparsed json config.
	// Config template:
	// {
	//   remotes: ["localhost:1025:2"],
	//   groups: [2],
	//   logfile: "/dev/stderr",
	//   loglevel: 0
	// }
	static elliptics_client_state create(const rapidjson::Document &args) {
		std::string logfile = "/dev/stderr";
		uint loglevel = DNET_LOG_ERROR;
		std::vector<std::string> remotes;
		std::vector<int> groups;
		int wait_timeout = 0;
		int check_timeout = 0;
		int net_thread_num = 0;
		int io_thread_num = 0;

		try {
			{
				const auto *m = args.FindMember("logfile");
				if (m && m->value.IsString()) {
					logfile = m->value.GetString();
				}
			}
			{
				const auto *m = args.FindMember("loglevel");
				if (m && m->value.IsInt()) {
					loglevel = m->value.GetInt();
				}
			}
			{
				const auto *m = args.FindMember("wait-timeout");
				if (m && m->value.IsInt()) {
					wait_timeout = m->value.GetInt();
				}
			}
			{
				const auto *m = args.FindMember("check-timeout");
				if (m && m->value.IsInt()) {
					check_timeout = m->value.GetInt();
				}
			}
			{
				const auto *m = args.FindMember("net-thread-num");
				if (m && m->value.IsInt()) {
					net_thread_num = m->value.GetInt();
				}
			}
			{
				const auto *m = args.FindMember("io-thread-num");
				if (m && m->value.IsInt()) {
					io_thread_num = m->value.GetInt();
				}
			}

			const rapidjson::Value &remotesArray = args["remotes"];
			std::transform(remotesArray.Begin(), remotesArray.End(),
				std::back_inserter(remotes),
				std::bind(&rapidjson::Value::GetString, std::placeholders::_1)
				);
			const rapidjson::Value &groupsArray = args["groups"];
			std::transform(groupsArray.Begin(), groupsArray.End(),
				std::back_inserter(groups),
				std::bind(&rapidjson::Value::GetInt, std::placeholders::_1)
				);
		} catch (const std::exception &e) {
			throw configuration_error(e.what());
		}

		return create(remotes, groups, logfile, loglevel, wait_timeout, check_timeout, net_thread_num, io_thread_num);
	}

	static elliptics_client_state create(const std::string &conf, rapidjson::Document &doc) {
		FILE *cf;

		cf = fopen(conf.c_str(), "r");
		if (!cf) {
			std::ostringstream str;
			str << "failed to open config file '" << conf << "'";
			throw configuration_error(str.str().c_str());
		}

		try {
			rapidjson::FileStream fs(cf);

			doc.ParseStream<rapidjson::kParseDefaultFlags, rapidjson::UTF8<>, rapidjson::FileStream>(fs);
			if (doc.HasParseError()) {
				std::ostringstream str;
				str << "can not parse config file '" << conf << "': " << doc.GetParseError();
				throw configuration_error(str.str().c_str());
			}

			fclose(cf);
			cf = NULL;

			return create(doc);
		} catch (...) {
			if (cf)
				fclose(cf);

			throw;
		}
	}

	static elliptics_client_state create(const std::vector<std::string> &remotes,
			const std::vector<int> &groups, const std::string &logfile, int loglevel,
			int wait_timeout = 0, int check_timeout = 0,
			int net_thread_num = 0, int io_thread_num = 0) {
		if (remotes.size() == 0) {
			throw configuration_error("no remotes have been specified");
		}
		if (groups.size() == 0) {
			throw configuration_error("no groups have been specified");
		}

		dnet_config cfg;
		memset(&cfg, 0, sizeof(cfg));
		if (net_thread_num) {
			cfg.net_thread_num = net_thread_num;
		}
		if (io_thread_num) {
			cfg.io_thread_num = io_thread_num;
		}

		elliptics_client_state result;
		result.logger.reset(new elliptics::file_logger(logfile.c_str(), loglevel));
		result.node.reset(new elliptics::node(*result.logger, cfg));
		result.groups = groups;

		if (wait_timeout != 0 || check_timeout != 0) {
			// if unset, use default values as in node.c:dnet_node_create()
			wait_timeout = wait_timeout ? wait_timeout : 5;
			check_timeout = check_timeout ? check_timeout : DNET_DEFAULT_CHECK_TIMEOUT_SEC;
			result.node->set_timeouts(wait_timeout, check_timeout);
		}

		if (remotes.size() == 1) {
			// any error is fatal if there is a single remote address
			result.node->add_remote(remotes.front().c_str());

		} else {
			// add_remote throws errors if:
			//  * it can not parse address
			//  * it can not connect to a specified address
			//  * there is address duplication (NOTE: is this still true?)
			// In any case we ignore all errors in hope that at least one would suffice.
			int added = 0;
			for (const auto &i : remotes) {
				try {
					result.node->add_remote(i.c_str());
					++added;
				} catch (const elliptics::error &e) {
					char buf[1024];

					snprintf(buf, sizeof(buf), "could not connect to: %s: %s\n",
							i.c_str(), e.what());

					result.logger->log(DNET_LOG_ERROR, buf);
				}
			}
			if (added == 0) {
				throw configuration_error("no remotes were added successfully");
			}
		}

		return result;
	}
};

#endif // ELLIPTICS_CLIENT_STATE_HPP__
