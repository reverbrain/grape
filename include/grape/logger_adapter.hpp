#include <memory>

#include <cocaine/framework/logging.hpp>
#include <elliptics/session.hpp>

namespace ioremap { namespace grape {

class logger_adapter : public cocaine::framework::logger_t
{
private:
	std::shared_ptr<ioremap::elliptics::logger> base_logger;
public:
	logger_adapter(std::shared_ptr<ioremap::elliptics::logger> base_logger)
		: base_logger(base_logger)
	{}

	logger_adapter(ioremap::elliptics::logger base_logger)
		: base_logger(std::make_shared<ioremap::elliptics::logger>(base_logger))
	{}

	virtual
	~logger_adapter() {
		// pass
	}

	template<typename... Args>
	void emit(cocaine::logging::priorities priority, const std::string &format, const Args&... args) {
		emit(priority, cocaine::format(format, args...));
	}

	virtual
	void emit(cocaine::logging::priorities priority,
			const std::string &message) {
		if (message.back() != '\n') {
			base_logger->log(prio_to_dnet_log_level(priority), (message + "\n").c_str());
		}
		else {
			base_logger->log(prio_to_dnet_log_level(priority), message.c_str());
		}
	}

	virtual
	cocaine::logging::priorities verbosity() const {
		return dnet_log_level_to_prio(base_logger->get_log_level());
	}

	// XXX: Following methods have been copy-pasted from elliptics' srw.cpp

	// INFO level has value 2 in elliptics and value 3 in cocaine,
	// nevertheless we want to support unified sense of INFO across both systems,
	// so we need to play with the mapping a bit.
	//
	// Specifically:
	//  1) cocaine warning and info levels are both mapped into eliptics info level
	//  2) elliptics notice level means cocaine info level

	static cocaine::logging::priorities dnet_log_level_to_prio(int level) {
		cocaine::logging::priorities prio = (cocaine::logging::priorities)level;
		// elliptics info level becomes cocaine warning level,
		// so we must to level it up
		if (prio == cocaine::logging::warning) {
			prio = cocaine::logging::info;
		}
		return prio;
	}

	static int prio_to_dnet_log_level(cocaine::logging::priorities prio) {
		int level = DNET_LOG_DATA;
		if (prio == cocaine::logging::debug)
			level = DNET_LOG_DEBUG;
		if (prio == cocaine::logging::info)
			level = DNET_LOG_INFO;
		if (prio == cocaine::logging::warning)
			level = DNET_LOG_INFO;
		if (prio == cocaine::logging::error)
			level = DNET_LOG_ERROR;
		return level;
	}
};

}}
