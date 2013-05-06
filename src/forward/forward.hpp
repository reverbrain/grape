#ifndef EXECUTOR_HPP__
#define EXECUTOR_HPP__

#include <cocaine/framework/logging.hpp>
#include <cocaine/framework/application.hpp>
#include <elliptics/cppdef.h>

namespace cocaine {
namespace worker {

class executor : public framework::application<executor>
{
	public:
		struct scope_t {
			bool closed;
			std::weak_ptr<api::stream_t> upstream;
		};

		struct session_handler {
			void operator() (const ioremap::elliptics::exec_result_entry &result);
			void operator() (const ioremap::elliptics::error_info &error);

			std::shared_ptr<scope_t> scope;
			std::shared_ptr<cocaine::framework::logger_t> log;
			std::string sent_event;
		};

		struct queue_handler : public cocaine::framework::handler<executor> {
			queue_handler(std::shared_ptr<executor> app): handler<executor>(app) {
			}

			virtual void on_chunk(const char *data, size_t size);
			virtual void on_close();
			virtual void on_error(int code, const std::string &message);
		};


		std::string on_unexpected_event(const std::string &event, const std::vector<std::string> &args);

		executor(std::shared_ptr<framework::service_manager_t> service_manager);
		void initialize();

		ioremap::elliptics::session create_session();

	private:
		elliptics_client_state _elliptics_client_state;
		std::shared_ptr<cocaine::framework::logger_t> m_log;
		std::string m_forward_event;
};

} // namespace worker
} // namespace cocaine

#endif // EXECUTOR_HPP__
