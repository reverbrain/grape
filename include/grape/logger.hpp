#ifndef __XROUTE_LOGGER_HPP
#define __XROUTE_LOGGER_HPP

#include <stdio.h>

#include <boost/thread/mutex.hpp>

#define __LOG_CHECK  __attribute__ ((format(printf, 3, 4)))

namespace ioremap { namespace grape {

enum grape_log_levels {
	__LOG_DATA = 0,
	__LOG_ERROR,
	__LOG_INFO,
	__LOG_NOTICE,
	__LOG_DEBUG,
};

class logger {
	public:
		int log_level();

		static logger *instance(void);
		void init(const std::string &path, int log_level, bool flush = true);

		void do_log(const int log_level, const char *format, ...) __LOG_CHECK;

	private:
		int m_log_level;
		FILE *m_log;
		bool m_flush;
		boost::mutex m_lock;

		logger(void);
		logger(const logger &);
		~logger(void);

		logger & operator= (logger const &);

		static void destroy(void);

		static logger *m_logger;
};

#define xlog(level, msg...) \
	do { \
		if (ioremap::grape::logger::instance()->log_level() >= (level)) \
			ioremap::grape::logger::instance()->do_log((level), ##msg); \
	} while (0)

}}

#endif /* __XROUTE_LOGGER_HPP */
