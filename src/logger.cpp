#include <stdarg.h>

#include <grape/logger.hpp>

using namespace ioremap::grape;

static boost::mutex logger_init_lock;
logger *logger::m_logger = NULL;

logger::logger(void) : m_log_level(__LOG_ERROR), m_log(NULL), m_flush(true)
{
}

logger *logger::instance(void)
{
	if (!m_logger) {
		boost::mutex::scoped_lock(logger_init_lock);
		if (!m_logger)
			m_logger = new logger();
	}

	return m_logger;
}

void logger::init(const std::string &path, int log_level, bool flush)
{
	boost::mutex::scoped_lock guard(m_lock);

	m_log_level = log_level;
	m_flush = flush;

	if (m_log)
		fclose(m_log);

	m_log = fopen(path.c_str(), "a");
	if (!m_log) {
		int err = -errno;
		std::ostringstream str;

		str << "Could not open log '" << path << "': " << err;
		throw std::runtime_error(str.str());
	}
}

void logger::do_log(const int level, const char *format, ...)
{
	boost::mutex::scoped_lock guard(m_lock);

	if (m_log) {
		char str[64];
		struct tm tm;
		struct timeval tv;

		gettimeofday(&tv, NULL);
		localtime_r((time_t *)&tv.tv_sec, &tm);
		strftime(str, sizeof(str), "%F %R:%S", &tm);


		va_list args;
		char buf[1024];
		int buflen = sizeof(buf);

		va_start(args, format);
		vsnprintf(buf, buflen, format, args);
		buf[buflen-1] = '\0';
		fprintf(m_log, "%s.%06lu %1d: %s", str, (unsigned long)tv.tv_usec, level, buf);
		va_end(args);

		if (m_flush)
			fflush(m_log);
	}
}
