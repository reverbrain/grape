#include <stdarg.h>

#include <grape/logger.hpp>

using namespace ioremap::grape;

static boost::mutex logger_init_lock_;
logger *logger::logger_ = NULL;

logger::logger(void) : log_mask_(__LOG_ERROR | __LOG_INFO | __LOG_DATA), log_(NULL), flush_(true)
{
}

logger *logger::instance(void)
{
	if (!logger_) {
		boost::mutex::scoped_lock(logger_init_lock_);
		if (!logger_)
			logger_ = new logger();
	}

	return logger_;
}

void logger::init(const std::string &path, int log_mask, bool flush)
{
	boost::mutex::scoped_lock guard(lock_);

	log_mask_ = log_mask;
	flush_ = flush;

	if (log_)
		fclose(log_);

	log_ = fopen(path.c_str(), "a");
	if (!log_) {
		int err = -errno;
		std::ostringstream str;

		str << "Could not open log '" << path << "': " << err;
		throw std::runtime_error(str.str());
	}
}

void logger::do_log(const int mask, const char *format, ...)
{
	boost::mutex::scoped_lock guard(lock_);

	if (log_) {
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
		fprintf(log_, "%s.%06lu %1x: %s", str, (unsigned long)tv.tv_usec, mask, buf);
		va_end(args);

		if (flush_)
			fflush(log_);
	}
}
