#ifndef __XROUTE_LOGGER_HPP
#define __XROUTE_LOGGER_HPP

#include <stdio.h>

#include <boost/thread/mutex.hpp>

#define __LOG_CHECK  __attribute__ ((format(printf, 3, 4)))

namespace ioremap { namespace grape {

#define __LOG_NOTICE			(1<<0)
#define __LOG_INFO			(1<<1)
#define __LOG_TRANS			(1<<2)
#define __LOG_ERROR			(1<<3)
#define __LOG_DSA			(1<<4)
#define __LOG_DATA			(1<<5)

class logger {
	public:
		int log_mask_;

		static logger *instance(void);
		void init(const std::string &path, int log_mask, bool flush = true);

		void do_log(const int mask, const char *format, ...) __LOG_CHECK;

	private:
		FILE *log_;
		bool flush_;
		boost::mutex lock_;

		logger(void);
		logger(const logger &);
		~logger(void);

		logger & operator= (logger const &);

		static void destroy(void);

		static logger *logger_;
};

#define xlog(mask, msg...) \
	do { \
		if (logger::instance()->log_mask_ & (mask)) \
			logger::instance()->do_log((mask), ##msg); \
	} while (0)

}}

#endif /* __XROUTE_LOGGER_HPP */
