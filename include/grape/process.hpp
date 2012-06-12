#ifndef __XROUTE_PROCESS_HPP
#define __XROUTE_PROCESS_HPP

namespace ioremap { namespace grape {

class process_t {
	public:
		virtual void process(const std::string &event, const std::string &data) = 0;
};

}}

#endif /* __XROUTE_PROCESS_HPP */
