#include <xroute/xroute.hpp>
#include <xroute/elliptics.hpp>
#include <xroute/logger.hpp>

using namespace ioremap::srw;
using namespace ioremap::xroute;

extern "C" {
	void init(shared *sh);
}

void init(shared *sh)
{
	logger::instance()->init(sh->get_log(), __LOG_ERROR | __LOG_INFO | __LOG_NOTICE, true);

	event_handler_t *ev = new elliptics_event_handler_t(sh->get_log(), sh->get_config(), sh);

	sh->add_handler("new-task", ev);
}
