#include <grape/grape.hpp>

using namespace ioremap::grape;

extern "C" {
	void cleanup(void *__top);
	int process(void *__ev, const char *event, const size_t esize, const char *data, const size_t dsize);
}

void cleanup(void *__top)
{
	topology_t *top = reinterpret_cast<topology_t *>(__top);
	delete top;
}

int process(void *__ev, const char *event, const size_t esize, const char *data, const size_t dsize)
{
	topology_t *top = reinterpret_cast<topology_t *>(__ev);

	std::string ev(event, esize);
	top->run_slot(ev, data, dsize);

	return 0;
}
