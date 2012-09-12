#include <cocaine/binary.hpp>
#include <grape/grape.hpp>

using namespace ioremap::grape;

extern "C" {
	void cleanup(void *__top);
	int process(void *__ev, struct binary_io *io);
}

void cleanup(void *__top)
{
	topology_t *top = reinterpret_cast<topology_t *>(__top);
	delete top;
}

int process(void *__ev, struct binary_io *io)
{
	topology_t *top = reinterpret_cast<topology_t *>(__ev);

	if (io->chunk.size < sizeof(struct sph)) {
		xlog(__LOG_ERROR, "grape-process: chunk size (%zd) is less than sph header size (%zd)\n",
				io->chunk.size, sizeof(struct sph));
		return -E2BIG;
	}

	struct sph *sph = (struct sph *)io->chunk.data;

	if (io->chunk.size != sizeof(struct sph) + sph->event_size + sph->data_size + sph->binary_size) {
		xlog(__LOG_ERROR, "grape-process: invalid chunk size: %zd, must be equal to sum of "
				"sizeof(sph): %zd, event-size: %d, binary-size: %zd, data-size: %zd",
				io->chunk.size, sizeof(struct sph), sph->event_size, sph->data_size, sph->binary_size);
		return -E2BIG;
	}

	top->run_slot(sph);
	return 0;
}
