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

	std::string event = xget_event(sph, io->chunk.data + sizeof(struct sph));
	xlog(__LOG_NOTICE, "grape-process: chunk-data: %p, chunk-size: %zd, event-size: %d, binary-size: %zd, data-size: %zd, event: %s\n",
			io->chunk.data, io->chunk.size, sph->event_size, sph->binary_size, sph->data_size,
			event.c_str());

	std::vector<std::string> strs;
	boost::split(strs, event, boost::is_any_of("@"));

	if (strs.size() != 2) {
		xlog(__LOG_ERROR, "grape-process: %s: must be app-name@event-name\n", event.c_str());
		return -EINVAL;
	}

	std::string ret;
	ret = top->run_slot(strs[1], io->chunk.data, io->chunk.size);
	if (ret.size())
		io->write(io, ret.data(), ret.size());

	return 0;
}
