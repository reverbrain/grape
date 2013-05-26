#include "grape/elliptics_client_state.hpp"

#include <iostream>
#include <vector>

#include <unistd.h>

using namespace ioremap;

static void starter_usage(const char *name)
{
	std::cerr << "Usage (all parameters are mandatory): " << name << std::endl <<
		" -r addr:port:family  - remote node to configure queue on.\n" <<
		" -n num               - number of workers to start (configure). This number must match queue profile.\n"
		" -q id                - queue will run on given node with given ID. Consider this as per node extension of 'queue' name.\n"
		" -h                   - this help.\n" <<
		std::endl;

	exit(-1);
}

int main(int argc, char *argv[])
{
	int ch, err;
	char *remote_addr = NULL;
	int port, family;
	struct dnet_config cfg;
	int num_workers = -1;
	char *queue_name = NULL;

	memset(&cfg, 0, sizeof(struct dnet_config));
	cfg.wait_timeout = 60;

	while ((ch = getopt(argc, argv, "g:r:n:q:h")) != -1) {
		switch (ch) {
		case 'r':
			err = dnet_parse_addr(optarg, &port, &family);
			if (err)
				return err;
			remote_addr = optarg;
			break;
		case 'n':
			num_workers = atoi(optarg);
			break;
		case 'q':
			queue_name = optarg;
			break;
		case 'h':
		default:
			starter_usage(argv[0]);
			/* never reached */
		}
	}

	if (num_workers == -1 || remote_addr == NULL || queue_name == NULL) {
		starter_usage(argv[0]);
	}

	elliptics::file_logger log("/dev/stdout", DNET_LOG_ERROR);
	elliptics::node n(log, cfg);

	elliptics::session s(n);

	err = dnet_add_state(n.get_native(), remote_addr, port, family, DNET_CFG_NO_ROUTE_LIST);
	if (err)
		return err;

	std::vector<std::pair<struct dnet_id, struct dnet_addr> > routes = s.get_routes();
	if (routes.size() == 0) {
		std::cerr << "route table is empty, exiting" << std::endl;
		return -1;
	}

	std::vector<int> groups;
	groups.push_back(routes[0].first.group_id);
	s.set_groups(groups);
	s.set_cflags(DNET_FLAGS_DIRECT);

	std::string event, data;

	event = "queue@start-multiple-task";
	s.exec(NULL, event, data).wait();

	for (int i = 0; i < num_workers; ++i) {
		struct dnet_id id;

		memset(&id, 0, sizeof(struct dnet_id));
		s.set_filter(elliptics::filters::all_with_ack);

		event = "queue@configure";
		data = queue_name;

		auto result = s.exec(&id, i, event, data);
		for (auto it = result.begin(); it != result.end(); ++it) {
			if (it->error()) {
				elliptics::error_info error = it->error();
				std::cout << dnet_server_convert_dnet_addr(it->address())
					<< ": failed to process: \"" << error.message() << "\": " << error.code() << std::endl;
			} else {
				elliptics::exec_context context = it->context();
				if (context.is_null()) {
					std::cout << dnet_server_convert_dnet_addr(it->address())
						<< ": acknowledge" << std::endl;
				} else {
					std::cout << dnet_server_convert_dnet_addr(context.address())
						<< ": " << context.event()
						<< " \"" << context.data().to_string() << "\"" << std::endl;
				}
			}
		}
	}

	return 0;
}
