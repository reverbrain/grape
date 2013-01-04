#include <sys/syscall.h>

#include <stdlib.h>

#include <atomic>
#include <iostream>

#include <boost/lexical_cast.hpp>
#include <boost/program_options.hpp>
#include <boost/thread.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

#include <json/reader.h>
#include <json/writer.h>

#include <grape/grape.hpp>
#include <grape/elliptics.hpp>

namespace po = boost::program_options;
namespace pt = boost::posix_time;
using namespace ioremap;

class starter : public grape::elliptics_node_t {
	public:
		starter(Json::Value &root, const std::string &config, int thread_num, int request_num, int start_event):
		elliptics_node_t(config),
		m_limit(request_num),
		m_jconf(root),
		m_start_event(start_event)
		{
			struct timeval tv;

			gettimeofday(&tv, NULL);
			srand(tv.tv_sec + tv.tv_usec);

			for (int i = 0; i < thread_num; ++i)
				m_tgroup.create_thread(boost::bind(&starter::loop, this));

			m_tgroup.join_all();
		}

		void handle(struct sph *) {
		}

	private:
		boost::thread_group m_tgroup;
		std::atomic_int m_limit;
		std::string m_base_event;
		Json::Value m_jconf;
		int m_start_event;

		long gettid(void) {
			return syscall(SYS_gettid);
		}

		void loop() {
			elliptics::session s(*m_node);

			std::string base_event(m_jconf["event-base"].asString() + boost::lexical_cast<std::string>(m_start_event));
			Json::Value groups(m_jconf["groups"]);
			if (!groups.empty() && groups.isArray()) {
				std::vector<int> gr;
				std::transform(groups.begin(), groups.end(), std::back_inserter(gr), grape::json_digitizer());
				s.set_groups(gr);
			}

			std::string binary;
			std::string data("Test data");

			while (--m_limit >= 0) {
				std::string key = boost::lexical_cast<std::string>(rand()) + "starter";
				struct dnet_id id;
				s.transform(key, id);
				id.group_id = 0;
				std::string reply = s.exec_unlocked(&id, base_event, data, binary);
			}
		}

};

int main(int argc, char *argv[])
{
	int thread_num;
	long request_num;
	int log_level;
	int start_event;
	std::string mpath, log;

	po::options_description desc("Allowed options");

	desc.add_options()
		("help,h", "this help message")
		("thread,t", po::value<int>(&thread_num)->default_value(10), "number of threads")
		("manifest,m", po::value<std::string>(&mpath), "manifest file")
		("log,l", po::value<std::string>(&log), "log file")
		("log-level,L", po::value<int>(&log_level)->default_value(2), "log level")
		("request,r", po::value<long>(&request_num)->default_value(1000000))
		("start-event,s", po::value<int>(&start_event)->default_value(0), "starting event position (0 - event-num from manifest)")
	;

	po::variables_map vm;
	po::store(po::parse_command_line(argc, argv, desc), vm);
	po::notify(vm);

	if (vm.count("help")) {
		std::cout << desc << std::endl;
		return -1;
	}

	if (!vm.count("manifest")) {
		std::cerr << "You must provide manifest path\n" << desc << std::endl;
		return -1;
	}

	Json::Reader reader;
	Json::Value root;

	std::ifstream min(mpath.c_str());
	reader.parse(min, root);

	if (vm.count("log"))
		root["args"]["config"]["log"] = log;
	if (vm.count("log-level"))
		root["args"]["config"]["log-level"] = log_level;

	grape::logger::instance()->init(root["args"]["config"]["log"].asString(), root["args"]["config"]["log-level"].asInt(), true);

	Json::FastWriter writer;
	std::string config = writer.write(root["args"]["config"]);

	pt::ptime time_start(pt::microsec_clock::local_time());
	starter start(root["args"]["config"], config, thread_num, request_num, start_event);
	pt::ptime time_end(pt::microsec_clock::local_time());

	pt::time_duration duration(time_end - time_start);

	std::cout << "Total time: " << duration << ", rps: " << request_num * 1000000 / duration.total_microseconds() << std::endl;

	return 0;
}
