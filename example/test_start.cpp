#include <sys/syscall.h>

#include <stdlib.h>

#include <iostream>

#include <boost/detail/atomic_count.hpp>
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
		starter(Json::Value &root, const std::string &config, const std::string &start_event, int thread_num, int request_num):
		elliptics_node_t(config),
		m_limit(request_num),
		m_jconf(root),
		m_start_event(start_event)
		{
			struct timeval tv;

			gettimeofday(&tv, NULL);
			srand(tv.tv_sec + tv.tv_usec);

			for (int i = 0; i < thread_num; ++i)
				m_tgroup.create_thread(boost::bind(&starter::loop_io, this));
		}

		~starter() {
			m_tgroup.join_all();
		}

		void handle(struct sph *) {
		}

	private:
		boost::thread_group m_tgroup;
		boost::detail::atomic_count m_limit;
		std::string m_base_event;
		Json::Value m_jconf;
		std::string m_start_event;

		long gettid(void) {
			return syscall(SYS_gettid);
		}

		static std::string lexical_cast(size_t value) {
			if (value == 0) {
				return std::string("0");
			}
			std::string result;
			size_t length = 0;
			size_t calculated = value;
			while (calculated) {
				calculated /= 10;
				++length;
			}
			result.resize(length);
			while (value) {
				--length;
				result[length] = '0' + (value % 10);
				value /= 10;
			}
			return result;
		}

		void loop(void) {
			elliptics::session s(*m_node);

			Json::Value groups(m_jconf["groups"]);
			if (!groups.empty() && groups.isArray()) {
				std::vector<int> gr;
				std::transform(groups.begin(), groups.end(), std::back_inserter(gr), grape::json_digitizer());
				s.set_groups(gr);
			}

			std::string binary;
			std::string data("Test data");

			while (--m_limit >= 0) {
				std::string key = lexical_cast(rand()) + "starter";
				struct dnet_id id;
				s.transform(key, id);
				id.group_id = 0;
				std::string reply = s.exec_unlocked(&id, m_start_event, data, binary);
			}
		}

		void loop_io(void) {
			elliptics::session s(*m_node);

			Json::Value groups(m_jconf["groups"]);
			if (!groups.empty() && groups.isArray()) {
				std::vector<int> gr;
				std::transform(groups.begin(), groups.end(), std::back_inserter(gr), grape::json_digitizer());
				s.set_groups(gr);
			}

			s.set_ioflags(DNET_IO_FLAGS_CACHE | DNET_IO_FLAGS_CACHE_ONLY);

			std::string data("Test data");

			while (--m_limit >= 0) {
				std::string key = lexical_cast(rand()) + "starter";

				struct dnet_id id;
				s.transform(key, id);
				id.group_id = 0;

				try {
					s.read_data(id, 0, 0);
				} catch (...) {
				}
			}
		}
};

int main(int argc, char *argv[])
{
	int thread_num;
	long request_num;
	int log_level;
	int connection_num;
	std::string start_event;
	std::string mpath, log;

	po::options_description desc("Allowed options");

	desc.add_options()
		("help,h", "this help message")
		("thread,t", po::value<int>(&thread_num)->default_value(10), "number of threads")
		("manifest,m", po::value<std::string>(&mpath), "manifest file")
		("log,l", po::value<std::string>(&log), "log file")
		("log-level,L", po::value<int>(&log_level)->default_value(2), "log level")
		("request,r", po::value<long>(&request_num)->default_value(1000000))
		("start-event,s", po::value<std::string>(&start_event), "starting event")
		("connections,c", po::value<int>(&connection_num)->default_value(1))
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

	if (!vm.count("start-event")) {
		std::cerr << "You must provide starting event path\n" << desc << std::endl;
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
	{
		std::vector<boost::shared_ptr<starter> > proc;

		for (int i = 0; i < connection_num; ++i) {
			boost::shared_ptr<starter> start(new starter(root["args"]["config"], config,
						start_event, thread_num, request_num / connection_num));
			proc.push_back(start);
		}
	}
	pt::ptime time_end(pt::microsec_clock::local_time());

	pt::time_duration duration(time_end - time_start);

	std::cout << "Total time: " << duration << ", rps: " << request_num * 1000000 / duration.total_microseconds() << std::endl;

	return 0;
}
