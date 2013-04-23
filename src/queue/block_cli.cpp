#include <fstream>
#include <boost/program_options.hpp>

#include "block.hpp"

using namespace boost::program_options;

void read_file(std::string *content, const std::string &filename)
{
	std::stringstream stream;
	std::ifstream in(filename, std::ios::in | std::ios::binary);
	stream << in.rdbuf();
	*content = stream.str();
}

void write_file(const std::string &filename, const char *data, size_t size)
{
	std::ofstream out(filename, std::ios::out | std::ios::binary | std::ios::trunc);
	out.write(data, size);
}

int main(int argc, char** argv)
{
	options_description optdesc("Block format testing utility");
	optdesc.add_options()
		("help", "help message")
		("file", value<std::string>()->default_value("block.bin"), "file containing the block")
		("append", value<std::string>(), "data to append to the block")
		("get", value<int>(), "get item at specified index")
		;

	variables_map args;
	store(parse_command_line(argc, argv, optdesc), args);
	notify(args);

	if (args.count("help")) {
		std::cout << optdesc << "\n";
		return 1;
	}

	std::string filename(args["file"].as<std::string>());
	block_t block;

	{
		std::cout << "reading block from " << filename << "\n";
		std::string temp;
		read_file(&temp, filename);
		int n = block.assign(temp.c_str(), temp.size());
		if (n < 0) {
			std::cout << "bad format " << n << "\n";
			return 2;
		}
		std::cout << "block: " << block._size << " bytes, " << block._item_count << " items" << "\n";
	}

	if(args.count("get")) {
		int n = args["get"].as<int>();
		const block_t::item * item = block.at(n);
		if (item) {
			std::cout << "item #" << n << ": " << std::string(item->data, item->size) << "\n";
		} else {
			std::cout << "index " << n << " is out of range" << "\n";
		}
	}

	if(args.count("append")) {
		std::string data = args["append"].as<std::string>();
		std::cout << "appending " << data << "\n";
		block.append(data.c_str(), data.size());
		std::cout << "block: " << block._size << " bytes, " << block._item_count << " items" << "\n";
	
		std::cout << "writing block to " << filename << "\n";
		write_file(filename, block._bytes, block._size);
	}

	return 0;
}