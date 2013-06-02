#include "queue.hpp"

#include <iostream>

int main(int argc, char *argv[])
{
	ioremap::grape::queue q("queue.conf", "test-queue-id");

	for (int i = 0; i < 10; ++i) {
		std::string data = "this is a test: " + lexical_cast(i);
		std::cout << "<< " << data << std::endl;

		q.push(data);
	}

	for (int i = 0; i < 10; ++i) {
		ioremap::grape::data_array d = q.pop(3);

		if (d.empty())
			break;

		size_t pos = 0;
		for (auto sz : d.sizes()) {
			std::cout << ">> " << d.data().substr(pos, sz) << std::endl;
			pos += sz;
		}

		ioremap::grape::data_array copy = ioremap::grape::data_array::deserialize(d.serialize());
		pos = 0;
		for (auto sz : copy.sizes()) {
			std::cout << "copy >> " << d.data().substr(pos, sz) << std::endl;
			pos += sz;
		}
	}

	std::string end = "at the end (test push/pop in the same chunk)";
	q.push(end);

	std::cout << "going to pop final message" << std::endl;
	std::cout << end << " : " << q.pop(10).data() << std::endl;
}
