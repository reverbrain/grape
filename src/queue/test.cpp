#include "queue.hpp"

#include <iostream>

int main(int argc, char *argv[])
{
	ioremap::grape::queue q("queue.conf", "test-queue-id", 4);

	for (int i = 0; i < 10; ++i) {
		std::string data = "this is a test: " + ioremap::grape::lexical_cast(i);
		std::cout << data << std::endl;

		q.push(data);
	}

	for (int i = 0; i < 10; ++i) {
		ioremap::elliptics::data_pointer d = q.pop();
		std::cout << i << ": " << (d.empty() ? "empty" : d.to_string()) << std::endl;
	}

	std::string end = "at the end (test push/pop in the same chunk)";
	q.push(end);
	std::cout << end << " : " << q.pop().to_string() << std::endl;
}
