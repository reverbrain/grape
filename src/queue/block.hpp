#ifndef BLOCK_HPP__
#define BLOCK_HPP__

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include <stdexcept>

// block_t provides lenval format mapping onto raw memory
struct block_t
{
	struct item
	{
		uint32_t size;
		char data[0];
	} __attribute__((packed));

	char *_bytes;
	size_t _size;
	int _item_count;
	bool _owner;

	block_t()
		: _bytes(NULL)
		, _size(0)
		, _item_count(0)
		, _owner(true)
	{}

	~block_t() {
		if (_owner) {
			free(_bytes);
		}
	}

	// set block to look into someone else's memory
	int set(char *data, size_t size) {
		int count = _count_items(data, size);
		if (count < 0) {
			return count;
		}
		_owner = false;
		_bytes = data;
		_size = size;
		_item_count = count;
		return _item_count;
	}

	// set block to take a copy of the data
	int assign(const char *data, size_t size) {
		int count = _count_items(data, size);
		if (count < 0) {
			return count;
		}
		_owner = true;
		_bytes = (char *)malloc(size);
		memcpy(_bytes, data, size);
		_size = size;
		_item_count = count;
		return _item_count;
	}

	int _count_items(const char *data, size_t size) {
		const char *p = data;
		const char *end = p + size;
		int count = 0;
		while(p < end) {
			const item *rec = reinterpret_cast<const item*>(p);
			p += sizeof(item) + rec->size;
			++count;
		}
		if (p > end) {
			// error in data format: too big len somewhere
			return -count;
		}
		return count;
	}

	const item *at(int index) {
		if (index < 0 || index >= _item_count) {
			return NULL;
		}
		// no boundary or format checks
		const char *p = _bytes;
		const item *rec = reinterpret_cast<const item*>(p);
		for (int i = 0; i < index; ++i) {
			p += sizeof(item) + rec->size;
			rec = reinterpret_cast<const item*>(p);
		}
		return rec;
	}

	// append could be called on empty block_t
	void append(const char *from, size_t size) {
		if (!_owner) {
			throw std::runtime_error("Cannot modify someone else's memory, use assign() instead of set()");
		}
		size_t prev_size = _size;
		_size += sizeof(item) + size;
		_bytes = (char *)realloc(_bytes, _size);
		item *rec = reinterpret_cast<item*>(_bytes + prev_size);
		rec->size = uint32_t(size);
		memcpy(rec->data, from, size);
		++_item_count;
	}
};

#endif //BLOCK_HPP_
