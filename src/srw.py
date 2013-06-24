from cocaine.services import Service
import struct

event = 'queue@pop'
data = ''

data_size = len(data)
event_size = len(event)

flags = 1
status = 0
__key = 0
src_key = 3

packed = struct.pack('<64sQQiiii32s%ds%ds' % (event_size, data_size),
		"this is id",
		data_size,
		flags,
		event_size,
		status,
		__key,
		src_key,
		"this is address",
		event,
		data)

response = Service('queue', hostname = 'localhost', port = 10053).enqueue('pop', packed, 'localhost-queue-1')
print response.get()
