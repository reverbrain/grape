#!/usr/bin/python
# -*- coding: utf-8 -*-

import socket
from nodes import connect
from bisect import bisect_right

def print_routing_table(table):
	for node_id, node_ip in table:
		print node_ip, node_id

if __name__ == '__main__':
	import argparse
	parser = argparse.ArgumentParser()
	parser.add_argument('remote', help='address of seed node')
	parser.add_argument('id', help='hexadecimal id (or prefix of) to lookup responsible host for')
	parser.add_argument('--verbose', '-v', action='count')
	args = parser.parse_args()

	remote = args.remote
	# supplement id to even number of chars
	# (as hex form must represent whole number bytes)
	if len(args.id) % 2 != 0:
		args.id += '0'
	try:
		eid = map(ord, args.id.decode("hex"))
	except TypeError, e:
		print 'Invalid id:', e
		exit(1)

	session = connect([remote], [1])

	routing_table = session.get_routes()
	sorted_ids = sorted(routing_table, key=lambda x: x[0].id)

	if args.verbose > 1:
		print 'Routing table:'
		print_routing_table(sorted_ids)
		print

	def find_responsible(a, x):
		'''Find responsible routing item in the circular routing table.
		Find righmost value less then or equal to x.
		And last entry is responsible for [0, a[0]) span.
		'''
		keys = [i[0].id for i in a]
		i = bisect_right(keys, x)
		return a[i-1]

	id, addr = find_responsible(sorted_ids, eid)

	if args.verbose > 0:
		print 'Responsible entry:'
		print addr, id
		print
		print 'Answer:'

	print '%s:%s' % (socket.gethostbyaddr(addr.host)[0], addr.port)

