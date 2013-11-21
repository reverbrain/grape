#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
sys.path.append('bindings/python/')
import elliptics

def connect(endpoints, groups):
	log = elliptics.Logger("/dev/stderr", 1)
	config = elliptics.Config()
	config.config.wait_timeout = 60
	n = elliptics.Node(log, config)

	remotes = []
	for r in endpoints:
		parts = r.split(":")
		remotes.append((parts[0], int(parts[1])))

	for r in remotes:
		try:
			n.add_remote(r[0], r[1])
		except Exception as e:
			pass

	s = elliptics.Session(n)
	s.add_groups(groups)
	return s

def node_id_map(routes):
	return dict([(i[1], i[0]) for i in routes])	
 
def nodes(routes):
	return sorted(set([i[1] for i in routes]))

def ids(routes):
	return node_id_map(routes).values()

def node_id(routes):
	return sorted(node_id_map(routes).items(), key=lambda x: x[1].id)

if __name__ == '__main__':
	import argparse
	parser = argparse.ArgumentParser()
	parser.add_argument('remote', help="address of seed node")
	parser.add_argument('what', choices=['ip', 'id', 'all'])
	args = parser.parse_args()

	remote = args.remote
	#if len(sys.argv) > 1:
	#	remote = sys.argv[1:]

	session = connect([remote], [1])

	routing_table = session.get_routes()
	#for node_id,node_ip in routing_table:
	#	print node_ip, ''.join(['{0:02x}'.format(k) for k in node_id.id])

	if args.what == 'ip':
		for i in nodes(routing_table):
			print i
	elif args.what == 'id':
		for i in ids(routing_table):
			print ''.join(['{0:02x}'.format(k) for k in i.id])
	elif args.what == 'all':
		for node_ip,node_id in node_id(routing_table):
			print node_ip, ''.join(['{0:02x}'.format(k) for k in node_id.id])

