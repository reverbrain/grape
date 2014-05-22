#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
sys.path.append('bindings/python/')
import elliptics

class PassthroughWrapper(object):
    ''' Wrapper to assure session/node destroy sequence: session first, node last '''
    def __init__(self, node, session):
        self.node = node
        self.session = session

    def __getattr__(self, name):
        return getattr(self.session, name)

    def __del__(self):
        del self.session
        del self.node

def connect(endpoints, groups, **kw):
	remotes = []
	for r in endpoints:
		parts = r.split(":")
		remotes.append((parts[0], int(parts[1])))

	def rename(new, old):
		if old in kw:
			kw[new] = kw.pop(old)

	kw.pop('elog', None)
	kw.pop('cfg', None)
	kw.pop('remotes', None)
	rename('log_file', 'logfile')
	rename('log_level', 'loglevel')

	n = elliptics.create_node(**kw)

#	def create_node(**kw):
#		log = elliptics.Logger(kw.get('logfile', '/dev/stderr'), kw.get('loglevel', 1))
#		config = elliptics.Config()
#		config.config.wait_timeout = kw.get('wait-timeout', 60)
#		return elliptics.Node(log, config)
#	n = create_node(**kw)

	for r in remotes:
		try:
			n.add_remote(r[0], r[1])
		except Exception as e:
			pass

	s = elliptics.Session(n)
	s.add_groups(groups)
	#XXX: Is it time to drop PassthroughWrapper binder?
	return PassthroughWrapper(n, s)

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
	parser.add_argument('remote', help='address of seed node')
	parser.add_argument('what', choices=['ip', 'id', 'all'], help='what information to print')
	parser.add_argument('-g', '--group', type=int, help='elliptics group')
	parser.add_argument('--loglevel', type=int, choices=xrange(5), default=1)
	parser.set_defaults(group=1)
	args = parser.parse_args()

	session = connect([args.remote], [args.group],
			loglevel=args.loglevel,
			io_thread_num=3,
			)

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

