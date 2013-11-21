#!/usr/bin/python
# -*- coding: utf-8 -*-

import json
from itertools import imap, ifilter
import elliptics
from nodes import connect, node_id
import prettytable

def get_app_worker_count(s, id, app):
    info = json.loads(s.exec_event(id, '%s@info' % (app), ''))
    return info['slaves']['capacity']

def app_worker_info(s, app):
    node_ids = node_id(s.get_routes())
    result = []
    for ip, id in node_ids:
        worker_count = get_app_worker_count(s, id, app)
        for worker in range(worker_count):
            result.append((ip, id, worker))
    return result

# Exec on single worker with dnet_ioclient:
#import subprocess, shlex
#cmd = './dnet_ioclient -r %s:2 -g %d -k %d -q -c queue@stats -I%s' % (
#        ip, id.group_id, worker, str(id)
#        )
#result = subprocess.check_output(shlex.split(cmd))

def exec_on_all_workers(s, event, data=None, ordered=True):
    app, command = event.split('@')

    worker_info = app_worker_info(s, app)
    if ordered:
        # retain order by worker number and order by ip addresses
        worker_info = sorted(worker_info, key=lambda x: x[0] + str(x[2]))

    asyncs = []
    for addr, direct_id, worker in worker_info:
        asyncs.append((s.exec_async(direct_id, event, src_key=worker, data=data or ''), addr, worker))

    results = []
    for async_result, addr, worker in asyncs:
        try:
	    async_result.wait()
            r = async_result.get()[0]
            #print r.address, i[1]
            results.append(r.data)
        except elliptics.Error, e:
            print 'ERROR: %s, worker %d: %s ' % (addr, worker, e)
            #results.append(None)

    return results

def get_stats(s):
    return [json.loads(i) for i in exec_on_all_workers(s, 'queue@stats')]

def select_stats(stats, names):
    return [ifilter(lambda x: x[0] in names, i.iteritems()) for i in stats]

def accumulate(*args):
    raw = [i[1] for i in args]
    return args[0][0], sum(raw), raw 

def summarize(stats):
    return stats and imap(accumulate, *stats)

def avg(*args):
    raw = [i[1] for i in args]
    return args[0][0], sum(raw) / len(args), raw

def average(stats):
    return stats and imap(avg, *stats)

def pass_raw(stats):
    def none(*args):
        raw = [i[1] for i in args]
        return args[0][0], None, raw
    return stats and imap(none, *stats)
    

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-r', '--remote', help='address of seed node')
    parser.add_argument('-g', '--group', help='elliptics group')
    args = parser.parse_args()

    session = connect([args.remote], [int(args.group)])
    stats = get_stats(session)


    ATTRS = [
        ('queue_id', pass_raw),
        ('high-id', pass_raw),
        ('low-id', pass_raw),
        ('push.count', summarize),
        ('pop.count', summarize),
        ('ack.count', summarize),
        ('timeout.count', summarize),
        ('push.rate', summarize),
        ('pop.rate', summarize),
        ('ack.rate', summarize),
        ('push.time', average),
        ('pop.time', average),
        ('ack.time', average),
    ]

    def print_attrs(stats, attrs):
        #x = prettytable.PrettyTable(['Name', 'Aggregate'] + [''] * len(stats[0][0][1]))
        #x.set_style(prettytable.PLAIN_COLUMN)
        #x.aligns = ['l', 'r', 'l']
        firsttime = True
        x = None
        for name, proc in attrs:
            for name, acc, raw in proc(select_stats(stats, [name])):
	        #print '%s\t= %r\t: %r' % (name, acc, raw)
                if firsttime:
                    x = prettytable.PrettyTable(['Name', 'Aggregate'] + ['Sub'] * len(raw))
                    x.aligns = ['l', 'r'] + ['r'] * len(raw)
                    firsttime = False
                x.add_row([name, acc] + raw)
        if x is not None:
            x.printt(border=False)

    print_attrs(stats, ATTRS)

#        for name, acc, raw in proc(select_stats(stats, ATTRS)):
#            print '%s\t= %r\t: %r' % (name, acc, raw)
#
#        for name, acc, raw in sorted(proc(select_stats(stats, ATTRS)), key=lambda x: x[0]):
#            print '%s\t= %r\t: %r' % (name, acc, raw)
#
#    print_attrs(summarize, 
#    for name, acc, raw in sorted(average(stats, ['push.time', 'pop.time', 'ack.time']):
#        print '%s\t= %r\t: %r' % (name, acc, raw)
#
#    for name, raw in select(stats, ['high-id', 'low-id', 'queue-id']):
#        print '%s\t= %r\t: %r' % (name, acc, raw)

