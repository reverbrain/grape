#!/usr/bin/python
# -*- coding: utf-8 -*-

import json
from itertools import imap, ifilter
import elliptics
from nodes import connect, node_id
import prettytable

def get_app_worker_count(s, key, app):
    result = s.exec_(key, '%s@info' % (app), '').get()[0]
    info = json.loads(str(result.context.data))
    return info['slaves']['capacity']

#NOTE: manually sends separate commands to each node
def app_worker_info(s, app):
    result = []
    for addr, key in s.routes.addresses_with_id():
        worker_count = get_app_worker_count(s, key, app)
        for worker in range(worker_count):
            result.append((addr, key, worker))
    return result

#NOTE: fanout sending using key=None
#FIXME: still does not work: context.src_id is empty, address is broken
#def app_worker_info(s, app):
#    result = []
#    for i in s.exec_(None, '%s@info' % (app)).get():
#        c = i.context
#        print i.address, c.src_id, c.src_key
#        info = json.loads(i.context.data)
#        worker_count = info['slaves']['capacity']
#        for worker in range(worker_count):
#            result.append((i.context.address, i.context.src_id, worker))
#    return result

def exec_on_all_workers(s, event, data=None, ordered=True):
    app, command = event.split('@')

    worker_info = app_worker_info(s, app)
    if ordered:
        # retain existing order by worker number and add order by ip addresses
        worker_info = sorted(worker_info, key=lambda x: str(x[0]) + str(x[2]))

    asyncs = []
    for addr, direct_id, worker in worker_info:
        asyncs.append((s.exec_(direct_id, src_key=worker, event=event, data=data or ''), addr, worker))

    results = []
    for async_result, addr, worker in asyncs:
        try:
            async_result.wait()
            rlist = async_result.get()
            if len(rlist) > 0:
                r = rlist[0]
                #print r.address, r.context.data
                results.append(r.context.data)
            else:
                print 'ERROR: %s, worker %d: no data returned' % (addr, worker)
        except elliptics.Error, e:
            print 'ERROR: %s, worker %d: %s' % (addr, worker, e)
            #results.append(None)

    return results

def get_stats(s):
    return [json.loads(str(i)) for i in exec_on_all_workers(s, 'queue@stats')]

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
    parser.add_argument('-g', '--group', type=int, help='elliptics group')
    parser.add_argument('--loglevel', type=int, choices=xrange(5), default=1)
    args = parser.parse_args()

    session = connect([args.remote], [args.group], loglevel=args.loglevel)
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

