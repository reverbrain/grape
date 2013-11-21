#!/usr/bin/python
# -*- coding: utf-8 -*-

from nodes import connect
from _queue_stats import exec_on_all_workers

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-r', '--remote', help='address of seed node')
    parser.add_argument('-g', '--group', help='elliptics group')
    args = parser.parse_args()

    s = connect([args.remote], [int(args.group)])
    exec_on_all_workers(s, 'queue@clear')
