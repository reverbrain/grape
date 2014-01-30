#!/usr/bin/python

import json

def get_value(node, path):
    for i in path.split('.'):
        node = node[i]
    return node
        

if __name__ == '__main__':
    import sys
    path = sys.argv[1]
    doc = json.load(sys.stdin)

    print get_value(doc, path)

