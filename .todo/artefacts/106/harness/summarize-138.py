#!/usr/bin/env python3
"""Tables of todo 138 from results-138.jsonl: MiB/s per test, one value per stack, base vs
after; then the largest heap and tunnel budget use per window."""
import json
import sys
from collections import defaultdict


def main():
    rows = [r for r in map(json.loads, open(sys.argv[1])) if "in_MBps" in r]
    mibs = defaultdict(lambda: defaultdict(list))
    gauges = defaultdict(lambda: defaultdict(list))
    for r in rows:
        name = r.get('name', '')
        build, rest = name.split(' ', 1)
        kind = 'base' if build.startswith('base') else 'after'
        stack = build.split('-', 1)[1]
        test = rest.replace('budget ', '')
        test = test.split(' #')[0]
        value = r['in_MBps'] if 'download' in test else r['out_MBps']
        mibs[test][kind].append((stack, value, r.get('cpu_cores', {})))
        if 'gauges' in r:
            gauges[test][kind].append((stack, r['gauges']))
    print('| | base | after |')
    print('|---|---|---|')
    for test in mibs:
        cells = []
        for kind in ('base', 'after'):
            by_stack = defaultdict(list)
            for stack, v, _ in mibs[test][kind]:
                by_stack[stack].append(v)
            cells.append(' / '.join(' '.join(f'{v:.1f}' for v in vs) for vs in by_stack.values()))
        print(f'| {test} | {cells[0]} | {cells[1]} |')
    print()
    print('CPU cores server/client per stack:')
    for test in mibs:
        for kind in ('base', 'after'):
            print(test, kind, [(s, c.get('server'), c.get('client')) for s, _, c in mibs[test][kind]])
    print()
    for test in gauges:
        for kind in ('base', 'after'):
            print(test, kind, [(s, g) for s, g in gauges[test][kind]])


if __name__ == '__main__':
    main()
