#!/usr/bin/env python3
"""Summarise a grpc-netty frame log (Http2FrameLogger at DEBUG): SETTINGS, PINGs,
WINDOW_UPDATEs and DATA per 100ms bucket. Usage: frames.py LOG [from-ts to-ts]"""
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime

LINE = re.compile(r'^(\S+) DEBUG .*\] (INBOUND|OUTBOUND) (\w+): (.*)$')


def ts(s):
    return datetime.fromisoformat(s).timestamp()


def main():
    path = sys.argv[1]
    lo = ts(sys.argv[2]) if len(sys.argv) > 2 else 0
    hi = ts(sys.argv[3]) if len(sys.argv) > 3 else 1e12
    settings = []
    pings = Counter()
    wu = Counter()
    data = defaultdict(int)
    dsizes = Counter()
    for line in open(path, errors='replace'):
        m = LINE.match(line)
        if not m:
            continue
        t = ts(m.group(1))
        if t < lo or t > hi:
            continue
        d, kind, rest = m.group(2), m.group(3), m.group(4)
        if kind == 'SETTINGS':
            settings.append((m.group(1), d, rest))
        elif kind == 'PING':
            pings[(d, rest.split()[0])] += 1
        elif kind == 'WINDOW_UPDATE':
            mm = re.search(r'streamId=(\d+) windowSizeIncrement=(\d+)', rest)
            wu[(d, mm.group(1) == '0', int(mm.group(2)))] += 1
        elif kind == 'DATA':
            mm = re.search(r'padding=\d+ endStream=\w+ length=(\d+)', rest)
            if mm:
                n = int(mm.group(1))
                data[(d, int(t * 10))] += n
                dsizes[(d, n)] += 1
    for s in settings:
        print('SETTINGS', *s)
    print('PINGS', dict(pings))
    for k, v in sorted(wu.items(), key=lambda kv: -kv[1])[:10]:
        print('WINDOW_UPDATE', 'dir', k[0], 'conn' if k[1] else 'stream', 'inc', k[2], 'x', v)
    for k, v in sorted(dsizes.items(), key=lambda kv: -kv[1])[:6]:
        print('DATA size', k, 'x', v)
    if len(sys.argv) > 4:
        for (d, b), n in sorted(data.items()):
            print('DATA100ms', d, datetime.fromtimestamp(b / 10).strftime('%H:%M:%S.%f')[:-5], n)


if __name__ == '__main__':
    main()
