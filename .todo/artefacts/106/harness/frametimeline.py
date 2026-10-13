#!/usr/bin/env python3
"""Compressed frame timeline of a grpc-netty frame log between two timestamps: runs of
DATA frames in one direction collapse to one line with their byte total.
Usage: tl.py LOG FROM TO   (timestamps like 06:31:03.000)"""
import re
import sys

LINE = re.compile(r'^\S+T(\S+)\+\S+ DEBUG .*\] (INBOUND|OUTBOUND) (\w+): (.*)$')


def main():
    path, lo, hi = sys.argv[1], sys.argv[2], sys.argv[3]
    run = None
    for line in open(path, errors='replace'):
        m = LINE.match(line)
        if not m:
            continue
        t, d, kind, rest = m.groups()
        if t < lo:
            continue
        if t > hi:
            break
        if kind == 'DATA':
            mm = re.search(r'streamId=(\d+) padding=\d+ endStream=\w+ length=(\d+)', rest)
            n = int(mm.group(2))
            if run and run[1] == d:
                run[2] += n
                run[3] += 1
                run[4] = t
            else:
                if run:
                    print(f'{run[0]}-{run[4]} {run[1]} DATA {run[3]} frames {run[2]} bytes')
                run = [t, d, n, 1, t]
            continue
        if run:
            print(f'{run[0]}-{run[4]} {run[1]} DATA {run[3]} frames {run[2]} bytes')
            run = None
        rest = re.sub(r' bytes=[0-9a-f]{20,}', '', rest)
        print(t, d, kind, rest[:100])
    if run:
        print(f'{run[0]}-{run[4]} {run[1]} DATA {run[3]} frames {run[2]} bytes')


if __name__ == '__main__':
    main()
