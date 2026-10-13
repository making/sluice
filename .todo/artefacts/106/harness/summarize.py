#!/usr/bin/env python3
"""Markdown tables from results.jsonl. Usage: summarize.py results.jsonl"""
import json
import sys

rows = [json.loads(line) for line in open(sys.argv[1])]


def g(d, *path, default=''):
    for p in path:
        if not isinstance(d, dict) or p not in d:
            return default
        d = d[p]
    return d


def table(title, prefix_filter, cols):
    sel = [r for r in rows if prefix_filter(r)]
    if not sel:
        return
    print(f'\n### {title}\n')
    print('| ' + ' | '.join(c[0] for c in cols) + ' |')
    print('|' + '---|' * len(cols))
    for r in sel:
        print('| ' + ' | '.join(str(c[1](r)) for c in cols) + ' |')


load_cols = [
    ('scenario', lambda r: r['name']),
    ('req/s', lambda r: r['rps']),
    ('MiB/s', lambda r: round(r['in_MBps'] + r['out_MBps'], 1)),
    ('p50 ms', lambda r: r['p50_ms']),
    ('p99 ms', lambda r: r['p99_ms']),
    ('max ms', lambda r: r['max_ms']),
    ('err', lambda r: r['errors']),
    ('server cores', lambda r: g(r, 'cpu_cores', 'server')),
    ('client cores', lambda r: g(r, 'cpu_cores', 'client')),
    ('server sys', lambda r: g(r, 'sys_share', 'server')),
    ('server ms/MiB', lambda r: g(r, 'cpu_ms_per_MiB', 'server')),
    ('server us/req', lambda r: round(g(r, 'cpu_ms_per_req', 'server', default=0) * 1000, 1)),
    ('server alloc B/B', lambda r: g(r, 'alloc_per_byte', 'server')),
    ('server alloc KiB/req', lambda r: g(r, 'alloc_KiB_per_req', 'server')),
    ('h2demux counters', lambda r: ' '.join(f'{k}={int(v)}' for k, v in g(r, 'h2demux_counters', default={}).items())),
]


def has(tag, *words):
    return lambda r: r.get('name', '').startswith(tag) and all(w in r['name'] for w in words) and 'rps' in r


table('Direct (harness ceiling, no sluice)', has('base direct'), load_cols)
table('Throughput, loopback', lambda r: r.get('name', '').startswith('base ') and ('download' in r['name'] or 'upload' in r['name']) and 'direct' not in r['name'], load_cols)
table('Small requests, loopback', lambda r: r.get('name', '').startswith('base small'), load_cols)
table('Tunnel RTT 20ms (10ms one-way between sluice-client and sluice-server)', has('wan10'), load_cols)
table('Client RTT 20ms (10ms one-way between load client and data port)', has('cwan10'), load_cols)
table('Window experiment', lambda r: r.get('name', '').startswith(('winbase', 'win1m')), load_cols)

lock = [r for r in rows if 'loop_lock' in r]
if lock:
    print('\n### Loop lock (instrumented build)\n')
    print('| scenario | req/s | MiB/s | acquired/s | contended | wait ms/s | held ms/s | avg wait us | avg hold us |')
    print('|---|---|---|---|---|---|---|---|---|')
    for r in lock:
        ll = r['loop_lock']
        print(f"| {r['name']} | {r['rps']} | {round(r['in_MBps'] + r['out_MBps'], 1)} | {ll['acquired_per_s']} | "
              f"{ll['contended_share'] * 100:.1f}% | {ll['wait_ms_per_s']} | {ll['hold_ms_per_s']} | "
              f"{ll['avg_wait_us']} | {ll['avg_hold_us']} |")

hold = [r for r in rows if r.get('name', '').startswith('hold ')]
if hold:
    print('\n### Held connections and streams (live heap after full GC, threads)\n')
    print('| scenario | server KiB/conn | server KiB/stream | server threads/conn | client KiB/conn | client threads/conn | server threads added |')
    print('|---|---|---|---|---|---|---|')
    for r in hold:
        s, c = r['server'], r['client']
        kinds = ', '.join(f'{k} {v}' for k, v in s['kinds_delta'].items() if 'ForkJoin' not in k and 'grpc' not in k)
        print(f"| {r['name']} | {s['heap_kb_per_conn']} | {s['heap_kb_per_stream']} | {s['threads_per_conn']} | "
              f"{c['heap_kb_per_conn']} | {c['threads_per_conn']} | {kinds} |")

hp = [r for r in rows if r.get('name', '').startswith('base small h2') and 'direct' not in r['name']]
if hp:
    print('\n### HPACK: header bytes per request (frame header included)\n')
    print('| scenario | request HEADERS client->relay | relay->upstream (tunnel) | response HEADERS upstream->relay (tunnel) | relay->client |')
    print('|---|---|---|---|---|')
    for r in hp:
        t, u = r['tally'], r['upstream_tally']['h2']
        n = t['in']['DATA'] / 11.0
        print(f"| {r['name']} | {t['out']['HEADERS'] / n:.1f} | {u['in']['HEADERS'] / n:.1f} | "
              f"{u['out']['HEADERS'] / n:.1f} | {t['in']['HEADERS'] / n:.1f} |")
