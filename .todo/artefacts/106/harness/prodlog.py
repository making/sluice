"""Summarizes sluice-server ECS log lines (kubectl logs output): the access log
conn/req lines and every WARN/ERROR, plus counts. Usage: prodlog.py file [--all]"""
import collections
import json
import re
import sys

lines = []
for raw in open(sys.argv[1]):
    try:
        lines.append(json.loads(raw))
    except Exception:
        lines.append({'@timestamp': '?', 'log': {'level': 'RAW', 'logger': '?'}, 'message': raw.strip()})

show_all = '--all' in sys.argv
levels = collections.Counter()
conn_close = []
for d in lines:
    lvl = d['log']['level']
    levels[lvl] += 1
    msg = d.get('message', '')
    logger = d['log']['logger']
    if show_all or lvl in ('WARN', 'ERROR', 'RAW') or 'type=conn' in msg and 'event=close' in msg:
        print(d['@timestamp'][:23], lvl, logger.split('.')[-1], msg[:400])
    if 'type=conn' in msg and 'event=close' in msg:
        conn_close.append(msg)
print('levels', dict(levels))
transports = collections.Counter(re.search(r'transport=(\S+)', m).group(1) for m in conn_close
                                 if re.search(r'transport=(\S+)', m))
print('conn closes', len(conn_close), dict(transports))
reasons = collections.Counter(re.search(r'reason=(\S+)', m).group(1) for m in conn_close
                              if re.search(r'reason=(\S+)', m))
print('close reasons', dict(reasons))
