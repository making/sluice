#!/usr/bin/env python3
"""Load harness orchestrator: upstream + sluice-server + sluice-client (+ delay proxy) as
separate JVMs, load client as another, CPU sampled with ps around the measured window.

Usage: bench.py <repo> <suite> [--delay MS] [--jfr SETTINGS] [--tag TAG]
"""
import argparse
import ctypes
import json
import os
import re
import signal
import subprocess
import sys
import threading
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, 'out')
CP = os.path.join(HERE, 'classes') + ':' + os.path.join(HERE, 'lib') + '/*'

DATA, GRPC, MGMT, PROXY, CMGMT = 18000, 18001, 18081, 18002, 19001
UP_H1, UP_H2, UP_CTL = 31081, 31082, 31083

procs = {}
JFC = ['contention']
PATCHED = [False]
WINDOW_PATCH = [False]


def log(*a):
    print(time.strftime('%H:%M:%S'), *a, flush=True)


def start(name, cmd, logfile):
    f = open(os.path.join(OUT, logfile), 'w')
    p = subprocess.Popen(cmd, stdout=f, stderr=subprocess.STDOUT, preexec_fn=os.setsid)
    procs[name] = p
    return p


class _RUsage(ctypes.Structure):
    _fields_ = [('ri_uuid', ctypes.c_uint8 * 16), ('ri_user_time', ctypes.c_uint64),
                ('ri_system_time', ctypes.c_uint64), ('rest', ctypes.c_uint64 * 8)]


class _Timebase(ctypes.Structure):
    _fields_ = [('numer', ctypes.c_uint32), ('denom', ctypes.c_uint32)]


_libproc = ctypes.CDLL('/usr/lib/libproc.dylib')
_tb = _Timebase()
ctypes.CDLL('/usr/lib/libSystem.dylib').mach_timebase_info(ctypes.byref(_tb))


def cpu_seconds(pid):
    """User + system CPU of the whole task, terminated threads included (macOS ps
    sums only the live threads, so its figure drops when a JVM thread exits)."""
    r = _RUsage()
    if _libproc.proc_pid_rusage(pid, 0, ctypes.byref(r)) != 0:
        return 0.0
    return (r.ri_user_time + r.ri_system_time) * _tb.numer / _tb.denom / 1e9


def sys_seconds(pid):
    r = _RUsage()
    if _libproc.proc_pid_rusage(pid, 0, ctypes.byref(r)) != 0:
        return 0.0
    return r.ri_system_time * _tb.numer / _tb.denom / 1e9


def rss_kb(pid):
    out = subprocess.run(['ps', '-o', 'rss=', '-p', str(pid)], capture_output=True, text=True).stdout.strip()
    return int(out or 0)


def http(url, headers=None, timeout=2):
    req = urllib.request.Request(url, headers=headers or {})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode()


def wait_ready(timeout=90):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if http(f'http://127.0.0.1:{DATA}/small', {'Host': 'h1.local'}) == 'ok':
                return
        except Exception:
            pass
        time.sleep(0.5)
    raise RuntimeError('stack not ready')


def start_stack(repo, delay=0, jfr=None, server_opts=(), server_args=(), client_opts=(), upstream_window=65535,
                tag='', client_args=()):
    os.makedirs(OUT, exist_ok=True)
    ports_free()
    start('upstream', ['java', '-Xmx1g', '-cp', CP, 'Upstream', str(UP_H1), str(UP_H2), str(UP_CTL),
                       str(upstream_window)], f'upstream{tag}.log')
    grpc_port = GRPC
    if delay > 0:
        start('proxy', ['java', '-cp', CP, 'DelayProxy', str(PROXY), '127.0.0.1', str(GRPC), str(delay)],
              f'proxy{tag}.log')
        grpc_port = PROXY
    sopts = ['-Xmx1g']
    if jfr:
        sopts.append(f'-XX:StartFlightRecording=filename={OUT}/server{tag}.jfr,settings={jfr},dumponexit=true')
    sopts += list(server_opts)
    if PATCHED[0]:
        u = f'{HERE}/patched/unpacked/BOOT-INF'
        extra_cp = f'{HERE}/patched/classes2:' if WINDOW_PATCH[0] else ''
        launch = ['-cp', f'{extra_cp}{HERE}/patched/classes:{u}/classes:{u}/lib/*', f'-Dloop.stats={OUT}/loop-stats.log',
                  'am.ik.sluice.server.SluiceServerApplication']
    else:
        launch = ['-jar', f'{repo}/sluice-server/target/sluice-server-0.0.1-SNAPSHOT-exec.jar']
    start('server', ['java', *sopts, *launch,
                     '--sluice.token=SECRET', f'--sluice.data-port={DATA}', f'--spring.grpc.server.port={GRPC}',
                     f'--server.port={MGMT}', '--sluice.limits.max-connections=4000', *server_args],
          f'server{tag}.log')
    time.sleep(4)
    start('client', ['java', '-Xmx1g', *client_opts, '-jar',
                     f'{repo}/sluice-client/target/sluice-client-0.0.1-SNAPSHOT-exec.jar',
                     f'--sluice.server-url=grpc://127.0.0.1:{grpc_port}', f'--server.port={CMGMT}',
                     '--sluice.token=SECRET', '--sluice.client.max-connections=4000',
                     '--sluice.client.upstream[0].host=h2.local',
                     f'--sluice.client.upstream[0].target=http://127.0.0.1:{UP_H2}',
                     '--sluice.client.upstream[1].host=h1.local',
                     f'--sluice.client.upstream[1].target=http://127.0.0.1:{UP_H1}', *client_args], f'client{tag}.log')
    wait_ready()
    log('stack ready', 'delay', delay)


def stop_stack():
    for name in ['client', 'server', 'proxy', 'upstream']:
        p = procs.pop(name, None)
        if p is None:
            continue
        try:
            os.killpg(p.pid, signal.SIGTERM)
            p.wait(timeout=20)
        except Exception as e:
            log('stop', name, 'did not end on SIGTERM:', repr(e))
            try:
                os.killpg(p.pid, signal.SIGKILL)
            except Exception:
                pass
            p.kill()
            p.wait(timeout=10)
    time.sleep(1)


def ports_free():
    import socket
    busy = []
    for port in (DATA, GRPC, MGMT, PROXY, CMGMT, UP_H1, UP_H2, UP_CTL, 18003):
        s = socket.socket()
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.bind(('0.0.0.0', port))
        except OSError:
            busy.append(port)
        finally:
            s.close()
    if busy:
        raise RuntimeError(f'ports in use: {busy}')


def tally_reset():
    http(f'http://127.0.0.1:{UP_CTL}/reset')


def tally():
    return json.loads(http(f'http://127.0.0.1:{UP_CTL}/tally'))


def prom(port=MGMT, pattern=r'^(sluice_|jvm_threads_live|jvm_memory_used_bytes\{.*heap)'):
    text = http(f'http://127.0.0.1:{port}/actuator/prometheus', timeout=5)
    keep = {}
    for line in text.splitlines():
        if line.startswith('#'):
            continue
        if re.search(pattern, line):
            k, _, v = line.rpartition(' ')
            keep[k] = float(v)
    return keep


def alloc_bytes():
    out = {}
    for name, port in (('server', MGMT), ('client', CMGMT)):
        try:
            text = http(f'http://127.0.0.1:{port}/actuator/prometheus', timeout=5)
            m = re.search(r'^jvm_gc_memory_allocated_bytes_total\S* (\S+)$', text, re.M)
            out[name] = float(m.group(1)) if m else 0.0
        except Exception:
            out[name] = 0.0
    return out


def loop_stats():
    with open(os.path.join(OUT, 'loop-stats.log')) as f:
        last = f.readlines()[-1].split()
    return [int(x) for x in last]


def thread_dump(pid):
    """(name, stack frames) of every thread, virtual threads included."""
    path = os.path.join(OUT, f'dump-{pid}.json')
    if os.path.exists(path):
        os.remove(path)
    subprocess.run(['jcmd', str(pid), 'Thread.dump_to_file', '-format=json', path], capture_output=True)
    try:
        with open(path) as f:
            d = json.load(f)
    except Exception:
        return []
    out = []
    for c in d['threadDump']['threadContainers']:
        for t in c.get('threads', []):
            out.append((t.get('name', ''), t.get('stack', [])))
    return out


def classify(stack):
    """What a thread is doing, from its stack (top first)."""
    text = '\n'.join(stack[:40])
    if 'ReentrantLock.lock' in text or 'ReentrantLock$Sync.lock' in text:
        for fr in stack:
            if 'am.ik.sluice' in fr:
                if 'Loop.call' in fr:
                    return 'wait-loop-lock'
                return 'wait-lock@' + re.sub(r'\(.*', '', fr.split('/')[-1]).replace('am.ik.sluice.', '')
        return 'wait-lock@jdk'
    if not stack:
        return 'no-stack'
    return 'other'


def thread_kind(name):
    n = re.sub(r'\d+', '#', name)
    return n


class DumpSampler(threading.Thread):
    def __init__(self, pid, period=0.25):
        super().__init__(daemon=True)
        self.pid, self.period = pid, period
        self.stop_flag = threading.Event()
        self.samples = 0
        self.states = {}
        self.kinds_max = {}

    def run(self):
        while not self.stop_flag.is_set():
            threads = thread_dump(self.pid)
            if threads:
                self.samples += 1
                kinds = {}
                for name, stack in threads:
                    st = classify(stack)
                    self.states[st] = self.states.get(st, 0) + 1
                    k = thread_kind(name)
                    kinds[k] = kinds.get(k, 0) + 1
                for k, v in kinds.items():
                    self.kinds_max[k] = max(self.kinds_max.get(k, 0), v)
            self.stop_flag.wait(self.period)

    def summary(self):
        n = max(self.samples, 1)
        return {'samples': self.samples,
                'avg_threads_in': {k: round(v / n, 3) for k, v in sorted(self.states.items())},
                'max_threads_by_kind': dict(sorted(self.kinds_max.items(), key=lambda kv: -kv[1])[:30])}


class GaugeSampler(threading.Thread):
    """The largest value of the server's and the client's memory gauges over the measured
    window (tunnel budget used, tunnel payload buffered, heap used), every 250ms."""

    PATTERN = (r'^(sluice_tunnel_budget_used_bytes|sluice_tunnel_buffered_bytes|sluice_h2demux_budget_used_bytes'
               r'|sluice_h2demux_credit_bytes|sluice_h2demux_held_bytes|jvm_memory_used_bytes\{.*heap)')

    def __init__(self, period=0.25):
        super().__init__(daemon=True)
        self.period = period
        self.stop_flag = threading.Event()
        self.maxima = {}

    def run(self):
        while not self.stop_flag.is_set():
            for name, port in (('server', MGMT), ('client', CMGMT)):
                try:
                    values = prom(port, self.PATTERN)
                except Exception:
                    continue
                heap = sum(v for k, v in values.items() if k.startswith('jvm_memory_used_bytes'))
                sample = {'heap_MiB': heap / MiB}
                for k, v in values.items():
                    if k.startswith('sluice_tunnel_budget_used_bytes'):
                        sample['budget_used_KiB'] = v / 1024
                    elif k.startswith('sluice_tunnel_buffered_bytes'):
                        sample['buffered_KiB'] = v / 1024
                    elif k.startswith('sluice_h2demux_budget_used_bytes'):
                        sample['h2_budget_used_KiB'] = v / 1024
                    elif k.startswith('sluice_h2demux_credit_bytes'):
                        sample['h2_credit_KiB'] = v / 1024
                    elif k.startswith('sluice_h2demux_held_bytes'):
                        sample['h2_held_KiB'] = v / 1024
                for k, v in sample.items():
                    key = f'{name}_max_{k}'
                    self.maxima[key] = round(max(self.maxima.get(key, 0), v), 1)
            self.stop_flag.wait(self.period)


GAUGES = [False]


def h2counters():
    try:
        d = prom(pattern=r'^sluice_(h2demux|data_connections)\w*_total')
        return {re.sub(r',?node="[^"]*"', '', k): v for k, v in d.items()}
    except Exception:
        return {}


def run_load(name, args, results, extra=None, port=DATA, jfr=None, dumps=False):
    """Runs one load client; samples server / client / upstream CPU at the MARK lines."""
    tally_reset()
    before = h2counters()
    pids = {n: procs[n].pid for n in ['server', 'client', 'upstream'] if n in procs}
    cmd = ['java', '-Xmx4g', '-cp', CP, 'Load', '--port', str(port), *args]
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    pids['load'] = p.pid
    marks = {}
    loop_marks = {}
    result = None
    other = []
    for line in p.stdout:
        line = line.strip()
        if line in ('MARK start', 'MARK stop'):
            if jfr and line == 'MARK stop':
                subprocess.run(['jcmd', str(pids['server']), 'JFR.stop', 'name=w', f'filename={OUT}/{jfr}.jfr'],
                               capture_output=True)
            marks[line] = (time.time(), {n: cpu_seconds(pid) for n, pid in pids.items()}, alloc_bytes(),
                           {n: sys_seconds(pid) for n, pid in pids.items()})
            if PATCHED[0]:
                loop_marks[line] = loop_stats()
            if jfr and line == 'MARK start':
                subprocess.run(['jcmd', str(pids['server']), 'JFR.start', 'name=w',
                                f'settings={HERE}/{JFC[0]}.jfc'], capture_output=True)
            if dumps and line == 'MARK start':
                sampler = DumpSampler(pids['server'])
                sampler.start()
            if GAUGES[0] and line == 'MARK start':
                gauges = GaugeSampler()
                gauges.start()
            if GAUGES[0] and line == 'MARK stop':
                gauges.stop_flag.set()
                gauges.join()
            if dumps and line == 'MARK stop':
                sampler.stop_flag.set()
                sampler.join()
        elif line.startswith('{'):
            result = json.loads(line)
        else:
            other.append(line)
    p.wait()
    if result is None:
        log('load failed', name, other[-20:])
        return None
    (ta, ca, aa, sa), (tb, cb, ab, sb) = marks['MARK start'], marks['MARK stop']
    result['sys_share'] = {n: round((sb[n] - sa[n]) / max(cb[n] - ca[n], 1e-9), 2) for n in ca}
    result['alloc_MiB'] = {n: round((ab[n] - aa[n]) / MiB, 1) for n in aa}
    wall = tb - ta
    result['cpu_cores'] = {n: round((cb[n] - ca[n]) / wall, 3) for n in ca}
    reqs = max(result['requests'], 1)
    mib = (result['in_MBps'] + result['out_MBps']) * result['secs']
    result['cpu_ms_per_req'] = {n: round((cb[n] - ca[n]) * 1000 / reqs, 4) for n in ca}
    if mib > 1:
        result['cpu_ms_per_MiB'] = {n: round((cb[n] - ca[n]) * 1000 / mib, 3) for n in ca}
        result['alloc_per_byte'] = {n: round((ab[n] - aa[n]) / (mib * MiB), 2) for n in aa}
    result['alloc_KiB_per_req'] = {n: round((ab[n] - aa[n]) / 1024 / reqs, 2) for n in aa}
    result['upstream_tally'] = tally()
    time.sleep(1.5)
    after = h2counters()
    result['h2demux_counters'] = {k: v - before.get(k, 0) for k, v in after.items() if v - before.get(k, 0)}
    if result['h2demux_counters']:
        log('counters', json.dumps(result['h2demux_counters']))
    if PATCHED[0]:
        a, b = loop_marks['MARK start'], loop_marks['MARK stop']
        secs = (b[0] - a[0]) / 1e9
        acq, cont = b[1] - a[1], b[2] - a[2]
        result['loop_lock'] = {'acquired_per_s': round(acq / secs), 'contended_share': round(cont / max(acq, 1), 4),
                               'wait_ms_per_s': round((b[3] - a[3]) / 1e6 / secs, 1),
                               'hold_ms_per_s': round((b[4] - a[4]) / 1e6 / secs, 1),
                               'avg_wait_us': round((b[3] - a[3]) / 1e3 / max(cont, 1), 1),
                               'avg_hold_us': round((b[4] - a[4]) / 1e3 / max(acq, 1), 2),
                               'acq_per_req': round(acq / max(result['requests'], 1), 1)}
        log('loop lock', json.dumps(result['loop_lock']))
    if GAUGES[0]:
        result['gauges'] = gauges.maxima
        log('gauges', json.dumps(gauges.maxima))
    if dumps:
        result['server_threads'] = sampler.summary()
        log('threads', json.dumps(result['server_threads']['avg_threads_in']))
    result['name'] = name
    if extra:
        result.update(extra)
    results.append(result)
    with open(os.path.join(OUT, 'results.jsonl'), 'a') as f:
        f.write(json.dumps(result) + '\n')
    log(name, 'rps', result['rps'], 'in', result['in_MBps'], 'out', result['out_MBps'], 'p50', result['p50_ms'],
        'p99', result['p99_ms'], 'max', result['max_ms'], 'err', result['errors'], result['error_kinds'], 'cpu',
        result['cpu_cores'])
    return result


def lg(proto, conns, streams, path, dur=10, warm=3, extra=()):
    auth = 'h2.local' if proto == 'h2' else 'h1.local'
    return ['--proto', proto, '--authority', auth, '--connections', str(conns), '--streams', str(streams),
            '--path', path, '--duration', str(dur), '--warmup', str(warm), *extra]


GiB = 1 << 30
MiB = 1 << 20


def suite_throughput(results, tag):
    big = f'/bytes?n={256 * MiB}'
    for proto in ['h1', 'h2']:
        for c in [1, 10]:
            run_load(f'{tag}download {proto} c{c} s1', lg(proto, c, 1, big), results)
    for s in [10]:
        run_load(f'{tag}download h2 c1 s{s}', lg('h2', 1, s, big), results)
    # large client windows (browser-like: 6MiB stream, 15MiB connection)
    run_load(f'{tag}download h2 c1 s1 bigwin', lg('h2', 1, 1, big, extra=('--stream-window', str(6 * MiB),
                                                                       '--conn-window', str(15 * MiB))), results)
    for proto in ['h1', 'h2']:
        for c in [1, 10]:
            run_load(f'{tag}upload {proto} c{c} s1', lg(proto, c, 1, '/sink', extra=('--upload', str(64 * MiB))),
                     results)
    run_load(f'{tag}upload h2 c1 s10', lg('h2', 1, 10, '/sink', extra=('--upload', str(64 * MiB))), results)


def suite_small(results, tag):
    for c in [1, 10, 50]:
        for s in [1, 10, 100]:
            run_load(f'{tag}small h2 c{c} s{s}', lg('h2', c, s, '/small'), results)
    for c in [1, 10, 50, 100]:
        run_load(f'{tag}small h1 c{c}', lg('h1', c, 1, '/small'), results)


def suite_direct(results, tag):
    """The harness without sluice: load client straight to the upstream ports."""
    big = f'/bytes?n={256 * MiB}'
    run_load(f'{tag}direct download h2 c1 s1', lg('h2', 1, 1, big), results, port=UP_H2)
    run_load(f'{tag}direct download h1 c1 s1', lg('h1', 1, 1, big), results, port=UP_H1)
    run_load(f'{tag}direct upload h2 c1 s1', lg('h2', 1, 1, '/sink', extra=('--upload', str(64 * MiB))), results,
             port=UP_H2)
    run_load(f'{tag}direct small h2 c10 s100', lg('h2', 10, 100, '/small'), results, port=UP_H2)
    run_load(f'{tag}direct small h2 c1 s10', lg('h2', 1, 10, '/small'), results, port=UP_H2)
    run_load(f'{tag}direct small h1 c10', lg('h1', 10, 1, '/small'), results, port=UP_H1)


def suite_wan(results, tag):
    """Per-stream throughput against the tunnel RTT (run with --delay)."""
    big = f'/bytes?n={64 * MiB}'
    for proto in ['h1', 'h2']:
        run_load(f'{tag}download {proto} c1 s1', lg(proto, 1, 1, big, dur=15), results)
    run_load(f'{tag}download h2 c1 s1 bigwin', lg('h2', 1, 1, big, dur=15, extra=(
        '--stream-window', str(6 * MiB), '--conn-window', str(15 * MiB))), results)
    run_load(f'{tag}download h2 c1 s8', lg('h2', 1, 8, big, dur=15), results)
    run_load(f'{tag}download h1 c8', lg('h1', 8, 1, big, dur=15), results)
    for proto in ['h1', 'h2']:
        run_load(f'{tag}upload {proto} c1 s1', lg(proto, 1, 1, '/sink', dur=15, extra=('--upload', str(16 * MiB))),
                 results)
    run_load(f'{tag}small h2 c1 s1', lg('h2', 1, 1, '/small'), results)
    run_load(f'{tag}small h1 c1', lg('h1', 1, 1, '/small'), results)


def suite_cwan(results, tag):
    """Per-stream throughput against the client RTT: a 10ms one-way delay proxy between
    the load client and the data port (tunnel on loopback)."""
    start('cproxy', ['java', '-cp', CP, 'DelayProxy', '18003', '127.0.0.1', str(DATA), '10'], 'cproxy.log')
    time.sleep(1.5)
    try:
        big = f'/bytes?n={64 * MiB}'
        for proto in ['h1', 'h2']:
            run_load(f'{tag}cwan download {proto} c1 s1', lg(proto, 1, 1, big, dur=15), results, port=18003)
        run_load(f'{tag}cwan download h2 c1 s1 bigwin', lg('h2', 1, 1, big, dur=15, extra=(
            '--stream-window', str(6 * MiB), '--conn-window', str(15 * MiB))), results, port=18003)
        for proto in ['h1', 'h2']:
            run_load(f'{tag}cwan upload {proto} c1 s1', lg(proto, 1, 1, '/sink', dur=15, extra=(
                '--upload', str(16 * MiB))), results, port=18003)
        run_load(f'{tag}cwan upload h2 c1 s8', lg('h2', 1, 8, '/sink', dur=15, extra=(
            '--upload', str(16 * MiB))), results, port=18003)
    finally:
        p = procs.pop('cproxy')
        os.killpg(p.pid, signal.SIGTERM)


def suite_profile(results, tag):
    """Server JFR (contention.jfc: parks and monitor enters at 0ms, allocation samples)
    over the measured window of a few scenarios; the numbers of these runs carry the JFR
    overhead and are not the throughput figures."""
    big = f'/bytes?n={256 * MiB}'
    run_load(f'{tag}prof download h2 c1 s1', lg('h2', 1, 1, big, dur=8), results, jfr='prof-download-h2-c1s1')
    run_load(f'{tag}prof download h1 c1 s1', lg('h1', 1, 1, big, dur=8), results, jfr='prof-download-h1-c1s1')
    run_load(f'{tag}prof upload h2 c1 s1', lg('h2', 1, 1, '/sink', dur=8, extra=('--upload', str(64 * MiB))),
             results, jfr='prof-upload-h2-c1s1')
    run_load(f'{tag}prof small h2 c1 s100', lg('h2', 1, 100, '/small', dur=8), results, jfr='prof-small-h2-c1s100')
    run_load(f'{tag}prof small h2 c10 s100', lg('h2', 10, 100, '/small', dur=8), results,
             jfr='prof-small-h2-c10s100')
    run_load(f'{tag}prof small h2 c50 s1', lg('h2', 50, 1, '/small', dur=8), results, jfr='prof-small-h2-c50s1')
    run_load(f'{tag}prof small h1 c50', lg('h1', 50, 1, '/small', dur=8), results, jfr='prof-small-h1-c50')


def suite_cpuprof(results, tag):
    """As suite_profile with the plain profile settings (CPU samples, allocation)."""
    JFC[0] = 'cpu'
    big = f'/bytes?n={256 * MiB}'
    run_load(f'{tag}cpuprof download h2 c1 s1', lg('h2', 1, 1, big, dur=8), results, jfr='cpu-download-h2-c1s1')
    run_load(f'{tag}cpuprof download h1 c1 s1', lg('h1', 1, 1, big, dur=8), results, jfr='cpu-download-h1-c1s1')
    run_load(f'{tag}cpuprof small h2 c1 s100', lg('h2', 1, 100, '/small', dur=8), results,
             jfr='cpu-small-h2-c1s100')
    run_load(f'{tag}cpuprof small h2 c50 s1', lg('h2', 50, 1, '/small', dur=8), results, jfr='cpu-small-h2-c50s1')
    run_load(f'{tag}cpuprof small h1 c50', lg('h1', 50, 1, '/small', dur=8), results, jfr='cpu-small-h1-c50')


def suite_contention(results, tag):
    """Loop lock waits, from virtual-thread dumps every 250ms of the measured window
    (JFR does not record a virtual thread parking on a ReentrantLock)."""
    big = f'/bytes?n={256 * MiB}'
    run_load(f'{tag}cont download h2 c1 s1', lg('h2', 1, 1, big), results, dumps=True)
    run_load(f'{tag}cont download h2 c1 s10', lg('h2', 1, 10, big), results, dumps=True)
    run_load(f'{tag}cont upload h2 c1 s10', lg('h2', 1, 10, '/sink', extra=('--upload', str(64 * MiB))), results,
             dumps=True)
    run_load(f'{tag}cont small h2 c1 s100', lg('h2', 1, 100, '/small'), results, dumps=True)
    run_load(f'{tag}cont small h2 c10 s100', lg('h2', 10, 100, '/small'), results, dumps=True)
    run_load(f'{tag}cont small h2 c50 s10', lg('h2', 50, 10, '/small'), results, dumps=True)
    run_load(f'{tag}cont small h1 c50', lg('h1', 50, 1, '/small'), results, dumps=True)


def suite_lock(results, tag):
    """Loop lock counters of the instrumented server build (run with --patched)."""
    big = f'/bytes?n={256 * MiB}'
    run_load(f'{tag}lock download h2 c1 s1', lg('h2', 1, 1, big), results)
    run_load(f'{tag}lock download h2 c1 s10', lg('h2', 1, 10, big), results)
    run_load(f'{tag}lock upload h2 c1 s10', lg('h2', 1, 10, '/sink', extra=('--upload', str(64 * MiB))), results)
    for c, st in [(1, 1), (1, 10), (1, 100), (10, 100), (50, 1), (50, 10)]:
        run_load(f'{tag}lock small h2 c{c} s{st}', lg('h2', c, st, '/small'), results)


def suite_winexp(results, tag):
    """h2 per-connection throughput, for the window experiment (compare runs with and
    without --window-patch, at --delay 0 and 10)."""
    big = f'/bytes?n={64 * MiB}'
    run_load(f'{tag}winexp download h2 c1 s1', lg('h2', 1, 1, big, dur=12), results)
    run_load(f'{tag}winexp download h2 c1 s8', lg('h2', 1, 8, big, dur=12), results)
    run_load(f'{tag}winexp download h1 c1', lg('h1', 1, 1, big, dur=12), results)
    run_load(f'{tag}winexp upload h2 c1 s1', lg('h2', 1, 1, '/sink', dur=12, extra=('--upload', str(16 * MiB))),
             results)
    run_load(f'{tag}winexp small h2 c1 s100', lg('h2', 1, 100, '/small', dur=8), results)


def suite_budget(results, tag):
    """Throughput against the tunnel RTT (run with --delay) with one and many connections,
    and the largest heap, tunnel budget use and buffered payload of the server and the
    client over each measured window (todo 134: windows budgeted instead of reserved)."""
    GAUGES[0] = True
    big = f'/bytes?n={64 * MiB}'
    run_load(f'{tag}budget download h1 c1', lg('h1', 1, 1, big, dur=12), results)
    run_load(f'{tag}budget download h2 c1 s1', lg('h2', 1, 1, big, dur=12), results)
    for c in [8, 32, 100]:
        run_load(f'{tag}budget download h1 c{c}', lg('h1', c, 1, big, dur=12), results)
    run_load(f'{tag}budget upload h1 c1', lg('h1', 1, 1, '/sink', dur=12, extra=('--upload', str(16 * MiB))),
             results)
    run_load(f'{tag}budget upload h2 c1 s1', lg('h2', 1, 1, '/sink', dur=12, extra=('--upload', str(16 * MiB))),
             results)
    run_load(f'{tag}budget upload h1 c32', lg('h1', 32, 1, '/sink', dur=12, extra=('--upload', str(16 * MiB))),
             results)


def suite_tl(results, tag):
    """One h1 download then one h1 upload at the tunnel RTT, three times each, short."""
    GAUGES[0] = True
    for i in range(3):
        run_load(f'{tag}tl download h1 c1 #{i}', lg('h1', 1, 1, f'/bytes?n={64 * MiB}', dur=6), results)
    for i in range(2):
        run_load(f'{tag}tl upload h1 c1 #{i}', lg('h1', 1, 1, '/sink', dur=6, extra=('--upload', str(16 * MiB))),
                 results)


def histo_kb(pid, label):
    """Live heap from one class histogram (a full GC and the count in one safepoint), KiB;
    the top classes go to out/histo-<label>.txt."""
    out = subprocess.run(['jcmd', str(pid), 'GC.class_histogram'], capture_output=True, text=True).stdout
    with open(os.path.join(OUT, f'histo-{label}.txt'), 'w') as f:
        f.write('\n'.join(out.splitlines()[:25]))
    m = re.search(r'Total\s+\d+\s+(\d+)', out)
    return int(m.group(1)) // 1024 if m else -1


def nmt_other_kb(pid):
    """NMT's 'Other' committed KiB (direct buffers among it); -1 without NMT."""
    out = subprocess.run(['jcmd', str(pid), 'VM.native_memory', 'summary'], capture_output=True, text=True).stdout
    m = re.search(r'-\s+Other \(reserved=\d+KB, committed=(\d+)KB\)', out)
    return int(m.group(1)) if m else -1


def suite_live(results, tag):
    """Live heap (after full GCs) of server and client in the middle of 32 h1 downloads and
    of 32 h1 uploads at full speed, against idle."""
    idle = {n: histo_kb(procs[n].pid, f'{tag}idle-{n}') for n in ('server', 'client')}
    idle.update({n + '_nmt_other_kb': nmt_other_kb(procs[n].pid) for n in ('server', 'client')})
    log(tag, 'live idle', idle)
    for what, path, extra in [('download', f'/bytes?n={64 * MiB}', ()),
                              ('upload', '/sink', ('--upload', str(16 * MiB)))]:
        cmd = ['java', '-Xmx4g', '-cp', CP, 'Load', '--port', str(DATA), *lg('h1', 32, 1, path, dur=12, extra=extra)]
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        time.sleep(9)
        live = {n: histo_kb(procs[n].pid, f'{tag}{what}-{n}') for n in ('server', 'client')}
        live.update({n + '_nmt_other_kb': nmt_other_kb(procs[n].pid) for n in ('server', 'client')})
        out = p.communicate()[0]
        line = [l for l in out.splitlines() if 'MiB/s' in l or 'in_MBps' in l or l.startswith('RESULT')]
        log(tag, 'live', what, 'c32', live, line[-1][:200] if line else '')
        results.append({'name': f'{tag}live {what} h1 c32', 'idle_kb': idle, 'live_kb': live})
        with open(os.path.join(OUT, 'results.jsonl'), 'a') as f:
            f.write(json.dumps(results[-1]) + '\n')
        time.sleep(3)


def suite_budgetone(results, tag):
    """One h1 connection each way at the tunnel RTT, three times (run with --delay)."""
    GAUGES[0] = True
    for i in range(3):
        run_load(f'{tag}budget download h1 c1 #{i}', lg('h1', 1, 1, f'/bytes?n={64 * MiB}', dur=12), results)
        run_load(f'{tag}budget upload h1 c1 #{i}', lg('h1', 1, 1, '/sink', dur=12, extra=('--upload', str(16 * MiB))),
                 results)


def suite_h2credit(results, tag):
    """h2 throughput against the tunnel RTT (run with --delay) with one and many h2
    connections, and the largest h2 queue budget use, peer credit and held bytes of the
    server over each measured window (todo 135: connection windows charged to the h2
    queue budget instead of reserved per connection)."""
    GAUGES[0] = True
    big = f'/bytes?n={64 * MiB}'
    run_load(f'{tag}h2credit download h2 c1 s1', lg('h2', 1, 1, big, dur=12), results)
    for c in [8, 32, 100]:
        run_load(f'{tag}h2credit download h2 c{c} s1', lg('h2', c, 1, big, dur=12), results)
    run_load(f'{tag}h2credit upload h2 c1 s1', lg('h2', 1, 1, '/sink', dur=12, extra=('--upload', str(16 * MiB))),
             results)
    run_load(f'{tag}h2credit small h2 c10 s10', lg('h2', 10, 10, '/small', dur=8), results)


def live_heap_kb(pid):
    """Used heap after two full GCs, in KiB."""
    for _ in range(2):
        subprocess.run(['jcmd', str(pid), 'GC.run'], capture_output=True)
    out = subprocess.run(['jcmd', str(pid), 'GC.heap_info'], capture_output=True, text=True).stdout
    m = re.search(r'used (\d+)K', out)
    return int(m.group(1)) if m else -1


def snapshot(label):
    snap = {'label': label}
    for name in ('server', 'client'):
        pid = procs[name].pid
        threads = thread_dump(pid)
        kinds = {}
        for tname, _ in threads:
            k = thread_kind(tname)
            kinds[k] = kinds.get(k, 0) + 1
        snap[name] = {'heap_kb': live_heap_kb(pid), 'threads': len(threads), 'rss_kb': rss_kb(pid),
                      'kinds': kinds}
    return snap


def suite_hold(results, tag):
    """Live heap, RSS and threads per held connection / stream (streams held open by
    the upstream's /hold, which never ends its response)."""
    time.sleep(2)
    base = snapshot('baseline')
    log('baseline', json.dumps({n: {k: v for k, v in base[n].items() if k != 'kinds'} for n in ('server', 'client')}))
    configs = [('h2', 1, 1), ('h2', 50, 1), ('h2', 100, 1), ('h2', 1, 100), ('h2', 10, 100), ('h2', 50, 10),
               ('h1', 50, 1), ('h1', 100, 1)]
    for proto, c, st in configs:
        auth = 'h2.local' if proto == 'h2' else 'h1.local'
        cmd = ['java', '-Xmx2g', '-cp', CP, 'Load', '--port', str(DATA), '--proto', proto, '--authority', auth,
               '--connections', str(c), '--streams', str(st), '--path', '/hold', '--hold', '45']
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        holding = None
        for line in p.stdout:
            if line.startswith('HOLDING'):
                holding = line.strip()
                break
        time.sleep(2)
        snap = snapshot(f'{proto} c{c} s{st}')
        rec = {'name': f'{tag}hold {proto} c{c} s{st}', 'holding': holding, 'connections': c, 'streams': st,
               'proto': proto}
        for n in ('server', 'client'):
            dh = snap[n]['heap_kb'] - base[n]['heap_kb']
            dt = snap[n]['threads'] - base[n]['threads']
            rec[n] = {'heap_delta_kb': dh, 'heap_kb_per_conn': round(dh / c, 1),
                      'heap_kb_per_stream': round(dh / (c * st), 2), 'threads_delta': dt,
                      'threads_per_conn': round(dt / c, 2), 'rss_delta_kb': snap[n]['rss_kb'] - base[n]['rss_kb'],
                      'kinds_delta': {k: v - base[n]['kinds'].get(k, 0) for k, v in snap[n]['kinds'].items()
                                      if v != base[n]['kinds'].get(k, 0)}}
        log(rec['name'], holding, json.dumps({n: {k: v for k, v in rec[n].items() if k != 'kinds_delta'}
                                               for n in ('server', 'client')}))
        log('   server threads', json.dumps(rec['server']['kinds_delta']))
        log('   client threads', json.dumps(rec['client']['kinds_delta']))
        results.append(rec)
        with open(os.path.join(OUT, 'results.jsonl'), 'a') as f:
            f.write(json.dumps(rec) + '\n')
        p.kill()
        p.wait()
        time.sleep(4)


def suite_resets(results, tag):
    """One short h2 upload ended by the load client mid-request, for the reset counters."""
    run_load(f'{tag}resets upload h2 c1 s1', lg('h2', 1, 1, '/sink', dur=2, warm=1,
                                                 extra=('--upload', str(64 * MiB))), results)


def suite_smoke(results, tag):
    run_load(f'{tag}smoke h1 small', lg('h1', 1, 1, '/small', dur=2, warm=1), results)
    run_load(f'{tag}smoke h2 small', lg('h2', 1, 10, '/small', dur=2, warm=1), results)
    run_load(f'{tag}smoke h2 download', lg('h2', 1, 1, f'/bytes?n={64 * MiB}', dur=3, warm=1), results)
    run_load(f'{tag}smoke h2 upload', lg('h2', 1, 1, '/sink', dur=3, warm=1, extra=('--upload', str(16 * MiB))),
             results)
    run_load(f'{tag}smoke h1 upload', lg('h1', 1, 1, '/sink', dur=3, warm=1, extra=('--upload', str(16 * MiB))),
             results)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('repo')
    ap.add_argument('suite')
    ap.add_argument('--delay', type=int, default=0)
    ap.add_argument('--jfr')
    ap.add_argument('--tag', default='')
    ap.add_argument('--upstream-window', type=int, default=65535)
    ap.add_argument('--patched', action='store_true')
    ap.add_argument('--window-patch', action='store_true')
    ap.add_argument('--server-opt', action='append', default=[])
    ap.add_argument('--server-arg', action='append', default=[])
    ap.add_argument('--client-arg', action='append', default=[])
    ap.add_argument('--client-opt', action='append', default=[])
    a = ap.parse_args()
    results = []
    PATCHED[0] = a.patched or a.window_patch
    WINDOW_PATCH[0] = a.window_patch
    try:
        start_stack(a.repo, delay=a.delay, jfr=a.jfr, upstream_window=a.upstream_window, tag=a.tag,
                    server_opts=a.server_opt, server_args=a.server_arg, client_args=a.client_arg, client_opts=a.client_opt)
        prefix = (a.tag + ' ') if a.tag else ''
        for s in a.suite.split(','):
            globals()['suite_' + s](results, prefix)
        try:
            with open(os.path.join(OUT, f'prom{a.tag}.json'), 'w') as f:
                json.dump({'server': prom(), 'client': prom(CMGMT)}, f, indent=1)
        except Exception as e:
            log('prom failed', e)
    finally:
        stop_stack()


if __name__ == '__main__':
    main()
