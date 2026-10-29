#!/usr/bin/env python3
"""Spike: a Cloud Controller v3 / UAA / log-cache stub, driven by the stock cf CLI.

    python3 stub.py [port]      # every request logged to stderr

No wicket behind it: start/stop only flip states. Records what the CLI asks for and
which response fields it needs.
"""
import base64
import hashlib
import hmac
import io
import json
import os
import random
import re
import sys
import threading
import time
import uuid
import zipfile
from datetime import datetime, timezone
from email.parser import BytesParser
from email.policy import default as email_policy
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import yaml

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 18080
BASE = f"http://localhost:{PORT}"
UAA = BASE + os.environ.get("UAA_PREFIX", "/uaa")
LOG_CACHE = BASE + os.environ.get("LOG_CACHE_PREFIX", "")
VERSION = os.environ.get("CC_VERSION", "3.200.0")
KEY = os.urandom(32)
LOCK = threading.RLock()


def now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def new_guid():
    return str(uuid.uuid4())


def meta():
    return {"labels": {}, "annotations": {}}


def rel(guid):
    return {"data": {"guid": guid} if guid else None}


ORG = new_guid()
SPACE = new_guid()
STACK = new_guid()
BROKER = new_guid()
QUOTA = new_guid()

S = {
    "organizations": {},
    "spaces": {},
    "domains": {},
    "stacks": {},
    "apps": {},
    "processes": {},
    "packages": {},
    "builds": {},
    "droplets": {},
    "routes": {},
    "jobs": {},
    "service_brokers": {},
    "service_offerings": {},
    "service_plans": {},
    "service_instances": {},
    "service_credential_bindings": {},
    "deployments": {},
}
DEPLOYMENTS = S["deployments"]
ENV = {}  # app guid -> env vars
CURRENT_DROPLET = {}  # app guid -> droplet guid
BITS = {}  # package guid -> zip bytes
LOGS = {}  # app guid -> [(ns, source_type, line)]


def add(kind, resource):
    resource.setdefault("created_at", now())
    resource.setdefault("updated_at", resource["created_at"])
    resource.setdefault("metadata", meta())
    resource.setdefault("links", {"self": {"href": f"{BASE}/v3/{kind}/{resource['guid']}"}})
    S[kind][resource["guid"]] = resource
    return resource


add("organizations", {"guid": ORG, "name": "sluice", "suspended": False,
                      "relationships": {"quota": rel(QUOTA)}})
add("spaces", {"guid": SPACE, "name": "wicket",
               "relationships": {"organization": rel(ORG), "quota": rel(None)}})
for name in ("localhost",):
    add("domains", {"guid": new_guid(), "name": name, "internal": False, "router_group": None,
                    "supported_protocols": ["http"],
                    "relationships": {"organization": rel(None),
                                      "shared_organizations": {"data": []}}})
add("stacks", {"guid": STACK, "name": "wasip3", "description": "WASI 0.3 components",
               "build_rootfs_image": "wasip3", "run_rootfs_image": "wasip3", "default": True})
add("service_brokers", {"guid": BROKER, "name": "sluice-wicket", "url": BASE + "/broker",
                        "relationships": {"space": rel(None)}})


def offering(name, description, plans):
    o = add("service_offerings", {
        "guid": new_guid(), "name": name, "description": description, "available": True,
        "tags": [], "requires": [], "shareable": False, "documentation_url": None,
        "broker_catalog": {"id": name, "metadata": {}, "features": {
            "plan_updateable": False, "bindable": True, "instances_retrievable": False,
            "bindings_retrievable": False, "allow_context_updates": False}},
        "relationships": {"service_broker": rel(BROKER)}})
    for plan, text in plans:
        add("service_plans", {
            "guid": new_guid(), "name": plan, "description": text, "visibility_type": "public",
            "available": True, "free": True, "costs": [], "maintenance_info": {},
            "broker_catalog": {"id": f"{name}-{plan}", "metadata": {},
                               "maximum_polling_duration": None,
                               "features": {"bindable": True, "plan_updateable": False}},
            "schemas": {"service_instance": {"create": {"parameters": {}},
                                             "update": {"parameters": {}}},
                        "service_binding": {"create": {"parameters": {}}}},
            "relationships": {"service_offering": rel(o["guid"])}})


offering("keyvalue", "wasi:keyvalue buckets", [("local", "sqlite store 'local'")])
offering("wasi-network", "outgoing sockets for the guest",
         [("inherit-network", "every address"), ("allow-ip-name-lookup", "DNS lookups")])


def default_domain():
    return next(iter(S["domains"].values()))


# ---- JWT ---------------------------------------------------------------------------------

def b64(data):
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def jwt(claims):
    head = b64(json.dumps({"alg": "HS256", "typ": "JWT", "kid": "spike"}).encode())
    body = b64(json.dumps(claims).encode())
    sig = b64(hmac.new(KEY, f"{head}.{body}".encode(), hashlib.sha256).digest())
    return f"{head}.{body}.{sig}"


def token(user, client="cf", grant="password"):
    issued = int(time.time())
    user_id = str(uuid.uuid5(uuid.NAMESPACE_URL, user))
    claims = {"jti": new_guid(), "sub": user_id, "scope": [
        "cloud_controller.read", "cloud_controller.write", "openid",
        "cloud_controller.admin"], "client_id": client, "cid": client, "azp": client,
        "grant_type": grant, "user_id": user_id, "origin": "uaa", "user_name": user,
        "email": user, "iat": issued, "exp": issued + 12 * 3600, "iss": UAA + "/oauth/token",
        "zid": "uaa", "aud": ["cloud_controller", "openid", client]}
    return {"access_token": jwt(claims), "token_type": "bearer",
            "refresh_token": jwt({**claims, "jti": new_guid() + "-r",
                                  "exp": issued + 30 * 86400}),
            "expires_in": 12 * 3600 - 1, "scope": " ".join(claims["scope"]),
            "jti": claims["jti"]}


# ---- views -------------------------------------------------------------------------------

def paginate(path, items, included=None):
    body = {"pagination": {"total_results": len(items), "total_pages": 1,
                           "first": {"href": BASE + path}, "last": {"href": BASE + path},
                           "next": None, "previous": None},
            "resources": items}
    if included is not None:
        body["included"] = included
    return body


def csv(query, key):
    values = query.get(key)
    if not values:
        return None
    return [v for value in values for v in value.split(",") if v != ""] or [""]


def rel_guid(resource, name):
    data = resource.get("relationships", {}).get(name, {}).get("data")
    return data and data.get("guid")


FILTERS = {
    "names": lambda r: [r.get("name")],
    "guids": lambda r: [r["guid"]],
    "space_guids": lambda r: [rel_guid(r, "space")],
    "organization_guids": lambda r: [rel_guid(r, "organization")],
    "app_guids": lambda r: ([rel_guid(r, "app")] if "app" in r.get("relationships", {})
                            else [d["app"]["guid"] for d in r.get("destinations", [])]),
    "hosts": lambda r: [r.get("host")],
    "paths": lambda r: [r.get("path")],
    "ports": lambda r: [str(r.get("port"))],
    "domain_guids": lambda r: [rel_guid(r, "domain")],
    "types": lambda r: [r.get("type")],
    "states": lambda r: [r.get("state")],
    "service_instance_guids": lambda r: [rel_guid(r, "service_instance")],
    "service_instance_names": lambda r: [S["service_instances"].get(
        rel_guid(r, "service_instance"), {}).get("name")],
    "service_offering_guids": lambda r: [rel_guid(r, "service_offering")],
    "service_offering_names": lambda r: [S["service_offerings"].get(
        rel_guid(r, "service_offering"), {}).get("name")],
    "service_plan_guids": lambda r: [rel_guid(r, "service_plan")],
    "service_plan_names": lambda r: [S["service_plans"].get(
        rel_guid(r, "service_plan"), {}).get("name")],
    "service_broker_names": lambda r: ["sluice-wicket"],
    "service_broker_guids": lambda r: [BROKER],
}
IGNORED = {"order_by", "per_page", "page", "include", "label_selector", "available",
           "fields", "lifecycle_type", "available", "include"}


def select(kind, query):
    items = list(S[kind].values())
    for key in query:
        if key.startswith("fields[") or key in IGNORED:
            continue
        if kind in ("service_plans", "service_offerings") and key in ("space_guids",
                                                                       "organization_guids"):
            continue  # visibility: every plan is public
        values = csv(query, key)
        accessor = FILTERS.get(key)
        if accessor is None:
            print(f"   !! unhandled filter {key}={values} on {kind}", file=sys.stderr)
            continue
        items = [r for r in items if any(v in values for v in accessor(r))]
    order = (query.get("order_by") or [""])[0]
    if order:
        field = order.lstrip("-+")
        items.sort(key=lambda r: r.get(field) or "", reverse=order.startswith("-"))
    return items


def included_for(kind, items, query):
    includes = set(csv(query, "include") or [])
    fields = {k[7:-1]: v for k, v in query.items() if k.startswith("fields[")}
    included = {}
    if not includes and not fields:
        return None
    if kind == "apps" and ("space" in includes or "space.organization" in includes):
        spaces = {rel_guid(r, "space") for r in items}
        included["spaces"] = [S["spaces"][g] for g in spaces if g]
        if "space.organization" in includes:
            orgs = {rel_guid(S["spaces"][g], "organization") for g in spaces if g}
            included["organizations"] = [S["organizations"][g] for g in orgs]
    if "domain" in includes:
        doms = {rel_guid(r, "domain") for r in items}
        included["domains"] = [S["domains"][g] for g in doms if g]
    if "organization" in includes and kind == "spaces":
        orgs = {rel_guid(r, "organization") for r in items}
        included["organizations"] = [S["organizations"][g] for g in orgs]
    if kind == "service_plans" and ("service_offering" in includes or fields):
        offs = {rel_guid(r, "service_offering") for r in items}
        included["service_offerings"] = [S["service_offerings"][g] for g in offs]
        included["service_brokers"] = list(S["service_brokers"].values())
    if kind == "service_offerings" and fields:
        included["service_brokers"] = list(S["service_brokers"].values())
    if kind == "service_instances" and (fields or includes):
        plans = {rel_guid(r, "service_plan") for r in items}
        included["service_plans"] = [S["service_plans"][g] for g in plans if g]
        offs = {rel_guid(p, "service_offering") for p in included["service_plans"]}
        included["service_offerings"] = [S["service_offerings"][g] for g in offs]
        included["service_brokers"] = list(S["service_brokers"].values())
        included["spaces"] = [S["spaces"][SPACE]]
        included["organizations"] = [S["organizations"][ORG]]
    if kind == "service_credential_bindings" and "app" in includes:
        apps = {rel_guid(r, "app") for r in items}
        included["apps"] = [S["apps"][g] for g in apps if g in S["apps"]]
    if kind == "service_credential_bindings" and "service_instance" in includes:
        sis = {rel_guid(r, "service_instance") for r in items}
        included["service_instances"] = [S["service_instances"][g] for g in sis]
    return included


def error(status, code, title, detail):
    return status, {"errors": [{"code": code, "title": title, "detail": detail}]}


def not_found(what):
    return error(404, 10010, "CF-ResourceNotFound", f"{what} not found")


def job(operation, errors=None, resource=None):
    g = new_guid()
    links = {"self": {"href": f"{BASE}/v3/jobs/{g}"}}
    if resource:
        links[resource[0]] = {"href": resource[1]}
    add("jobs", {"guid": g, "operation": operation,
                 "state": "FAILED" if errors else "COMPLETE", "errors": errors or [],
                 "warnings": [], "links": links})
    return g


def accepted(job_guid):
    return 202, None, {"Location": f"{BASE}/v3/jobs/{job_guid}"}


def megabytes(value):
    if isinstance(value, int):
        return value
    m = re.fullmatch(r"(\d+)\s*([MG])B?", str(value).strip().upper())
    if not m:
        raise ValueError(f"bad size {value}")
    return int(m[1]) * (1024 if m[2] == "G" else 1)


def log(app_guid, source, line):
    LOGS.setdefault(app_guid, []).append((time.time_ns(), source, line))


# ---- domain ------------------------------------------------------------------------------

def create_app(name, lifecycle=None):
    g = new_guid()
    app = add("apps", {
        "guid": g, "name": name, "state": "STOPPED",
        "lifecycle": lifecycle or {"type": "buildpack",
                                   "data": {"buildpacks": [], "stack": "wasip3"}},
        "relationships": {"space": rel(SPACE)}})
    app["links"].update({
        "space": {"href": f"{BASE}/v3/spaces/{SPACE}"},
        "processes": {"href": f"{BASE}/v3/apps/{g}/processes"},
        "packages": {"href": f"{BASE}/v3/apps/{g}/packages"},
        "environment_variables": {"href": f"{BASE}/v3/apps/{g}/environment_variables"},
        "current_droplet": {"href": f"{BASE}/v3/apps/{g}/droplets/current"},
        "droplets": {"href": f"{BASE}/v3/apps/{g}/droplets"},
        "start": {"href": f"{BASE}/v3/apps/{g}/actions/start", "method": "POST"},
        "stop": {"href": f"{BASE}/v3/apps/{g}/actions/stop", "method": "POST"}})
    process(g, "web")
    ENV[g] = {}
    return app


def process(app_guid, kind):
    for p in S["processes"].values():
        if rel_guid(p, "app") == app_guid and p["type"] == kind:
            return p
    g = new_guid()
    return add("processes", {
        "guid": g, "type": kind, "command": None, "instances": 1, "memory_in_mb": 256,
        "disk_in_mb": 1024, "log_rate_limit_in_bytes_per_second": -1,
        "health_check": {"type": "port", "data": {"timeout": None, "invocation_timeout": None}},
        "readiness_health_check": {"type": "process", "data": {"invocation_timeout": None}},
        "relationships": {"app": rel(app_guid), "revision": rel(None)}, "version": new_guid()})


def find_route(host, domain_guid, path=""):
    for r in S["routes"].values():
        if r["host"] == host and rel_guid(r, "domain") == domain_guid and r["path"] == path:
            return r
    return None


def create_route(host, domain_guid, path=""):
    domain = S["domains"][domain_guid]
    g = new_guid()
    return add("routes", {
        "guid": g, "protocol": "http", "port": None, "host": host, "path": path,
        "url": (f"{host}." if host else "") + domain["name"] + path, "destinations": [],
        "options": {}, "relationships": {"space": rel(SPACE), "domain": rel(domain_guid)}})


def map_route(route, app_guid):
    if not any(d["app"]["guid"] == app_guid for d in route["destinations"]):
        route["destinations"].append({"guid": new_guid(), "app": {
            "guid": app_guid, "process": {"type": "web"}}, "weight": None, "port": 8080,
            "protocol": "http1"})


def parse_route(url):
    """host.domain[/path] against the known domains, the longest match first."""
    host_part, _, path = url.partition("/")
    path = "/" + path if path else ""
    for d in sorted(S["domains"].values(), key=lambda d: -len(d["name"])):
        if host_part == d["name"]:
            return "", d["guid"], path
        if host_part.endswith("." + d["name"]):
            return host_part[: -len(d["name"]) - 1], d["guid"], path
    raise ValueError(f"no domain for route {url}")


WORDS = (["brave", "quiet", "shiny", "lucky", "wise"], ["otter", "heron", "lynx", "fox", "elk"])


def apply_manifest(body):
    manifest = yaml.safe_load(body)
    for spec in manifest.get("applications", []):
        name = spec["name"]
        app = next((a for a in S["apps"].values() if a["name"] == name), None) or create_app(name)
        g = app["guid"]
        if "docker" in spec:
            app["lifecycle"] = {"type": "docker", "data": {}}
        if "buildpacks" in spec or "stack" in spec:
            data = app["lifecycle"]["data"]
            data["buildpacks"] = spec.get("buildpacks", data["buildpacks"])
            data["stack"] = spec.get("stack", data["stack"])
        ENV[g].update({k: str(v) for k, v in (spec.get("env") or {}).items()})
        if spec.get("metadata"):
            for k in ("labels", "annotations"):
                app["metadata"][k].update(spec["metadata"].get(k) or {})
        processes = list(spec.get("processes") or [])
        web = {k: spec[k] for k in ("instances", "memory", "disk_quota", "command",
                                    "health-check-type", "timeout") if k in spec}
        if web:
            processes.append({"type": "web", **web})
        for p in processes:
            proc = process(g, p.get("type", "web"))
            if "instances" in p:
                proc["instances"] = int(p["instances"])
            if "memory" in p:
                proc["memory_in_mb"] = megabytes(p["memory"])
            if "disk_quota" in p:
                proc["disk_in_mb"] = megabytes(p["disk_quota"])
            if "command" in p:
                proc["command"] = p["command"]
        mapped = [r for r in S["routes"].values()
                  if any(d["app"]["guid"] == g for d in r["destinations"])]
        if spec.get("no-route"):
            for r in mapped:
                r["destinations"] = [d for d in r["destinations"] if d["app"]["guid"] != g]
        elif spec.get("routes"):
            for entry in spec["routes"]:
                host, domain_guid, path = parse_route(entry["route"])
                route = find_route(host, domain_guid, path) or create_route(host, domain_guid, path)
                map_route(route, g)
        elif not mapped and (spec.get("default-route") or spec.get("random-route")):
            host = name
            if spec.get("random-route"):
                host = f"{name}-{random.choice(WORDS[0])}-{random.choice(WORDS[1])}-{random.randint(10, 99)}"
            domain = default_domain()
            route = find_route(host, domain["guid"]) or create_route(host, domain["guid"])
            map_route(route, g)
        for entry in spec.get("services") or []:
            si_name = entry if isinstance(entry, str) else entry["name"]
            si = next((s for s in S["service_instances"].values() if s["name"] == si_name), None)
            if si is None:
                raise ValueError(f"Service instance '{si_name}' not found")
            bind(si, g, None if isinstance(entry, str) else entry.get("parameters"))


def bind(si, app_guid, parameters):
    for b in S["service_credential_bindings"].values():
        if rel_guid(b, "service_instance") == si["guid"] and rel_guid(b, "app") == app_guid:
            return b
    return add("service_credential_bindings", {
        "guid": new_guid(), "type": "app", "name": None,
        "last_operation": {"type": "create", "state": "succeeded", "description": "",
                           "created_at": now(), "updated_at": now()},
        "relationships": {"app": rel(app_guid), "service_instance": rel(si["guid"])},
        "parameters": parameters or {}})


def stage(build):
    pkg = S["packages"][build["package"]["guid"]]
    app_guid = rel_guid(pkg, "app")
    if pkg["type"] == "docker":
        # an OCI wasm artifact would be pulled here (wkg / oras layout)
        image = pkg["data"]["image"]
        log(app_guid, "STG", f"Would pull the wasm component of {image}")
        droplet = add("droplets", {
            "guid": new_guid(), "state": "STAGED", "error": None,
            "lifecycle": {"type": "docker", "data": {}}, "execution_metadata": "",
            "process_types": {"web": ""}, "checksum": None, "buildpacks": [], "stack": None,
            "image": image, "relationships": {"app": rel(app_guid)},
            "links": {"package": {"href": f"{BASE}/v3/packages/{pkg['guid']}"}}})
        build["state"] = "STAGED"
        build["droplet"] = {"guid": droplet["guid"]}
        return
    try:
        names = [n for n in zipfile.ZipFile(io.BytesIO(BITS[pkg["guid"]])).namelist()
                 if n.endswith(".wasm")]
    except zipfile.BadZipFile:
        names = []
    log(app_guid, "STG", f"Package holds {len(names)} component(s): {', '.join(names)}")
    command = process(app_guid, "web")["command"]
    chosen = [n for n in names if command in (None, n)]
    if len(chosen) != 1:
        build["state"] = "FAILED"
        build["error"] = ("StagingError - the package must hold one .wasm component, or name "
                          f"one with -c (found: {', '.join(names) or 'none'})")
        log(app_guid, "STG", build["error"])
        return
    data = zipfile.ZipFile(io.BytesIO(BITS[pkg["guid"]])).read(chosen[0])
    droplet = add("droplets", {
        "guid": new_guid(), "state": "STAGED", "error": None,
        "lifecycle": {"type": "buildpack", "data": {}}, "execution_metadata": "",
        "process_types": {"web": chosen[0]},
        "checksum": {"type": "sha256", "value": hashlib.sha256(data).hexdigest()},
        "buildpacks": [{"name": "wasm", "detect_output": "wasm", "buildpack_name": "wasm",
                        "version": "0.0.1"}],
        "stack": "wasip3", "image": None, "relationships": {"app": rel(app_guid)},
        "links": {"package": {"href": f"{BASE}/v3/packages/{pkg['guid']}"}}})
    droplet["links"]["self"] = {"href": f"{BASE}/v3/droplets/{droplet['guid']}"}
    log(app_guid, "STG", f"Staged {chosen[0]} ({len(data)} bytes, sha256 "
                         f"{droplet['checksum']['value'][:12]})")
    build["state"] = "STAGED"
    build["droplet"] = {"guid": droplet["guid"]}


def stats(proc):
    app = S["apps"][rel_guid(proc, "app")]
    running = app["state"] == "STARTED" and app["guid"] in CURRENT_DROPLET
    crashed = running and app["name"].startswith("crash")
    return [{"type": proc["type"], "index": i,
             "state": "CRASHED" if crashed else "RUNNING" if running else "DOWN",
             "routable": True, "usage": {"time": now(), "cpu": 0.0, "cpu_entitlement": 0.0,
                                         "mem": 0, "disk": 0, "log_rate": 0},
             "host": "wicket", "instance_internal_ip": "127.0.0.1", "uptime": 1,
             "mem_quota": proc["memory_in_mb"] << 20, "disk_quota": proc["disk_in_mb"] << 20,
             "log_rate_limit": -1, "fds_quota": 16384, "isolation_segment": None,
             "details": ("wasm route 'crash.localhost': component crash.wasm imports "
                         "wasi:keyvalue/store@0.2.0-draft2: the route needs wasi=keyvalue")
             if crashed else None, "instance_ports": [], "port": None}
            for i in range(proc["instances"])]


def env_view(app_guid):
    services = {}
    for b in S["service_credential_bindings"].values():
        if rel_guid(b, "app") != app_guid:
            continue
        si = S["service_instances"][rel_guid(b, "service_instance")]
        plan = S["service_plans"][rel_guid(si, "service_plan")]
        off = S["service_offerings"][rel_guid(plan, "service_offering")]
        services.setdefault(off["name"], []).append({
            "name": si["name"], "label": off["name"], "plan": plan["name"], "tags": [],
            "instance_guid": si["guid"], "instance_name": si["name"], "binding_guid": b["guid"],
            "binding_name": None, "credentials": {"store": plan["name"],
                                                  "namespace": si["guid"]}})
    app = S["apps"][app_guid]
    return {"staging_env_json": {}, "running_env_json": {},
            "environment_variables": ENV[app_guid],
            "system_env_json": {"VCAP_SERVICES": services},
            "application_env_json": {"VCAP_APPLICATION": {
                "application_id": app_guid, "application_name": app["name"],
                "space_id": SPACE, "space_name": "wicket", "organization_id": ORG,
                "organization_name": "sluice", "cf_api": BASE, "limits": {},
                "uris": [r["url"] for r in S["routes"].values()
                         if any(d["app"]["guid"] == app_guid for d in r["destinations"])],
                "application_uris": []}}}


# ---- HTTP --------------------------------------------------------------------------------

def handle(method, path, query, body, headers):
    q = query
    # root and UAA
    if path in ("/", ""):
        return 200, {"links": {
            "self": {"href": BASE},
            "bits_service": None,
            "cloud_controller_v2": None,
            "cloud_controller_v3": {"href": BASE + "/v3", "meta": {"version": VERSION}},
            "network_policy_v0": None, "network_policy_v1": None,
            "login": {"href": UAA}, "uaa": {"href": UAA}, "credhub": None, "routing": None,
            "logging": None, "log_cache": {"href": LOG_CACHE}, "log_stream": None,
            "app_ssh": None}}
    if path == "/v3":
        return 200, {"links": {k: {"href": f"{BASE}/v3/{k}"} for k in (
            "apps", "domains", "organizations", "spaces", "routes", "service_instances")}}
    m = re.fullmatch(r"(/uaa)?/login", path)
    if m and method == "GET":
        return 200, {"app": {"version": "78.0.0"}, "showLoginLinks": True, "links": {
            "uaa": UAA, "passwd": "/forgot_password", "login": UAA, "register": "/create_account"},
            "zone_name": "uaa", "entityID": "cloudfoundry-saml-login",
            "commit_id": "spike", "idpDefinitions": {}, "prompts": {
                "username": ["text", "Email"], "password": ["password", "Password"],
                "passcode": ["password", f"Temporary Authentication Code ( Get one at {UAA}/passcode )"]},
            "timestamp": now()}
    if re.fullmatch(r"(/uaa)?/oauth/token", path) and method == "POST":
        form = {k: v[0] for k, v in parse_qs(body.decode()).items()}
        print(f"   token form: { {k: ('***' if 'pass' in k or 'secret' in k else v) for k, v in form.items()} }", file=sys.stderr)
        user = form.get("username") or "sso-user"
        if form.get("grant_type") == "refresh_token":
            claims = json.loads(base64.urlsafe_b64decode(
                form["refresh_token"].split(".")[1] + "==="))
            user = claims["user_name"]
        if form.get("grant_type") == "client_credentials":
            return 200, token(form.get("client_id", "client"), form.get("client_id", "client"),
                              "client_credentials")
        return 200, token(user)
    if re.fullmatch(r"(/uaa)?/token_keys", path):
        return 200, {"keys": []}
    # log cache
    m = re.fullmatch(r"(?:/logcache)?/api/v1/read/([^/]+)", path)
    if m:
        start = int((q.get("start_time") or ["0"])[0])
        batch = [{"timestamp": str(ns), "source_id": m[1], "instance_id": "0",
                  "tags": {"source_type": src},
                  "log": {"payload": base64.b64encode(line.encode()).decode(), "type": "OUT"}}
                 for ns, src, line in LOGS.get(m[1], []) if ns >= start]
        return 200, {"envelopes": {"batch": batch}}
    if re.fullmatch(r"(?:/logcache)?/api/v1/info", path):
        return 200, {"version": "3.0.0", "vm_uptime": "1"}

    # v3
    m = re.fullmatch(r"/v3/info", path)
    if m:
        return 200, {"build": "spike", "cli_version": {"minimum": "", "recommended": ""},
                     "custom": {}, "description": "sluice-wicket-cf-api spike",
                     "name": "sluice-wicket-cf-api", "version": 0, "osbapi_version": "2.15",
                     "links": {"self": {"href": BASE + "/v3/info"}, "support": {"href": ""}}}
    m = re.fullmatch(r"/v3/feature_flags/([^/]+)", path)
    if m:
        return 200, {"name": m[1], "enabled": True, "updated_at": None,
                     "custom_error_message": None, "links": {}}
    m = re.fullmatch(r"/v3/(organizations|spaces|domains|stacks|apps|processes|packages|builds|"
                     r"droplets|routes|service_brokers|service_offerings|service_plans|"
                     r"service_instances|service_credential_bindings)", path)
    if m and method == "GET":
        items = select(m[1], q)
        return 200, paginate(path, items, included_for(m[1], items, q))
    m = re.fullmatch(r"/v3/(organizations|spaces|domains|stacks|apps|processes|packages|builds|"
                     r"droplets|routes|service_brokers|service_offerings|service_plans|"
                     r"service_instances|service_credential_bindings|jobs)/([0-9a-f-]+)", path)
    if m and method == "GET":
        r = S[m[1]].get(m[2])
        if m[1] == "builds" and r and r["state"] == "STAGING":
            stage(r)
        return (200, r) if r else not_found(m[1][:-1])
    m = re.fullmatch(r"/v3/spaces/([^/]+)/manifest_diff", path)
    if m and method == "POST":
        diff = []
        for i, spec in enumerate(yaml.safe_load(body).get("applications", [])):
            if not any(a["name"] == spec["name"] for a in S["apps"].values()):
                diff.append({"op": "add", "path": f"/applications/{i}", "value": spec})
        return 201, {"diff": diff}
    m = re.fullmatch(r"/v3/packages/([^/]+)/droplets", path)
    if m:
        return 200, paginate(path, [d for d in S["droplets"].values()
                                    if d["links"]["package"]["href"].endswith(m[1])])
    if path == "/v3/deployments" and method == "POST":
        req = json.loads(body)
        app_guid = req["relationships"]["app"]["data"]["guid"]
        droplet = (req.get("droplet") or {}).get("guid") or CURRENT_DROPLET.get(app_guid)
        if req.get("droplet"):
            CURRENT_DROPLET[app_guid] = droplet
        S["apps"][app_guid]["state"] = "STARTED"
        log(app_guid, "CELL", f"Swapped in droplet {droplet[:8]} in place (no downtime)")
        g = new_guid()
        DEPLOYMENTS[g] = add("deployments", {
            "guid": g, "state": "DEPLOYING",
            "status": {"value": "ACTIVE", "reason": "DEPLOYING", "details": {}},
            "strategy": req.get("strategy", "rolling"), "options": req.get("options", {}),
            "droplet": {"guid": droplet}, "previous_droplet": {"guid": droplet},
            "new_processes": [], "revision": None,
            "relationships": {"app": rel(app_guid)}})
        return 201, DEPLOYMENTS[g]
    m = re.fullmatch(r"/v3/deployments/([^/]+)(/actions/cancel|/actions/continue)?", path)
    if m:
        d = DEPLOYMENTS.get(m[1])
        if d is None:
            return not_found("Deployment")
        if d["status"]["value"] == "ACTIVE":
            d["state"] = "DEPLOYED"  # one poll later: the wicket swaps at once
            d["status"] = {"value": "FINALIZED", "reason": "DEPLOYED", "details": {}}
        return 200, d
    if path == "/v3/deployments":
        return 200, paginate(path, [d for d in DEPLOYMENTS.values()
                                    if d["status"]["value"] in (csv(q, "status_values") or ["ACTIVE", "FINALIZED"])
                                    and rel_guid(d, "app") in (csv(q, "app_guids") or [rel_guid(d, "app")])])
    if path in ("/v3/buildpacks", "/v3/security_groups", "/v3/service_route_bindings",
                "/v3/audit_events", "/v3/isolation_segments", "/v3/users", "/v3/roles",
                "/v3/space_quotas", "/v3/organization_quotas"):
        return 200, paginate(path, [])
    m = re.fullmatch(r"/v3/organizations/([^/]+)/domains(/default)?", path)
    if m:
        return (200, default_domain()) if m[2] else (200, paginate(path, select("domains", q)))
    m = re.fullmatch(r"/v3/spaces/([^/]+)/actions/apply_manifest", path)
    if m and method == "POST":
        try:
            apply_manifest(body)
        except (ValueError, KeyError) as e:
            return accepted(job("app.apply_manifest", [
                {"code": 10008, "title": "CF-UnprocessableEntity", "detail": str(e)}]))
        return accepted(job("app.apply_manifest",
                            resource=("space", f"{BASE}/v3/spaces/{m[1]}")))
    m = re.fullmatch(r"/v3/spaces/([^/]+)/(features|running_security_groups|staging_security_groups)(/[^/]+)?", path)
    if m:
        if m[2] == "features":
            return 200, {"name": "ssh", "description": "", "enabled": False}
        return 200, paginate(path, [])
    if path == "/v3/apps" and method == "POST":
        req = json.loads(body)
        if any(a["name"] == req["name"] for a in S["apps"].values()):
            return error(422, 10016, "CF-UniquenessError", f"App with the name '{req['name']}' already exists.")
        app = create_app(req["name"], req.get("lifecycle"))
        ENV[app["guid"]].update(req.get("environment_variables") or {})
        return 201, app
    m = re.fullmatch(r"/v3/apps/([^/]+)(/.*)?", path)
    if m:
        app = S["apps"].get(m[1])
        if app is None:
            return not_found("App")
        g, rest = m[1], m[2] or ""
        if rest == "" and method == "DELETE":
            del S["apps"][g]
            for r in S["routes"].values():
                r["destinations"] = [d for d in r["destinations"] if d["app"]["guid"] != g]
            for b in list(S["service_credential_bindings"].values()):
                if rel_guid(b, "app") == g:
                    del S["service_credential_bindings"][b["guid"]]
            return accepted(job("app.delete"))
        if rest == "" and method == "PATCH":
            req = json.loads(body)
            for k in ("name", "lifecycle"):
                if k in req:
                    app[k] = req[k]
            return 200, app
        m2 = re.fullmatch(r"/actions/(start|stop|restart)", rest)
        if m2 and method == "POST":
            if m2[1] in ("start", "restart") and g not in CURRENT_DROPLET:
                return error(422, 10008, "CF-UnprocessableEntity",
                             "Assign a droplet before starting this app.")
            app["state"] = "STOPPED" if m2[1] == "stop" else "STARTED"
            app["updated_at"] = now()
            if m2[1] != "stop":
                log(g, "CELL", f"Deployed {S['droplets'][CURRENT_DROPLET[g]]['process_types']['web']} to the wicket")
            return 200, app
        if rest == "/processes":
            return 200, paginate(path, [p for p in S["processes"].values() if rel_guid(p, "app") == g])
        m2 = re.fullmatch(r"/processes/([^/]+)(/stats|/actions/scale)?", rest)
        if m2:
            proc = process(g, m2[1])
            if m2[2] == "/stats":
                return 200, {"resources": stats(proc)}
            if m2[2] == "/actions/scale":
                req = json.loads(body)
                proc["instances"] = req.get("instances", proc["instances"])
                proc["memory_in_mb"] = req.get("memory_in_mb", proc["memory_in_mb"])
                proc["disk_in_mb"] = req.get("disk_in_mb", proc["disk_in_mb"])
                return 202, proc
            return 200, proc
        if rest in ("/droplets/current", "/relationships/current_droplet"):
            if method == "PATCH":
                CURRENT_DROPLET[g] = json.loads(body)["data"]["guid"]
                return 200, {"data": {"guid": CURRENT_DROPLET[g]}, "links": {}}
            d = CURRENT_DROPLET.get(g)
            if d is None:
                return not_found("Droplet")
            return (200, S["droplets"][d]) if rest == "/droplets/current" else (
                200, {"data": {"guid": d}, "links": {}})
        if rest == "/droplets":
            return 200, paginate(path, [d for d in S["droplets"].values() if rel_guid(d, "app") == g])
        if rest == "/packages":
            return 200, paginate(path, [p for p in S["packages"].values() if rel_guid(p, "app") == g])
        if rest == "/routes":
            return 200, paginate(path, [r for r in S["routes"].values()
                                        if any(d["app"]["guid"] == g for d in r["destinations"])])
        if rest == "/env":
            return 200, env_view(g)
        if rest == "/environment_variables":
            if method == "PATCH":
                for k, v in json.loads(body)["var"].items():
                    if v is None:
                        ENV[g].pop(k, None)
                    else:
                        ENV[g][k] = v
            return 200, {"var": ENV[g], "links": {}}
        if rest == "/sidecars":
            return 200, paginate(path, [])
        if rest.startswith("/features"):
            if rest == "/features":
                return 200, {"resources": [{"name": "ssh", "description": "", "enabled": False},
                                           {"name": "revisions", "description": "", "enabled": False}],
                             "pagination": {"total_results": 2, "total_pages": 1}}
            return 200, {"name": rest.rsplit("/", 1)[1], "description": "", "enabled": False}
        if rest == "/ssh_enabled":
            return 200, {"enabled": False, "reason": "not supported"}
        if rest.startswith("/revisions"):
            return 200, paginate(path, [])
        if rest == "/manifest":
            return 200, "applications: []\n"
    m = re.fullmatch(r"/v3/processes/([^/]+)(/stats|/actions/scale|/sidecars)?", path)
    if m:
        proc = S["processes"].get(m[1])
        if proc is None:
            return not_found("Process")
        if m[2] == "/stats":
            return 200, {"resources": stats(proc)}
        if m[2] == "/sidecars":
            return 200, paginate(path, [])
        if m[2] == "/actions/scale" or method == "PATCH":
            req = json.loads(body)
            proc["instances"] = req.get("instances", proc["instances"])
            proc["memory_in_mb"] = req.get("memory_in_mb", proc["memory_in_mb"])
            proc["disk_in_mb"] = req.get("disk_in_mb", proc["disk_in_mb"])
            if "command" in req:
                proc["command"] = req["command"]
            return (202 if m[2] else 200), proc
    if path == "/v3/packages" and method == "POST":
        req = json.loads(body)
        g = new_guid()
        docker = req.get("type") == "docker"
        pkg = add("packages", {"guid": g, "type": req.get("type", "bits"), "data": (
            {"image": req["data"]["image"], "username": None, "password": None} if docker else {
                "checksum": {"type": "sha256", "value": None}, "error": None}),
            "state": "READY" if docker else "AWAITING_UPLOAD",
            "relationships": {"app": rel(req["relationships"]["app"]["data"]["guid"])}})
        pkg["links"].update({"upload": {"href": f"{BASE}/v3/packages/{g}/upload", "method": "POST"},
                             "download": {"href": f"{BASE}/v3/packages/{g}/download"}})
        return 201, pkg
    m = re.fullmatch(r"/v3/packages/([^/]+)/upload", path)
    if m and method == "POST":
        pkg = S["packages"][m[1]]
        msg = BytesParser(policy=email_policy).parsebytes(
            b"Content-Type: " + headers["Content-Type"].encode() + b"\r\n\r\n" + body)
        for part in msg.iter_parts():
            name = part.get_param("name", header="content-disposition")
            data = part.get_payload(decode=True)
            print(f"   part {name}: {len(data)} bytes {part.get_content_type()}"
                  + (f" {data[:300]!r}" if name == "resources" else ""), file=sys.stderr)
            if name == "bits":
                BITS[pkg["guid"]] = data
                try:
                    print(f"   zip entries: {zipfile.ZipFile(io.BytesIO(data)).namelist()}", file=sys.stderr)
                except zipfile.BadZipFile:
                    print("   bits: not a zip", file=sys.stderr)
        pkg["state"] = "READY"
        pkg["data"]["checksum"]["value"] = hashlib.sha256(BITS.get(pkg["guid"], b"")).hexdigest()
        return 200, pkg
    if path == "/v3/resource_matches" and method == "POST":
        return 201, {"resources": []}
    if path == "/v3/builds" and method == "POST":
        req = json.loads(body)
        pkg = S["packages"][req["package"]["guid"]]
        app = S["apps"][rel_guid(pkg, "app")]
        build = add("builds", {"guid": new_guid(), "state": "STAGING", "staging_memory_in_mb": 1024,
                               "staging_disk_in_mb": 1024, "staging_log_rate_limit_bytes_per_second": -1,
                               "error": None, "lifecycle": app["lifecycle"],
                               "package": {"guid": pkg["guid"]}, "droplet": None,
                               "created_by": {"guid": "", "name": "", "email": ""},
                               "relationships": {"app": rel(app["guid"])}})
        log(app["guid"], "STG", "Staging with the wasm lifecycle")
        return 201, build
    if path == "/v3/routes" and method == "POST":
        req = json.loads(body)
        domain_guid = req["relationships"]["domain"]["data"]["guid"]
        if find_route(req.get("host", ""), domain_guid, req.get("path", "")):
            return error(422, 10008, "CF-UnprocessableEntity", "Route already exists.")
        return 201, create_route(req.get("host", ""), domain_guid, req.get("path", ""))
    m = re.fullmatch(r"/v3/routes/([^/]+)(/destinations)?(/[^/]+)?", path)
    if m:
        route = S["routes"].get(m[1])
        if route is None:
            return not_found("Route")
        if not m[2] and method == "DELETE":
            del S["routes"][m[1]]
            return accepted(job("route.delete"))
        if m[2] and method == "POST":
            for d in json.loads(body)["destinations"]:
                map_route(route, d["app"]["guid"])
            return 200, {"destinations": route["destinations"], "links": {}}
        if m[2] and m[3] and method == "DELETE":
            route["destinations"] = [d for d in route["destinations"] if d["guid"] != m[3][1:]]
            return 204, None
        if m[2]:
            return 200, {"destinations": route["destinations"], "links": {}}
    if path == "/v3/service_instances" and method == "POST":
        req = json.loads(body)
        si = add("service_instances", {
            "guid": new_guid(), "name": req["name"], "type": req.get("type", "managed"),
            "tags": req.get("tags", []), "maintenance_info": {}, "upgrade_available": False,
            "dashboard_url": None, "last_operation": {"type": "create", "state": "succeeded",
                                                      "description": "", "created_at": now(),
                                                      "updated_at": now()},
            "relationships": {"service_plan": rel(req["relationships"].get("service_plan", {}).get("data", {}).get("guid")),
                              "space": rel(SPACE)}})
        return accepted(job("service_instance.create",
                            resource=("service_instances", f"{BASE}/v3/service_instances/{si['guid']}")))
    m = re.fullmatch(r"/v3/service_instances/([^/]+)(/.*)?", path)
    if m:
        si = S["service_instances"].get(m[1])
        if si is None:
            return not_found("Service instance")
        if method == "DELETE":
            del S["service_instances"][m[1]]
            return accepted(job("service_instance.delete"))
        if m[2] == "/parameters":
            return 200, {}
        if m[2] == "/relationships/shared_spaces":
            return 200, {"data": [], "links": {}}
        if m[2] == "/shared_spaces/usage_summary":
            return 200, {"usage_summary": [], "links": {}}
    if path == "/v3/service_credential_bindings" and method == "POST":
        req = json.loads(body)
        si = S["service_instances"][req["relationships"]["service_instance"]["data"]["guid"]]
        b = bind(si, req["relationships"]["app"]["data"]["guid"], req.get("parameters"))
        return accepted(job("service_bindings.create",
                            resource=("service_credential_binding", f"{BASE}/v3/service_credential_bindings/{b['guid']}")))
    m = re.fullmatch(r"/v3/service_credential_bindings/([^/]+)(/details|/parameters)?", path)
    if m:
        b = S["service_credential_bindings"].get(m[1])
        if b is None:
            return not_found("Service credential binding")
        if method == "DELETE":
            del S["service_credential_bindings"][m[1]]
            return accepted(job("service_bindings.delete"))
        if m[2] == "/details":
            return 200, {"credentials": {}, "syslog_drain_url": None, "volume_mounts": []}
        if m[2] == "/parameters":
            return 200, b["parameters"]
    return error(404, 10000, "CF-NotFound", f"Unknown request {method} {path}")


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def respond(self):
        url = urlparse(self.path)
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length) if length else b""
        if self.headers.get("Transfer-Encoding", "").lower() == "chunked":
            chunks = b""
            while True:
                size = int(self.rfile.readline().strip(), 16)
                if size == 0:
                    self.rfile.readline()
                    break
                chunks += self.rfile.read(size)
                self.rfile.readline()
            body = chunks
        query = parse_qs(url.query, keep_blank_values=True)
        with LOCK:
            try:
                result = handle(self.command, url.path, query, body, self.headers)
            except Exception as e:  # noqa: BLE001 -- a spike: show it
                import traceback
                traceback.print_exc()
                result = error(500, 10001, "CF-ServerError", repr(e))
        status, payload, extra = (result + (None,))[:3]
        excerpt = body[:400].decode(errors="replace").replace("\n", "\\n") if body and b"\0" not in body[:400] else (f"<{len(body)} bytes>" if body else "")
        print(f"{status} {self.command} {self.path} {excerpt}", file=sys.stderr, flush=True)
        if isinstance(payload, str):
            data, kind = payload.encode(), "application/x-yaml"
        else:
            data = b"" if payload is None else json.dumps(payload).encode()
            kind = "application/json"
        self.send_response(status)
        self.send_header("Content-Type", kind)
        self.send_header("Content-Length", str(len(data)))
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(data)

    do_GET = do_POST = do_PUT = do_PATCH = do_DELETE = respond


if __name__ == "__main__":
    print(f"stub on {BASE} (cc v3 {VERSION}, uaa {UAA}, log-cache {LOG_CACHE})", file=sys.stderr)
    ThreadingHTTPServer(("127.0.0.1", PORT), Handler).serve_forever()
