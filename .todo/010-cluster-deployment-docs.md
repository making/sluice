Difficulty: Medium

# HAProxy front-end example for the cluster deployment

The general cluster mode is documented in README ("Cluster (scale-out)"). This todo adds the
concrete HAProxy front end that satisfies its two deployment requirements:

- SNI routing of the per-node control plane (`sluice-N.tunnel.example.com` -> node N :8001)
  without terminating TLS, bootstrap host to any node, everything else to the data plane
  ports
- data plane / tcp route backends following the node list via DNS SRV
  (`server-template ... _grpc._tcp.<headless-service>`), health checking the readiness
  endpoint, `preStop` drain, `terminationGracePeriodSeconds` covering in-flight connections

Verify the HAProxy directives against the version in service before merging the example:

- `server-template` with SRV records + `resolvers`
- `do-resolve` / `set-dst` / `set-var ... req.ssl_sni,regsub(...)` in tcp-request content
  (the no-per-ordinal variant of the control plane backend)
- port-range `bind :9000-9010` with portless `server` lines

State in the doc that a ConfigMap change restarts HAProxy and drops tunnel streams (clients
reconnect); mitigate with several replicas + rolling updates and a static config.

Superseded the deployment example sections of the former todo 007; the k8s manifests
(StatefulSet / headless Service / LoadBalancer for HAProxy itself) are part of this example.
