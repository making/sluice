# Installing sluice-server on Kubernetes (kind)

Deploys `sluice-server` with the [stakater/application](https://github.com/stakater/application) Helm chart.
Verified against kind (Kubernetes 1.37) with MetalLB providing routable `LoadBalancer` IPs, chart `9.3.2`,
image `ghcr.io/making/sluice/sluice-server:native`.

## Exposed ports

| Port | Purpose |
|------|---------|
| 8001 | gRPC control plane (clients connect here) |
| 8000 | data plane (raw TCP, routed by Host header) |
| 8081 | actuator + console |

## values.yaml

```yaml
applicationName: sluice-server

deployment:
  image:
    repository: ghcr.io/making/sluice/sluice-server
    tag: native
    pullPolicy: Always
  resources:
    limits:
      memory: 1Gi
  env:
    SLUICE_TOKEN:
      value: SECRET # tunnel token; clients must pass the same value
  readinessProbe:
    enabled: true
    httpGet:
      path: /actuator/health/readiness
      port: 8081
    periodSeconds: 5
  livenessProbe:
    enabled: true
    httpGet:
      path: /actuator/health/liveness
      port: 8081
    periodSeconds: 10
  # the chart defaults to a read-only root filesystem; Tomcat needs a writable
  # temp dir, so back /tmp with an emptyDir
  containerSecurityContext:
    runAsNonRoot: true
  volumes:
    tmp:
      emptyDir: {}
  volumeMounts:
    tmp:
      mountPath: /tmp

service:
  type: LoadBalancer # MetalLB assigns a routable IP
  ports:
    - name: grpc
      port: 8001
      protocol: TCP
      targetPort: 8001
    - name: data
      port: 8000
      protocol: TCP
      targetPort: 8000
    - name: http
      port: 8081
      protocol: TCP
      targetPort: 8081
```

## Install

```sh
kubectl create namespace sluice

# only needed when the image is not yet pushed and was built locally:
kind load docker-image ghcr.io/making/sluice/sluice-server:native --name kind

helm upgrade --install sluice-server oci://ghcr.io/stakater/charts/application \
  --version 9.3.2 \
  --namespace sluice \
  --values /tmp/sluice-k8s/values.yaml
```

## Verify

```sh
kubectl get pods -n sluice
kubectl get svc -n sluice sluice-server -o jsonpath='{.status.loadBalancer.ingress[0].ip}'

# readiness via the LoadBalancer IP
curl http://<LB-IP>:8081/actuator/health/readiness
```

## Run the client

Point the client at the LoadBalancer IP; it reaches the gRPC control plane on 8001 and the
data plane is exposed on the same IP.

```sh
# fat jar
java -jar sluice-client/target/sluice-client-0.0.1-SNAPSHOT-exec.jar \
  --sluice.server-url=grpc://<LB-IP>:8001 \
  '--sluice.client.upstream[0]'.host=demo.local \
  '--sluice.client.upstream[0]'.target=http://127.0.0.1:31080 \
  --sluice.token=SECRET

# or the native binary
sluice-client/target/sluice-client \
  --sluice.server-url=grpc://<LB-IP>:8001 ... 
```

## Request through the tunnel

```sh
curl -H 'Host: demo.local' http://<LB-IP>:8000/
curl --http2-prior-knowledge -H 'Host: demo.local' http://<LB-IP>:8000/
```

## Scale-out (multiple nodes)

Every client keeps one tunnel stream to **every** node: the server publishes the member
list to connected clients, and each node holds the full route table locally. Clients
normally run outside the cluster, so every membership URL must be resolvable where the
clients are -- each node gets its own LoadBalancer Service with a pinned
`loadBalancerIP`, and `sluice.cluster.nodes` references those addresses. Membership is
static, so `spec.replicas` and the `sluice.cluster.nodes` env below must be kept in
sync; scaling means editing both and re-applying.

Use the `jvm` image for multi-node: the `native` image bakes `@ConditionalOnProperty`
bean conditions at build time (Spring AOT), so its static membership provider never
sees runtime `sluice.cluster.nodes` and silently stays single-node.

```yaml
apiVersion: v1
kind: Service
metadata:
  name: sluice-headless
  namespace: sluice
  labels:
    app: sluice-server
spec:
  clusterIP: None
  selector:
    app: sluice-server
  ports:
    - name: grpc
      port: 8001
      protocol: TCP
      targetPort: 8001
---
# per-node control plane: one LB per pod, pinned into the MetalLB pool so the
# membership env below can reference the addresses
apiVersion: v1
kind: Service
metadata:
  name: sluice-node-0
  namespace: sluice
spec:
  type: LoadBalancer
  loadBalancerIP: 192.168.107.210
  selector:
    app: sluice-server
    statefulset.kubernetes.io/pod-name: sluice-server-0
  ports:
    - name: grpc
      port: 8001
      protocol: TCP
      targetPort: 8001
---
apiVersion: v1
kind: Service
metadata:
  name: sluice-node-1
  namespace: sluice
spec:
  type: LoadBalancer
  loadBalancerIP: 192.168.107.211
  selector:
    app: sluice-server
    statefulset.kubernetes.io/pod-name: sluice-server-1
  ports:
    - name: grpc
      port: 8001
      protocol: TCP
      targetPort: 8001
---
# shared data plane entry: any node answers
apiVersion: v1
kind: Service
metadata:
  name: sluice-data
  namespace: sluice
spec:
  type: LoadBalancer
  loadBalancerIP: 192.168.107.212
  selector:
    app: sluice-server
  ports:
    - name: data
      port: 8000
      protocol: TCP
      targetPort: 8000
---
# console + actuator
apiVersion: v1
kind: Service
metadata:
  name: sluice-console
  namespace: sluice
spec:
  type: LoadBalancer
  loadBalancerIP: 192.168.107.213
  selector:
    app: sluice-server
  ports:
    - name: http
      port: 8081
      protocol: TCP
      targetPort: 8081
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: sluice-server
  namespace: sluice
spec:
  serviceName: sluice-headless
  replicas: 2 # keep in sync with sluice.cluster.nodes below
  selector:
    matchLabels:
      app: sluice-server
  template:
    metadata:
      labels:
        app: sluice-server
    spec:
      # service-link env vars (SLUICE_DATA_PORT=tcp://...) collide with the
      # sluice.data-port property binding and crash the server at startup
      enableServiceLinks: false
      containers:
        - name: sluice-server
          image: ghcr.io/making/sluice/sluice-server:jvm
          imagePullPolicy: Always
          resources:
            limits:
              memory: 1Gi
          env:
            - name: sluice.token
              value: SECRET
            - name: sluice.node.id
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            - name: sluice.cluster.nodes
              value: "sluice-server-0=grpc://192.168.107.210:8001,sluice-server-1=grpc://192.168.107.211:8001"
          ports:
            - name: grpc
              containerPort: 8001
            - name: data
              containerPort: 8000
            - name: http
              containerPort: 8081
          readinessProbe:
            httpGet:
              path: /actuator/health/readiness
              port: 8081
            periodSeconds: 5
          livenessProbe:
            httpGet:
              path: /actuator/health/liveness
              port: 8081
            periodSeconds: 10
          volumeMounts:
            - name: tmp
              mountPath: /tmp
      volumes:
        - name: tmp
          emptyDir: {}
```

Notes:

- Replace the single-node release when switching (`helm uninstall sluice-server -n sluice`).
- Env vars may use the plain property names (`sluice.token`) -- no need for
  `UPPER_SNAKE`; the values are plain strings and Spring binds them as-is.
- Clients only need one entry point: the bootstrap `--sluice.server-url` (any node LB);
    the rest of the membership arrives via the control plane. The data plane LB is the
    front for ordinary HTTP traffic (`Host` header routing); any node answers.
- In-cluster-only clients could use the headless pod DNS in `sluice.cluster.nodes`
  instead of pinned LB IPs.
- A dynamic membership provider (DNS SRV) is planned; once it lands the env goes away
  and `kubectl scale` is enough.

Verify two tunnels on the client, then request through the data LB:

```sh
kubectl logs -n sluice <client-pod> | grep 'tunnel established'
# [grpc://192.168.107.210:8001] tunnel established; ...
# [sluice-server-1] tunnel established; ...

curl -H 'Host: probe.local' http://192.168.107.212:8000/
curl --http2-prior-knowledge -H 'Host: probe.local' http://192.168.107.212:8000/
```

## Console

Exposed on the `sluice-console` LoadBalancer (see the scale-out section; the
single-node release serves it on the `http` port of `sluice-server`):

```sh
open http://192.168.107.213:8081/console # admin / admin by default
```

Without an LB, port-forward works as well:

```sh
kubectl port-forward -n sluice svc/sluice-console 8081:8081
```

## Uninstall

```sh
helm uninstall sluice-server -n sluice
```
