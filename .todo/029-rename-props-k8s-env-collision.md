Difficulty: Low

# Rename properties that collide with k8s service-link env vars

With `enableServiceLinks` on (k8s default), the kubelet injects `<SERVICE>_*` env vars
that Spring relaxed-binding matches against `sluice.*` properties. Observed 2026-10-09
(`kind`, service `sluice-data`): `SLUICE_DATA_PORT=tcp://10.96.213.125:8000` binds to
`sluice.data-port` (int) and crashes the server at startup
(`NumberFormatException: For input string: "tcp://..."`). `k8s.md` currently works
around it with `enableServiceLinks: false` in the StatefulSet; the helm path is
exposed the same way (chart default `enableServiceLinks: true`, release/service name
`sluice-server`).

- Audit `SluiceServerProperties` (and client properties) for names matchable by the
  service-link patterns `<name>_PORT`, `<name>_SERVICE_HOST`, `<name>_PORT_<port>_<proto>`
  for plausible service names (`sluice-server`, `sluice-data`, ...)
- Rename the colliding ones (known: `sluice.data-port`); prefer segmenting that no
  service-link var can produce, e.g. `sluice.data.listen-port` -- breaking change, so
  cover the alias/compat question explicitly (Boot 4 has no deprecated property
  reporting by default; decide: clean break vs `@DeprecatedConfigurationProperty`)
- Update README, `k8s.md`, and the wasmlet flags if they mirror the names
- Add a test binding the properties from an env-style map containing realistic
  service-link vars (`SLUICE_DATA_PORT=tcp://...`) so the collision stays covered
- Revisit `k8s.md`: if the rename removes the clash, the `enableServiceLinks: false`
  workaround (and its comment) can be dropped; keep whatever the audit concludes

Related: 028 (same "k8s runtime env vs static assumptions" family, different mechanism).
