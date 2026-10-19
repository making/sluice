Difficulty: Low

# Remove the deprecated `sluice.proxy-protocol` alias

`sluice.proxy-protocol` was renamed to `sluice.data-proxy-protocol` (next to
`sluice.control-proxy-protocol`). `RenamedPropertiesEnvironmentPostProcessor` still carries the
former name over to the current one, with a deprecation warning at startup, so deployments keep
working until they switch.

Precondition: no deployment sets the former name any more. Known user: the lemon server
(`k8s-gitops/lemon/platform/sluice-server/helm-values.yaml`, env `sluice.proxy-protocol`); switch it
to `sluice.data-proxy-protocol` and confirm the startup log has no
`sluice.proxy-protocol is deprecated` line before removing the alias.

- Drop the entry from `RenamedPropertiesEnvironmentPostProcessor.RENAMED`. If none is left, delete
  the post-processor, its test and `META-INF/spring.factories` (keep them if todo 029's renames have
  added entries by then)
- README / `application.properties`: drop the "formerly `sluice.proxy-protocol`" notes; the
  `SluiceServerProperties` javadoc likewise
- A server started with only `sluice.proxy-protocol=true` then parses no PROXY header: say so in the
  commit message (breaking for anyone left on the old name)
