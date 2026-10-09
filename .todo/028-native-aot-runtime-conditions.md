Difficulty: Medium

# Runtime-safe bean conditions for the native image

Spring AOT evaluates `@ConditionalOnProperty` / `@ConditionalOnMissingBean` on `@Bean`
methods at BUILD time and bakes the result into the generated bean definitions
(`target/spring-aot/main/.../__BeanDefinitions.java`). Verified on 2026-10-09: the
`sluice-server:native` image ships WITHOUT the static `NodeDirectory` bean, so runtime
`sluice.cluster.nodes` (env `SLUICE_CLUSTER_NODES` or `--sluice.cluster.nodes=...`) is
ignored and the node silently stays single-node (membership version 0 fallback). The
`jvm` image is unaffected; single-node native deployments are unaffected.

- Replace the build-time conditionals in `server/cluster/NodeDirectoryConfiguration`
  with a single `@Bean` that decides from `SluiceServerProperties` at runtime (static
  list vs fallback), keeping an extension point for 009's SRV provider
- Audit other runtime-toggled `@ConditionalOnProperty` bean methods in server / client
  for the same freeze (access control, proxy-protocol, console auth, tls-bundle paths)
- Verify with the native build (`-Pnative`) after the change; a plain unit test cannot
  catch the AOT freeze
- After the fix, switch the `k8s.md` scale-out section back to the `native` image

Related: 009 -- its provider selection must be runtime-decided for the same reason.
