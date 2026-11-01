// Embeds the build identification (git commit, dirty flag, build time); the tunnel's and the
// flume's messages are sluice-wicket-tunnel's.
fn main() {
    sluice_build_info::embed(&["build.rs", "Cargo.toml", "src"]);
}
