// Embeds the build identification (git commit, dirty flag, build time).
fn main() {
    sluice_build_info::embed(&["build.rs", "Cargo.toml", "src"]);
}
