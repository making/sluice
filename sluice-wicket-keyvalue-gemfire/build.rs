// Compiles the descriptions of the GemFire service and of the locators' discovery: the
// clients, and with the mock (feature `mock`) the servers too. The values are `Bytes`.
fn main() {
    let mock = std::env::var_os("CARGO_FEATURE_MOCK").is_some();
    tonic_prost_build::configure()
        .build_server(mock)
        .bytes(".gemfire.v1")
        .compile_protos(
            &[
                "proto/gemfire/v1/gemfire.proto",
                "proto/envoy/service/discovery/v3/discovery.proto",
                "proto/envoy/config/endpoint/v3/endpoint.proto",
            ],
            &["proto"],
        )
        .expect("failed to compile the protos");
}
