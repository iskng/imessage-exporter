fn main() {
    // Ensure a known-good protoc is available
    let protoc = protoc_bin_vendored::protoc_bin_path().expect("failed to find protoc");
    std::env::set_var("PROTOC", protoc);

    let mut config = prost_build::Config::new();
    // Derive serde for generated types to satisfy WireRecord bounds
    config.type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]");
    // Keep package module paths
    config.compile_protos(
        &["protos/imessage_record.proto"],
        &["protos"],
    ).expect("failed to compile protos");
}
