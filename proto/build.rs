fn main() {
    tonic_build::configure()
        .build_server(true)
        .out_dir("target/warden")
        .compile(
            &["src/warden.proto", "src/prefetch.proto"],
            &["src"], // specify the root location to search proto dependencies
        )
        .unwrap();
}
