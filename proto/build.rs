fn main() {
    tonic_build::configure()
        .build_server(true)
        .out_dir("target/warden")
        .compile(
            &["src/warden.proto"],
            &["src"], // specify the root location to search proto dependencies
        )
        .unwrap();
}
