use std::fs;

fn main() {
    let epoch_broadcaster_out_dir = "target/epoch_broadcaster";
    fs::create_dir_all(epoch_broadcaster_out_dir).unwrap();
    tonic_build::configure()
        .build_server(true)
        .out_dir(epoch_broadcaster_out_dir)
        .compile(
            &["src/epoch_broadcaster.proto"],
            &["src"], // specify the root location to search proto dependencies
        )
        .unwrap();

    let range_server_out_dir = "target/rangeserver";
    fs::create_dir_all(range_server_out_dir).unwrap();
    tonic_build::configure()
        .build_server(true)
        .out_dir(range_server_out_dir)
        .compile(
            &["src/rangeserver.proto"],
            &["src"], // specify the root location to search proto dependencies
        )
        .unwrap();

    let warden_out_dir = "target/warden";
    fs::create_dir_all(warden_out_dir).unwrap();
    tonic_build::configure()
        .build_server(true)
        .out_dir(warden_out_dir)
        .compile(
            &["src/warden.proto"],
            &["src"], // specify the root location to search proto dependencies
        )
        .unwrap();
}
