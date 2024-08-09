use flatc_rust;

use std::path::Path;

fn main() {
    let epoch_broadcaster_out_dir = "target/epoch_broadcaster/";
    println!("cargo:rerun-if-changed=src/epoch_broadcaster/schema.fbs");
    flatc_rust::run(flatc_rust::Args {
        inputs: &[Path::new("src/epoch_broadcaster/schema.fbs")],
        out_dir: Path::new(epoch_broadcaster_out_dir),
        ..Default::default()
    })
    .expect("flatc");
    println!("cargo:rerun-if-changed=src/rangeserver/schema.fbs");
    let range_server_out_dir = "target/rangeserver/";
    flatc_rust::run(flatc_rust::Args {
        inputs: &[Path::new("src/rangeserver/schema.fbs")],
        out_dir: Path::new(range_server_out_dir),
        ..Default::default()
    })
    .expect("flatc");
}
