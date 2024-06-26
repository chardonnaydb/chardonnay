use flatc_rust;

use std::path::Path;

fn main() {
    println!("cargo:rerun-if-changed=src/rangeserver/schema.fbs");
    flatc_rust::run(flatc_rust::Args {
        inputs: &[Path::new("src/rangeserver/schema.fbs")],
        out_dir: Path::new("target/rangeserver/"),
        ..Default::default()
    }).expect("flatc");
}