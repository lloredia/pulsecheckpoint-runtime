use std::env;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(env::var("OUT_DIR")?);
    tonic_prost_build::configure()
        .file_descriptor_set_path(out_dir.join("pulse_descriptor.bin"))
        .compile_protos(&["../proto/pulse.proto"], &["../proto"])?;
    println!("cargo:rerun-if-changed=../proto/pulse.proto");
    Ok(())
}
