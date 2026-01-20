use std::env;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    
    // Compile protobuf definitions
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .file_descriptor_set_path(out_dir.join("pulse_descriptor.bin"))
        .compile(&["../proto/pulse.proto"], &["../proto"])?;
    
    // Recompile if proto files change
    println!("cargo:rerun-if-changed=../proto/pulse.proto");
    
    Ok(())
}
