use flatc_rust;
use std::path::Path;

fn main() {
    //let out_dir = env::var("OUT_DIR").unwrap();
    let out_dir = "src";
    let out_path = format!("{}/", out_dir);
    println!(
        "{}",
        format!(
            "cargo:rerun-if-changed={}/a19_messaging/flat_buffers/message.fbs",
            out_dir
        )
    );
    flatc_rust::run(flatc_rust::Args {
        inputs: &[Path::new("flat_buffers/message.fbs")],
        out_dir: Path::new(&out_path),
        ..Default::default()
    })
    .expect("flatc");
}
