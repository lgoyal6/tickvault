use sha2::{Digest, Sha256};
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

fn collect(path: &Path, files: &mut Vec<PathBuf>) {
    println!("cargo:rerun-if-changed={}", path.display());
    for entry in fs::read_dir(path)
        .expect("source directory")
        .map(Result::unwrap)
    {
        let path = entry.path();
        if entry.file_type().expect("file type").is_dir() {
            collect(&path, files);
        } else if path.extension().is_some_and(|s| s == "rs" || s == "toml") {
            files.push(path);
        }
    }
}
fn git(root: &Path, args: &[&str]) -> Option<String> {
    let output = Command::new("git")
        .current_dir(root)
        .args(args)
        .output()
        .ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
}
fn main() {
    let root = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap())
        .join("../..")
        .canonicalize()
        .unwrap();
    let mut files = vec![
        root.join("Cargo.toml"),
        root.join("Cargo.lock"),
        root.join("rust-toolchain.toml"),
    ];
    for dir in ["src", "crates"] {
        collect(&root.join(dir), &mut files);
    }
    files.sort();
    let mut hash = Sha256::new();
    // Length framing prevents ambiguous concatenations; new untracked source
    // participates too, so a dirty workspace cannot borrow the commit's cache.
    for path in files {
        println!("cargo:rerun-if-changed={}", path.display());
        let name = path.strip_prefix(&root).unwrap().to_string_lossy();
        let bytes = fs::read(&path).expect("read build inputs");
        hash.update((name.len() as u64).to_le_bytes());
        hash.update(name.as_bytes());
        hash.update((bytes.len() as u64).to_le_bytes());
        hash.update(bytes);
    }
    let sha = git(&root, &["rev-parse", "HEAD"]).unwrap_or_else(|| "unknown".into());
    let dirty = git(&root, &["status", "--porcelain", "--untracked-files=all"])
        .is_none_or(|s| !s.is_empty());
    println!(
        "cargo:rerun-if-changed={}",
        root.join(".git/HEAD").display()
    );
    if let Some(reference) = git(&root, &["symbolic-ref", "-q", "HEAD"]) {
        println!(
            "cargo:rerun-if-changed={}",
            root.join(".git").join(reference).display()
        );
    }
    println!(
        "cargo:rustc-env=TICKVAULT_SOURCE_HASH={}",
        hash.finalize()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>()
    );
    println!("cargo:rustc-env=TICKVAULT_GIT_SHA={sha}");
    println!("cargo:rustc-env=TICKVAULT_DIRTY={dirty}");
}
