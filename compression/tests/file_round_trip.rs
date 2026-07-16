use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new() -> Self {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "compression-round-trip-{}-{unique}",
            std::process::id()
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn join(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn run_compression(mode: &str, input: &Path, output: &Path) {
    let result = Command::new(env!("CARGO_BIN_EXE_compression"))
        .args([mode])
        .arg(input)
        .arg(output)
        .output()
        .unwrap();

    assert!(
        result.status.success(),
        "{mode} failed with status {}\nstdout:\n{}\nstderr:\n{}",
        result.status,
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
}

#[test]
fn test_txt_survives_a_complete_file_round_trip() {
    let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("test.txt");
    assert!(
        source.is_file(),
        "missing test fixture: {}",
        source.display()
    );

    let directory = TestDirectory::new();
    let compressed = directory.join("test.huff");
    let restored = directory.join("test-restored.txt");
    let recompressed = directory.join("test-recompressed.huff");

    run_compression("encode", &source, &compressed);
    run_compression("decode", &compressed, &restored);
    run_compression("encode", &restored, &recompressed);

    let original_bytes = fs::read(&source).unwrap();
    let restored_bytes = fs::read(&restored).unwrap();
    assert_eq!(restored_bytes, original_bytes);

    let compressed_bytes = fs::read(&compressed).unwrap();
    assert_eq!(&compressed_bytes[..4], b"HUFF");
    assert!(
        compressed_bytes.len() < original_bytes.len(),
        "expected the fixture to compress: input={} bytes, output={} bytes",
        original_bytes.len(),
        compressed_bytes.len()
    );

    let recompressed_bytes = fs::read(&recompressed).unwrap();
    if recompressed_bytes != compressed_bytes {
        let first_difference = recompressed_bytes
            .iter()
            .zip(&compressed_bytes)
            .position(|(recompressed, original)| recompressed != original);
        panic!(
            "re-encoding changed the encoded file: first_difference={first_difference:?}, original_size={}, recompressed_size={}",
            compressed_bytes.len(),
            recompressed_bytes.len()
        );
    }
}
