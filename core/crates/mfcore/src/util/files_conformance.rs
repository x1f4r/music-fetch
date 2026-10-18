// Included by util/files.rs (unit tests) and tests/wasm_store.rs (browsers).

/// Checks every [`FileStore`] backend must pass. Uses only `FileStore`, which
/// the including module brings into scope.
#[allow(dead_code)]
pub(crate) fn exercise_store(files: &dyn FileStore) {
    assert_eq!(files.len("a/b.bin").unwrap(), None);
    assert!(files.list("").unwrap().is_empty());

    files.write("a/b.bin", b"hello world").unwrap();
    assert_eq!(files.len("a/b.bin").unwrap(), Some(11));
    assert_eq!(files.read("a/b.bin").unwrap(), b"hello world");
    let mut buf = [0u8; 5];
    assert_eq!(files.read_at("a/b.bin", 6, &mut buf).unwrap(), 5);
    assert_eq!(&buf, b"world");
    assert_eq!(files.read_at("a/b.bin", 9, &mut buf).unwrap(), 2);
    assert_eq!(files.read_at("a/b.bin", 11, &mut buf).unwrap(), 0);
    assert_eq!(files.read_at("a/b.bin", 99, &mut buf).unwrap(), 0);

    // Positional writes leave zeros in gaps; appends continue at the end.
    let mut w = files.create("a/c.bin").unwrap();
    w.write_at(4, b"xy").unwrap();
    w.append(b"z").unwrap();
    w.write_at(0, b"q").unwrap();
    assert_eq!(w.len(), 7);
    assert_eq!(w.finish().unwrap(), 7);
    assert_eq!(files.read("a/c.bin").unwrap(), b"q\0\0\0xyz");

    // Large files cross chunk boundaries (the SQLite backend stores 4 MiB chunks).
    let big: Vec<u8> = (0..9 * 1024 * 1024 + 123).map(|i: u32| (i.wrapping_mul(2_654_435_761) >> 13) as u8).collect();
    let mut w = files.create("big/x.pcm").unwrap();
    for part in big.chunks(300_001) {
        w.append(part).unwrap();
    }
    assert_eq!(w.finish().unwrap(), big.len() as u64);
    assert_eq!(files.len("big/x.pcm").unwrap(), Some(big.len() as u64));
    let mut window = vec![0u8; 1_000_000];
    let at = 4 * 1024 * 1024 - 500_000;
    assert_eq!(files.read_at("big/x.pcm", at as u64, &mut window).unwrap(), window.len());
    assert_eq!(window, big[at..at + window.len()]);
    assert_eq!(files.read("big/x.pcm").unwrap(), big);

    // Replacing a file drops the old contents.
    files.write("big/x.pcm", b"short").unwrap();
    assert_eq!(files.read("big/x.pcm").unwrap(), b"short");

    files.rename("a/c.bin", "a/d.bin").unwrap();
    assert_eq!(files.len("a/c.bin").unwrap(), None);
    assert_eq!(files.read("a/d.bin").unwrap(), b"q\0\0\0xyz");
    files.rename("a/d.bin", "a/b.bin").unwrap();
    assert_eq!(files.read("a/b.bin").unwrap(), b"q\0\0\0xyz");

    files.write("a/e.bin", b"1").unwrap();
    files.write("ab.bin", b"22").unwrap();
    assert_eq!(
        files.list("a/").unwrap(),
        vec![("a/b.bin".to_string(), 7), ("a/e.bin".to_string(), 1)],
        "a prefix with a slash matches the folder only"
    );
    assert_eq!(files.total_len("a").unwrap(), 10);
    assert_eq!(files.remove_prefix("a/").unwrap(), 2);
    assert_eq!(files.list("").unwrap(), vec![("ab.bin".to_string(), 2), ("big/x.pcm".to_string(), 5)]);
    assert!(files.remove("ab.bin").unwrap());
    assert!(!files.remove("ab.bin").unwrap());

    for bad in ["", "/abs", "a/../b", "a//b", "a/", "a\\b", "./a"] {
        assert!(files.create(bad).is_err(), "{bad:?}");
    }
}
