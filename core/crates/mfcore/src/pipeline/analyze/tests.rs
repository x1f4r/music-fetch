use super::*;

#[test]
fn validates_links_and_files() {
    assert_eq!(
        validate("  check this https://youtu.be/dQw4w9WgXcQ?si=x out "),
        Ok(AnalyzeInput::Url("https://youtu.be/dQw4w9WgXcQ?si=x".into()))
    );
    assert_eq!(validate("youtu.be/dQw4w9WgXcQ"), Ok(AnalyzeInput::Url("https://youtu.be/dQw4w9WgXcQ".into())));
    assert!(matches!(validate(""), Err(MfError::InvalidInput { .. })));
    assert!(matches!(validate("   "), Err(MfError::InvalidInput { .. })));
    assert!(matches!(validate("just some words"), Err(MfError::InvalidInput { .. })));

    let dir = std::env::temp_dir().join(format!("mf-validate-{}", crate::util::new_id()));
    std::fs::create_dir_all(&dir).unwrap();
    let file = dir.join("My Song.mp3");
    std::fs::write(&file, b"x").unwrap();
    assert_eq!(validate(file.to_str().unwrap()), Ok(AnalyzeInput::File(file.clone())));
    let url = format!("file://{}", file.to_str().unwrap().replace(' ', "%20"));
    assert_eq!(validate(&url), Ok(AnalyzeInput::File(file.clone())));
    assert!(matches!(validate(dir.to_str().unwrap()), Err(MfError::InvalidInput { .. })));
    assert!(matches!(validate(dir.join("missing.mp3").to_str().unwrap()), Err(MfError::InvalidInput { .. })));
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn durations_within_tolerance() {
    assert!(within(212_000, 213_000, 0.12));
    assert!(within(240_000, 213_000, 0.13));
    assert!(!within(357_000, 822_000, 0.12));
}

#[test]
fn source_refresh_from_probe() {
    let mut source = SourceInfo { url: Some("https://example.com/v".into()), ..SourceInfo::default() };
    let info = MediaInfo {
        title: Some("T".into()),
        uploader: Some("U".into()),
        duration_ms: Some(1000),
        is_playlist: true,
        entries: vec![crate::media::MediaEntry::default(); 3],
        ..MediaInfo::default()
    };
    refresh_source(&mut source, &info);
    assert_eq!(source.title.as_deref(), Some("T"));
    assert_eq!(source.item_count, 3);
    assert_eq!(source.url.as_deref(), Some("https://example.com/v"));
}
