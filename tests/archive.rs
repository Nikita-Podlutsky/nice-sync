use anyhow::Result;
use nice_sync::store::Store;
use std::fs;

#[test]
fn archives_changes_idempotently_and_keeps_deleted_sources() -> Result<()> {
    let temporary = tempfile::tempdir()?;
    let source = temporary.path().join("camera");
    let state = temporary.path().join("archive");
    let photo = source.join("photo.jpg");
    fs::create_dir(&source)?;
    fs::write(&photo, "first")?;
    let mut archive = Store::open(&state)?;
    assert_eq!(archive.index(&source)?, 1);
    assert_eq!(archive.index(&source)?, 0);
    fs::write(&photo, "second")?;
    assert_eq!(archive.index(&source)?, 1);
    fs::remove_file(&photo)?;
    assert_eq!(archive.index(&source)?, 0);
    assert!(Store::open(&state).is_err());
    drop(archive);
    let archive = Store::open(&state)?;
    let restored = temporary.path().join("restored.jpg");
    archive.restore("photo.jpg", Some(1), &restored)?;
    assert_eq!(fs::read_to_string(&restored)?, "first");
    archive.restore("photo.jpg", None, &restored)?;
    assert_eq!(fs::read_to_string(&restored)?, "second");
    assert_eq!(archive.version("photo.jpg", None)?.parent, Some(1));
    assert!(archive.version("../photo.jpg", None).is_err());
    Ok(())
}
