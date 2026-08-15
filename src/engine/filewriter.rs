use std::path::PathBuf;

use super::fsutil;

/// Mirrors each parameter onto disk as one file per parameter, under `root`, following
/// the parameter's own path (e.g. `/myapp/prod/db/password` -> `<root>/myapp/prod/db/password`).
pub struct FileWriter {
    root: PathBuf,
    dir_mode: u32,
    file_mode: u32,
}

impl FileWriter {
    pub fn new(root: PathBuf, dir_mode: u32, file_mode: u32) -> Self {
        Self {
            root,
            dir_mode,
            file_mode,
        }
    }

    pub fn path_for(&self, name: &str) -> anyhow::Result<PathBuf> {
        validate_name(name)?;
        Ok(self.root.join(name.trim_start_matches('/')))
    }

    pub fn write(&self, name: &str, value: &[u8]) -> anyhow::Result<()> {
        let path = self.path_for(name)?;
        let parent = path
            .parent()
            .ok_or_else(|| anyhow::anyhow!("path has no parent directory: {}", path.display()))?;
        fsutil::ensure_dir_all(parent, self.dir_mode)?;
        fsutil::write_atomic(&path, value, self.file_mode)?;
        Ok(())
    }

    pub fn remove(&self, name: &str) -> anyhow::Result<()> {
        let path = self.path_for(name)?;
        match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err.into()),
        }
    }
}

fn validate_name(name: &str) -> anyhow::Result<()> {
    if !name.starts_with('/') {
        anyhow::bail!("parameter name {name:?} must be absolute (start with '/')");
    }
    if name.split('/').any(|segment| segment == "..") {
        anyhow::bail!("parameter name {name:?} must not contain '..' segments");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn maps_parameter_path_under_root() {
        let dir = tempfile::tempdir().unwrap();
        let writer = FileWriter::new(dir.path().to_path_buf(), 0o700, 0o600);
        let path = writer.path_for("/myapp/prod/db/password").unwrap();
        assert_eq!(path, dir.path().join("myapp/prod/db/password"));
    }

    #[test]
    fn write_creates_file_with_perms_and_content() {
        let dir = tempfile::tempdir().unwrap();
        let writer = FileWriter::new(dir.path().to_path_buf(), 0o700, 0o600);
        writer.write("/myapp/prod/db/password", b"hunter2").unwrap();

        let path = dir.path().join("myapp/prod/db/password");
        assert_eq!(std::fs::read(&path).unwrap(), b"hunter2");

        let file_mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(file_mode, 0o600);

        let dir_mode = std::fs::metadata(path.parent().unwrap())
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(dir_mode, 0o700);
    }

    #[test]
    fn remove_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let writer = FileWriter::new(dir.path().to_path_buf(), 0o700, 0o600);
        writer.write("/a", b"x").unwrap();
        writer.remove("/a").unwrap();
        assert!(!dir.path().join("a").exists());
        // removing again must not error
        writer.remove("/a").unwrap();
    }

    #[test]
    fn rejects_relative_and_traversal_names() {
        let dir = tempfile::tempdir().unwrap();
        let writer = FileWriter::new(dir.path().to_path_buf(), 0o700, 0o600);
        assert!(writer.path_for("relative/path").is_err());
        assert!(writer.path_for("/../etc/passwd").is_err());
        assert!(writer.path_for("/myapp/../../../etc/passwd").is_err());
    }
}
