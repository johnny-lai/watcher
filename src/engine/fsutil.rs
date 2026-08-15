use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

/// Creates every missing path component with the given mode, chmod'd explicitly after
/// creation (a mode passed to mkdir alone is still subject to umask). Directories that
/// already existed are left untouched -- pre-existing parents (e.g. `/var/lib`) are not
/// forced to a stricter mode.
pub fn ensure_dir_all(path: &Path, mode: u32) -> anyhow::Result<()> {
    let mut current = PathBuf::new();
    for component in path.components() {
        current.push(component);
        match std::fs::create_dir(&current) {
            Ok(()) => {
                std::fs::set_permissions(&current, std::fs::Permissions::from_mode(mode))?;
            }
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(err) => return Err(err.into()),
        }
    }
    Ok(())
}

/// Writes `data` to `path` atomically: write to a sibling temp file, chmod it, then
/// rename over the destination. The temp name includes the PID so concurrent processes
/// sharing a root don't collide.
pub fn write_atomic(path: &Path, data: &[u8], mode: u32) -> anyhow::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("path has no parent directory: {}", path.display()))?;
    let file_name = path
        .file_name()
        .ok_or_else(|| anyhow::anyhow!("path has no file name: {}", path.display()))?;
    let tmp_path = parent.join(format!(
        ".{}.tmp-{}",
        file_name.to_string_lossy(),
        std::process::id()
    ));
    std::fs::write(&tmp_path, data)?;
    std::fs::set_permissions(&tmp_path, std::fs::Permissions::from_mode(mode))?;
    std::fs::rename(&tmp_path, path)?;
    Ok(())
}
