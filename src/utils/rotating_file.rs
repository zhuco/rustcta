use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use serde::Serialize;

pub const DEFAULT_MAX_FILE_BYTES: u64 = 100 * 1024 * 1024;
pub const DEFAULT_MAX_FILES: usize = 10;

pub fn append_json_line<T: Serialize>(path: impl AsRef<Path>, value: &T) -> io::Result<()> {
    let mut bytes = serde_json::to_vec(value).map_err(io::Error::other)?;
    bytes.push(b'\n');
    append_record(
        path,
        None,
        &bytes,
        DEFAULT_MAX_FILE_BYTES,
        DEFAULT_MAX_FILES,
    )
}

pub fn append_record(
    path: impl AsRef<Path>,
    header: Option<&[u8]>,
    record: &[u8],
    max_bytes: u64,
    max_files: usize,
) -> io::Result<()> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let current_size = fs::metadata(path)
        .map(|metadata| metadata.len())
        .unwrap_or(0);
    let header_len = if current_size == 0 {
        header.map(|bytes| bytes.len()).unwrap_or(0)
    } else {
        0
    };
    let pending_len = header_len.saturating_add(record.len()) as u64;

    let write_header = if max_bytes > 0
        && current_size > 0
        && current_size.saturating_add(pending_len) > max_bytes
    {
        rotate(path, max_files)?;
        header.is_some()
    } else {
        current_size == 0 && header.is_some()
    };

    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    if write_header {
        if let Some(header) = header {
            file.write_all(header)?;
        }
    }
    file.write_all(record)?;
    Ok(())
}

fn rotate(path: &Path, max_files: usize) -> io::Result<()> {
    if !path.exists() {
        return Ok(());
    }
    if max_files <= 1 {
        fs::remove_file(path)?;
        return Ok(());
    }

    let last_rotated = max_files - 1;
    let oldest = rotated_path(path, last_rotated);
    if oldest.exists() {
        fs::remove_file(oldest)?;
    }

    for index in (1..last_rotated).rev() {
        let source = rotated_path(path, index);
        if source.exists() {
            fs::rename(source, rotated_path(path, index + 1))?;
        }
    }

    fs::rename(path, rotated_path(path, 1))?;
    Ok(())
}

fn rotated_path(path: &Path, index: usize) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(format!(".{index}"));
    PathBuf::from(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn append_record_should_rotate_and_keep_total_file_limit() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("events.jsonl");

        for index in 0..8 {
            let line = format!("{{\"index\":{index}}}\n");
            append_record(&path, None, line.as_bytes(), 20, 3).unwrap();
        }

        assert!(path.exists());
        assert!(rotated_path(&path, 1).exists());
        assert!(rotated_path(&path, 2).exists());
        assert!(!rotated_path(&path, 3).exists());
    }

    #[test]
    fn append_record_should_rewrite_header_after_rotation() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("events.csv");
        let header = b"kind,value\n";

        append_record(&path, Some(header), b"a,1\n", 16, 2).unwrap();
        append_record(&path, Some(header), b"b,2\n", 16, 2).unwrap();

        let current = fs::read_to_string(path).unwrap();
        assert!(current.starts_with("kind,value\n"));
    }
}
