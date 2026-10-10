//! OpenList paths are absolute, `/`-separated and compared byte for byte.
//!
//! Names are never decoded or case-folded, because OpenList passes them to
//! the storage provider unchanged and two spellings can name two files.

use crate::Error;

/// Returns `path` with exactly one `/` between components and a leading `/`.
///
/// # Errors
/// Rejects `.`, `..`, `\` and NUL. OpenList turns `\` into `/` before it
/// cleans a path, so `a\..\..` would climb out of the directory this client
/// believes it is addressing.
pub fn normalize(path: &str) -> Result<String, Error> {
    let mut out = String::with_capacity(path.len() + 1);
    for part in path.split('/').filter(|part| !part.is_empty()) {
        check_name(part).map_err(|()| invalid(path))?;
        out.push('/');
        out.push_str(part);
    }
    if out.is_empty() {
        out.push('/');
    }
    Ok(out)
}

/// Joins one entry name from a listing to a normalized directory path.
///
/// # Errors
/// Rejects names that are empty, `.`, `..`, or contain `/`, `\` or NUL.
pub fn join(dir: &str, name: &str) -> Result<String, Error> {
    check_name(name).map_err(|()| invalid(&format!("{dir}/{name}")))?;
    let dir = normalize(dir)?;
    Ok(format!("{}/{name}", dir.trim_end_matches('/')))
}

/// Splits a normalized path into its parent directory and final name.
/// Returns `None` for the root.
pub fn split(path: &str) -> Option<(&str, &str)> {
    let (parent, name) = path.rsplit_once('/')?;
    if name.is_empty() {
        return None;
    }
    Some((if parent.is_empty() { "/" } else { parent }, name))
}

fn check_name(name: &str) -> Result<(), ()> {
    if name.is_empty() || name == "." || name == ".." || name.contains(['/', '\\', '\0']) {
        Err(())
    } else {
        Ok(())
    }
}

fn invalid(path: &str) -> Error {
    Error::InvalidPath {
        path: path.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_collapses_separators_without_decoding() {
        assert_eq!(normalize("").unwrap(), "/");
        assert_eq!(normalize("//").unwrap(), "/");
        assert_eq!(normalize("a//b/").unwrap(), "/a/b");
        assert_eq!(normalize("/a/%2e%2e").unwrap(), "/a/%2e%2e");
    }

    #[test]
    fn normalize_rejects_dot_components_and_nul() {
        for path in ["/a/../b", "/a/./b", "..", "/a\0b", "/a\\b", "/w/..\\..\\x"] {
            assert!(normalize(path).is_err(), "{path:?}");
        }
    }

    #[test]
    fn join_validates_the_name() {
        assert_eq!(join("/", "a").unwrap(), "/a");
        assert_eq!(join("/a/", "b c").unwrap(), "/a/b c");
        for name in ["", ".", "..", "x/y", "x\0", "..\\x"] {
            assert!(join("/a", name).is_err(), "{name:?}");
        }
    }

    #[test]
    fn split_returns_parent_and_name() {
        assert_eq!(split("/a"), Some(("/", "a")));
        assert_eq!(split("/a/b"), Some(("/a", "b")));
        assert_eq!(split("/"), None);
    }
}
