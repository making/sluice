//! An app's disk: a writable drive of its one booted VM, a file of the console's directory that
//! outlives the VM, its image and its options. The wicket makes it (`mkfs.ext4`, with a
//! journal: no fsck runs in a VM, and none of the guest's bytes are parsed on the host), and
//! `/init` mounts it before the process starts (`wire::DISK_PARAMETER`).
//!
//! One writer, or the file system is lost: nothing in Firecracker refuses a second VM. So
//! whatever starts a VM on a disk first takes an exclusive `flock` of the file, on an open
//! file its `firecracker` inherits ([`hold`]; the keeper's own in jail mode, `keeper`): the
//! kernel lets it go when that process is gone, a crash of the wicket included. A reload
//! stops the VM holding a disk before it starts the next (`routes`), and a start waits for
//! the lock ([`Disk::ready`]): a killed VM's process takes tens of milliseconds to end. Taking
//! it waits a moment too ([`take`]), for a holder going at once.

use std::io;
use std::os::fd::{AsRawFd as _, IntoRawFd as _};
use std::os::unix::fs::{DirBuilderExt as _, OpenOptionsExt as _};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use crate::BoxError;
use crate::rootfs;
use crate::vm;

/// `disk-mib`'s default.
pub const DEFAULT_MIB: u32 = 1024;

/// `disk-mib`'s least: `mkfs.ext4` makes a smaller file system without a journal, saying so
/// alone, and none is added as it grows.
pub const MIN_MIB: u32 = 8;

/// How long a stop gives the process to end before `/init` kills it and writes all out.
pub const STOP_GRACE: Duration = Duration::from_secs(10);

/// The longest a VM's stop takes (`vm::Vm::stop`), a wedged guest's: then it is killed.
pub const STOP_MOST: Duration =
    Duration::from_secs(STOP_GRACE.as_secs() + vm::STOP_WRITES.as_secs());

/// How long a VM's start waits for its disk: a VM stopping ([`STOP_MOST`]), then a connection's
/// dial holding on to it (`vm::Vm::dial`, 10s at most), then its teardown.
const FREE_WAIT: Duration = Duration::from_secs(40);
const _: () = assert!(FREE_WAIT.as_secs() > STOP_MOST.as_secs());

/// How long taking a disk's lock waits for a holder going at once: [`held`]'s, its file
/// shared with a process the wicket spawns at that moment until that one's exec, or a backup's
/// `flock -n`. A VM on the disk is waited for before ([`Disk::ready`]).
const TAKE_WAIT: Duration = Duration::from_secs(1);

/// A VM's disk.
#[derive(Clone, Debug, PartialEq)]
pub struct Disk {
    /// Its file on the host, named by the console's directory; empty until an app's spec
    /// names it (no other route takes a disk).
    pub file: PathBuf,
    /// Where `/init` mounts it.
    pub mount: String,
    /// Its size at least: the file only grows.
    pub mib: u32,
}

impl Disk {
    /// The size it asks for, in bytes.
    pub fn bytes(&self) -> u64 {
        u64::from(self.mib) << 20
    }

    /// The size of its file: a VM's files are limited to it at least (`RLIMIT_FSIZE` applies
    /// to the offset of a write, in a file of any size).
    pub fn file_bytes(&self) -> Result<u64, String> {
        std::fs::metadata(&self.file)
            .map(|file| file.len())
            .map_err(|e| format!("{}: {e}", self.file.display()))
    }

    /// Whether `path` may be where a disk is mounted: absolute, no `.` or `..`, of what the
    /// kernel's command line carries as it is, and none of what `/init` mounts for itself.
    pub fn check_mount(path: &str) -> Result<(), String> {
        let fits = path.len() <= 256
            && path.starts_with('/')
            && path
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"/._-".contains(&byte))
            && path
                .split('/')
                .skip(1)
                .all(|part| !matches!(part, "" | "." | "..") && !part.is_empty());
        if !fits {
            return Err(format!(
                "disk {path}: expected an absolute path of A-Z, a-z, 0-9, '/', '.', '_' and '-'"
            ));
        }
        for reserved in ["/proc", "/sys", "/dev", "/sluice"] {
            if path == reserved || path.starts_with(&format!("{reserved}/")) {
                return Err(format!("disk {path}: {reserved} is the guest's own"));
            }
        }
        if path == "/tmp" {
            return Err("disk /tmp: /init mounts a tmpfs there".into());
        }
        Ok(())
    }

    /// The disk ready for a VM: no other VM on it (waited for, within [`FREE_WAIT`]), made, or
    /// grown to its size.
    pub async fn ready(&self) -> Result<(), BoxError> {
        let deadline = Instant::now() + FREE_WAIT;
        while held(&self.file)? {
            if Instant::now() > deadline {
                return Err(format!(
                    "{}: in use within {FREE_WAIT:?}: another wicket's VM?",
                    self.file.display()
                )
                .into());
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let disk = self.clone();
        tokio::task::spawn_blocking(move || disk.made()).await??;
        Ok(())
    }

    /// Makes the file when it is missing, grows it when it is smaller; a larger one stays as
    /// it is, a growth whose VM did not start having left it so (a PUT asking for less is
    /// refused before: `deployed`). Its room is taken on the host as well (`fallocate`, where
    /// the file system does it): a disk the host has no room for is refused here, not an I/O
    /// error in the guest later.
    fn made(&self) -> Result<(), String> {
        let wanted = self.bytes();
        let failed = |e: io::Error| format!("{}: {e}", self.file.display());
        match std::fs::metadata(&self.file) {
            Ok(kept) if kept.len() >= wanted => Ok(()),
            Ok(kept) => {
                let file = std::fs::OpenOptions::new()
                    .write(true)
                    .open(&self.file)
                    .map_err(failed)?;
                file.set_len(wanted).map_err(failed)?;
                allocate(&file, wanted).map_err(|e| {
                    // no room: as it was, the next try finding that out again
                    let _ = file.set_len(kept.len());
                    failed(e)
                })
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => self.make(),
            Err(e) => Err(failed(e)),
        }
    }

    /// Makes the file by a rename: a crash leaves none half made.
    fn make(&self) -> Result<(), String> {
        let failed = |e: io::Error| format!("{}: {e}", self.file.display());
        let parent = self.file.parent().ok_or("a disk's file has a directory")?;
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(parent)
            .map_err(failed)?;
        let name = self.file.file_name().ok_or("a disk's file has a name")?;
        let partial = parent.join(format!(".{}.partial", name.to_string_lossy()));
        let _ = std::fs::remove_file(&partial);
        let made = (|| {
            let file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&partial)
                .map_err(failed)?;
            file.set_len(self.bytes()).map_err(failed)?;
            let mkfs = std::process::Command::new(rootfs::Tools::mkfs()?)
                .args([
                    "-q",
                    "-F",
                    "-m",
                    "0",
                    "-e",
                    "remount-ro",
                    "-L",
                    "sluice-disk",
                ])
                .arg(&partial)
                .env_clear()
                .output()
                .map_err(|e| format!("mkfs.ext4: {e}"))?;
            if !mkfs.status.success() {
                return Err(format!(
                    "mkfs.ext4: {}",
                    String::from_utf8_lossy(&mkfs.stderr).trim()
                ));
            }
            allocate(&file, self.bytes()).map_err(failed)?;
            std::fs::rename(&partial, &self.file).map_err(failed)
        })();
        if made.is_err() {
            let _ = std::fs::remove_file(&partial);
        }
        made
    }
}

/// Takes the room of a file of `bytes`, as far as its file system takes such a request.
fn allocate(file: &std::fs::File, bytes: u64) -> io::Result<()> {
    // SAFETY: fallocate of an open file, its size unchanged but what it holds
    if unsafe { libc::fallocate(file.as_raw_fd(), 0, 0, bytes as libc::off_t) } == 0 {
        return Ok(());
    }
    let e = io::Error::last_os_error();
    match e.raw_os_error() {
        Some(libc::EOPNOTSUPP) | Some(libc::ENOSYS) => Ok(()),
        _ => Err(e),
    }
}

/// Takes the exclusive lock of `fd`: false when another holds it.
fn lock(fd: std::os::fd::RawFd) -> io::Result<bool> {
    // SAFETY: flock of an open file, not blocking
    if unsafe { libc::flock(fd, libc::LOCK_EX | libc::LOCK_NB) } == 0 {
        return Ok(true);
    }
    let e = io::Error::last_os_error();
    match e.raw_os_error() {
        Some(libc::EWOULDBLOCK) => Ok(false),
        _ => Err(e),
    }
}

/// Takes the exclusive lock of `fd`, waiting [`TAKE_WAIT`] at most for another to let it go:
/// false when another holds it still.
pub fn take(fd: std::os::fd::RawFd) -> io::Result<bool> {
    let deadline = Instant::now() + TAKE_WAIT;
    loop {
        if lock(fd)? {
            return Ok(true);
        }
        if Instant::now() >= deadline {
            return Ok(false);
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// Whether a VM holds the disk `file` (a missing one: no).
pub fn held(file: &Path) -> io::Result<bool> {
    let open = std::fs::File::open(file);
    match open {
        // given back as it is closed
        Ok(open) => Ok(!lock(open.as_raw_fd())?),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e),
    }
}

/// The launcher's part (`vm::launch`, single-threaded, before its exec): `file` opened and
/// locked for the process it becomes, the lock going with that process.
pub fn hold(file: &Path) -> io::Result<()> {
    let failed = |e: io::Error| io::Error::new(e.kind(), format!("{}: {e}", file.display()));
    let open = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(file)
        .map_err(failed)?;
    if !take(open.as_raw_fd()).map_err(failed)? {
        return Err(io::Error::other(format!(
            "{}: in use by another VM",
            file.display()
        )));
    }
    let fd = open.into_raw_fd();
    // SAFETY: fcntl of the fd just made ours, no more closed on exec
    if unsafe { libc::fcntl(fd, libc::F_SETFD, 0) } != 0 {
        return Err(failed(io::Error::last_os_error()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Made once, as an ext4 file system of its size for its owner alone, its room taken on
    /// the host; grown, what it held stays; asked for less, as it is. A lock held refuses
    /// another.
    #[tokio::test]
    async fn a_disk_is_made_once_grows_and_takes_one_holder() {
        use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
        let dir = std::env::temp_dir().join(format!("sfc-disk-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let mut disk = Disk {
            file: dir.join("cf/app.ext4"),
            mount: "/data".into(),
            mib: 64,
        };
        disk.ready().await.unwrap();
        let made = std::fs::metadata(&disk.file).unwrap();
        assert_eq!(made.len(), 64 << 20);
        assert_eq!(made.permissions().mode() & 0o777, 0o600);
        assert!(made.blocks() * 512 >= 64 << 20, "{} blocks", made.blocks());
        let head = std::fs::read(&disk.file).unwrap();
        assert_eq!(&head[1080..1082], [0x53, 0xEF], "an ext file system");
        // made once: the file stays what it is
        std::fs::write(dir.join("marker"), b"").unwrap();
        disk.ready().await.unwrap();
        assert_eq!(std::fs::read(&disk.file).unwrap(), head);

        disk.mib = 128;
        disk.ready().await.unwrap();
        let grown = std::fs::read(&disk.file).unwrap();
        assert_eq!(grown.len(), 128 << 20);
        assert_eq!(&grown[..head.len()], &head[..], "what it held");
        disk.mib = 64;
        disk.ready().await.unwrap();
        assert_eq!(disk.file_bytes(), Ok(128 << 20));

        assert!(!held(&disk.file).unwrap());
        let holder = std::fs::File::open(&disk.file).unwrap();
        assert!(lock(holder.as_raw_fd()).unwrap());
        assert!(held(&disk.file).unwrap());
        // a holder going at once is waited for
        let taker = std::fs::File::open(&disk.file).unwrap();
        let going = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            drop(holder);
        });
        assert!(take(taker.as_raw_fd()).unwrap());
        going.join().unwrap();
        assert!(held(&disk.file).unwrap());
        drop(taker);
        assert!(!held(&disk.file).unwrap());
        assert!(!held(&dir.join("none")).unwrap());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_disk_mounts_where_the_guest_has_room_for_it() {
        for path in ["/data", "/var/lib/app", "/tmp/data"] {
            assert_eq!(Disk::check_mount(path), Ok(()));
        }
        for path in [
            "data", "/", "/data/", "//data", "/a/../b", "/a b", "/proc", "/dev/x", "/sluice",
            "/tmp",
        ] {
            assert!(Disk::check_mount(path).is_err(), "{path}");
        }
    }
}
