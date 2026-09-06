use std::{
    fs::{File, OpenOptions},
    io,
    os::{
        fd::{AsFd, BorrowedFd},
        unix::prelude::AsRawFd,
    },
    sync::Arc,
};

use libc::{c_int, c_void, size_t};
use nix::Result as NixResult;
use nix::ioctl_read;

#[cfg(feature = "abi-7-40")]
use crate::passthrough::BackingId;
use crate::reply::ReplySender;

/// FUSE_DEV_IOC_CLONE ioctl constant matching the constant defined by the FUSE kernel module in fuse.h
const IOCTL_FUSE_DEV_IOC_CLONE: u8 = 229;

ioctl_read!(
    /// Ioctl to clone a fuse session onto a new file handle
    fuse_dev_ioc_clone,
    IOCTL_FUSE_DEV_IOC_CLONE,
    0,
    libc::c_int
);

/// A raw communication channel to the FUSE kernel driver
#[derive(Debug)]
pub struct Channel(Arc<File>);

impl AsFd for Channel {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.0.as_fd()
    }
}

impl Channel {
    /// Create a new communication channel to the kernel driver by mounting the
    /// given path. The kernel driver will delegate filesystem operations of
    /// the given path to the channel.
    pub(crate) fn new(device: Arc<File>) -> Self {
        Self(device)
    }

    /// Create a worker channel by opening a new /dev/fuse file descriptor and
    /// associating it with the session using FUSE_DEV_IOC_CLONE ioctl.
    pub fn clone_channel(&self) -> io::Result<Self> {
        let worker_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open("/dev/fuse")?;
        let worker_fd = worker_file.as_raw_fd();
        let mut session_fd = self.0.as_raw_fd();

        // Associate the worker fd with the session fd using FUSE_DEV_IOC_CLONE
        // SAFETY: `session_fd` is a valid open file descriptor. The ioctl
        // FUSE_DEV_IOC_CLONE expects a pointer to an int containing the fd
        // to clone from. The pointer is valid for the duration of the call.
        let result: NixResult<libc::c_int> = unsafe {
            let val = &mut session_fd as *mut libc::c_int;
            fuse_dev_ioc_clone(worker_fd, val)
        };

        if let Err(err) = result {
            return Err(io::Error::new(io::ErrorKind::Other, err));
        }
        Ok(Self(Arc::new(worker_file)))
    }

    /// Receives data up to the capacity of the given buffer (can block).
    pub fn receive(&self, buffer: &mut [u8]) -> io::Result<usize> {
        let rc = unsafe {
            libc::read(
                self.0.as_raw_fd(),
                buffer.as_ptr() as *mut c_void,
                buffer.len() as size_t,
            )
        };
        if rc < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(rc as usize)
        }
    }

    /// Returns a sender object for this channel. The sender object can be
    /// used to send to the channel. Multiple sender objects can be used
    /// and they can safely be sent to other threads.
    pub fn sender(&self) -> ChannelSender {
        // Since write/writev syscalls are threadsafe, we can simply create
        // a sender by using the same file and use it in other threads.
        ChannelSender(SenderKind::Classic(self.0.clone()))
    }
}

#[derive(Clone, Debug)]
pub struct ChannelSender(SenderKind);

/// Where a reply goes. The classic transport writes it back to `/dev/fuse`; the io_uring transport
/// copies it into the ring entry the request arrived on, and the owning worker commits it.
#[derive(Clone, Debug)]
enum SenderKind {
    Classic(Arc<File>),
    #[cfg(target_os = "linux")]
    Uring(crate::uring::UringSender),
}

impl ChannelSender {
    /// Create a sender that replies through an io_uring ring entry.
    #[cfg(target_os = "linux")]
    pub(crate) fn uring(sender: crate::uring::UringSender) -> Self {
        Self(SenderKind::Uring(sender))
    }
}

impl ReplySender for ChannelSender {
    fn send(&self, bufs: &[io::IoSlice<'_>]) -> io::Result<()> {
        let file = match &self.0 {
            SenderKind::Classic(file) => file,
            #[cfg(target_os = "linux")]
            SenderKind::Uring(sender) => return sender.send(bufs),
        };
        let rc = unsafe {
            libc::writev(
                file.as_raw_fd(),
                bufs.as_ptr() as *const libc::iovec,
                bufs.len() as c_int,
            )
        };
        if rc < 0 {
            Err(io::Error::last_os_error())
        } else {
            debug_assert_eq!(bufs.iter().map(|b| b.len()).sum::<usize>(), rc as usize);
            Ok(())
        }
    }

    #[cfg(feature = "abi-7-40")]
    fn open_backing(&self, fd: BorrowedFd<'_>) -> std::io::Result<BackingId> {
        match &self.0 {
            SenderKind::Classic(file) => BackingId::create(file, fd),
            #[cfg(target_os = "linux")]
            SenderKind::Uring(_) => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "passthrough is not supported over io_uring",
            )),
        }
    }
}
