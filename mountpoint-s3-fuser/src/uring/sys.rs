//! Minimal io_uring bindings.
//!
//! We only need `IORING_OP_URING_CMD` on an `IORING_SETUP_SQE128` ring driven by a single owning
//! thread, so this is a hand-rolled subset rather than a dependency on the `io-uring` crate. The
//! FUSE uring protocol requires setting `sqe->len`, which `io-uring` does not expose for
//! `UringCmd80`.

use std::io;
use std::mem;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::ptr;
use std::sync::atomic::{AtomicU32, Ordering};

const SYS_IO_URING_SETUP: libc::c_long = 425;
const SYS_IO_URING_ENTER: libc::c_long = 426;

pub const IORING_SETUP_SQE128: u32 = 1 << 10;
pub const IORING_ENTER_GETEVENTS: u32 = 1 << 0;
pub const IORING_FEAT_SINGLE_MMAP: u32 = 1 << 0;
pub const IORING_OP_URING_CMD: u8 = 46;

const IORING_OFF_SQ_RING: i64 = 0;
const IORING_OFF_CQ_RING: i64 = 0x0800_0000;
const IORING_OFF_SQES: i64 = 0x1000_0000;

#[repr(C)]
#[derive(Default, Debug)]
struct SqRingOffsets {
    head: u32,
    tail: u32,
    ring_mask: u32,
    ring_entries: u32,
    flags: u32,
    dropped: u32,
    array: u32,
    resv1: u32,
    user_addr: u64,
}

#[repr(C)]
#[derive(Default, Debug)]
struct CqRingOffsets {
    head: u32,
    tail: u32,
    ring_mask: u32,
    ring_entries: u32,
    overflow: u32,
    cqes: u32,
    flags: u32,
    resv1: u32,
    user_addr: u64,
}

#[repr(C)]
#[derive(Default, Debug)]
struct Params {
    sq_entries: u32,
    cq_entries: u32,
    flags: u32,
    sq_thread_cpu: u32,
    sq_thread_idle: u32,
    features: u32,
    wq_fd: u32,
    resv: [u32; 3],
    sq_off: SqRingOffsets,
    cq_off: CqRingOffsets,
}

/// A 128-byte submission queue entry (`IORING_SETUP_SQE128`). The `cmd` payload area begins at
/// offset 48 and runs to the end of the entry.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct Sqe {
    pub opcode: u8,
    pub flags: u8,
    pub ioprio: u16,
    pub fd: i32,
    pub cmd_op: u32,
    pub pad1: u32,
    pub addr: u64,
    pub len: u32,
    pub op_flags: u32,
    pub user_data: u64,
    pub buf_index: u16,
    pub personality: u16,
    pub splice_fd_in: i32,
    pub cmd: [u8; 80],
}

impl Default for Sqe {
    fn default() -> Self {
        // SAFETY: `Sqe` is a `repr(C)` struct of integers and a byte array, so all-zeroes is a
        // valid value.
        unsafe { mem::zeroed() }
    }
}

const _: () = assert!(mem::size_of::<Sqe>() == 128);

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct Cqe {
    pub user_data: u64,
    pub res: i32,
    pub flags: u32,
}

/// An io_uring owned by exactly one thread.
///
/// All submission and completion state is manipulated without locking, which is sound only because
/// a single thread owns the ring for its whole lifetime.
#[derive(Debug)]
pub struct IoUring {
    fd: OwnedFd,
    /// Held only to keep the mappings the pointers below refer to alive.
    _sq_mmap: Mmap,
    _cq_mmap: Option<Mmap>,
    _sqe_mmap: Mmap,

    sq_khead: *const AtomicU32,
    sq_ktail: *const AtomicU32,
    sq_array: *mut u32,
    sq_ring_mask: u32,
    sq_entries: u32,
    sqes: *mut Sqe,
    /// Tail we have filled in locally but may not have published to the kernel yet.
    sq_tail_local: u32,

    cq_khead: *const AtomicU32,
    cq_ktail: *const AtomicU32,
    cq_ring_mask: u32,
    cqes: *const Cqe,
}

// SAFETY: the pointers all reference mmaps owned by this struct (kept alive by the `Mmap` fields)
// and the `OwnedFd`. Nothing is shared with another thread, and `IoUring` is not `Sync`, so moving
// one to another thread transfers exclusive access.
unsafe impl Send for IoUring {}

#[derive(Debug)]
struct Mmap {
    ptr: *mut libc::c_void,
    len: usize,
}

impl Mmap {
    fn new(fd: RawFd, offset: i64, len: usize) -> io::Result<Self> {
        // SAFETY: `fd` is a live io_uring fd and `offset`/`len` come from the kernel-reported ring
        // layout, so the mapping request is valid.
        let ptr = unsafe {
            libc::mmap(
                ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED | libc::MAP_POPULATE,
                fd,
                offset,
            )
        };
        if ptr == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        Ok(Self { ptr, len })
    }
}

impl Drop for Mmap {
    fn drop(&mut self) {
        // SAFETY: `ptr`/`len` describe a mapping we created and have not yet unmapped.
        unsafe { libc::munmap(self.ptr, self.len) };
    }
}

impl IoUring {
    /// Set up a new ring with at least `entries` submission slots.
    pub fn new(entries: u32) -> io::Result<Self> {
        let mut params = Params {
            flags: IORING_SETUP_SQE128,
            ..Default::default()
        };

        // SAFETY: `params` is a correctly laid out `io_uring_params` and lives for the call.
        let fd = unsafe { libc::syscall(SYS_IO_URING_SETUP, entries as libc::c_long, &mut params as *mut Params) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: `io_uring_setup` returned a fresh fd that we now own.
        let fd = unsafe { OwnedFd::from_raw_fd(fd as RawFd) };

        let mut sq_len = params.sq_off.array as usize + params.sq_entries as usize * mem::size_of::<u32>();
        let cq_len = params.cq_off.cqes as usize + params.cq_entries as usize * mem::size_of::<Cqe>();
        let single_mmap = params.features & IORING_FEAT_SINGLE_MMAP != 0;
        if single_mmap {
            sq_len = sq_len.max(cq_len);
        }

        let sq_mmap = Mmap::new(fd.as_raw_fd(), IORING_OFF_SQ_RING, sq_len)?;
        let cq_mmap = if single_mmap {
            None
        } else {
            Some(Mmap::new(fd.as_raw_fd(), IORING_OFF_CQ_RING, cq_len)?)
        };
        let sqe_mmap = Mmap::new(
            fd.as_raw_fd(),
            IORING_OFF_SQES,
            params.sq_entries as usize * mem::size_of::<Sqe>(),
        )?;

        let sq_base = sq_mmap.ptr as *mut u8;
        let cq_base = cq_mmap.as_ref().map_or(sq_base, |m| m.ptr as *mut u8);

        // SAFETY: every offset below was reported by the kernel as lying inside the corresponding
        // mapping, and the mappings are kept alive by `self`.
        unsafe {
            Ok(Self {
                sq_khead: sq_base.add(params.sq_off.head as usize) as *const AtomicU32,
                sq_ktail: sq_base.add(params.sq_off.tail as usize) as *const AtomicU32,
                sq_array: sq_base.add(params.sq_off.array as usize) as *mut u32,
                sq_ring_mask: ptr::read(sq_base.add(params.sq_off.ring_mask as usize) as *const u32),
                sq_entries: params.sq_entries,
                sqes: sqe_mmap.ptr as *mut Sqe,
                sq_tail_local: (*(sq_base.add(params.sq_off.tail as usize) as *const AtomicU32)).load(Ordering::Relaxed),

                cq_khead: cq_base.add(params.cq_off.head as usize) as *const AtomicU32,
                cq_ktail: cq_base.add(params.cq_off.tail as usize) as *const AtomicU32,
                cq_ring_mask: ptr::read(cq_base.add(params.cq_off.ring_mask as usize) as *const u32),
                cqes: cq_base.add(params.cq_off.cqes as usize) as *const Cqe,

                fd,
                _sq_mmap: sq_mmap,
                _cq_mmap: cq_mmap,
                _sqe_mmap: sqe_mmap,
            })
        }
    }

    /// Copy `sqe` into the next free submission slot. Returns false if the queue is full.
    pub fn push(&mut self, sqe: &Sqe) -> bool {
        // SAFETY: `sq_khead` points into the live SQ mapping.
        let head = unsafe { (*self.sq_khead).load(Ordering::Acquire) };
        if self.sq_tail_local.wrapping_sub(head) >= self.sq_entries {
            return false;
        }
        let index = (self.sq_tail_local & self.sq_ring_mask) as usize;
        // SAFETY: `index` is within `sq_entries`, so both writes target slots inside the SQE and SQ
        // mappings, and the slot is free because the kernel has consumed up to `head`.
        unsafe {
            ptr::write(self.sqes.add(index), *sqe);
            ptr::write(self.sq_array.add(index), index as u32);
        }
        self.sq_tail_local = self.sq_tail_local.wrapping_add(1);
        true
    }

    /// Publish any pushed entries and, if `wait_nr > 0`, block until that many completions are
    /// available. Returns the number of entries the kernel consumed.
    pub fn submit_and_wait(&mut self, wait_nr: u32) -> io::Result<u32> {
        // Count everything the kernel has not consumed yet, not just what we pushed since the last
        // call: `io_uring_enter` can return EINTR before consuming, and re-offering an already
        // consumed entry is impossible because the kernel advances `sq_khead` as it takes them.
        // SAFETY: both pointers reference the live SQ mapping, which outlives `self`.
        let to_submit = unsafe {
            let head = (*self.sq_khead).load(Ordering::Acquire);
            let pending = self.sq_tail_local.wrapping_sub(head);
            // The release store publishes the SQE writes made in `push`.
            (*self.sq_ktail).store(self.sq_tail_local, Ordering::Release);
            pending
        };

        let flags = if wait_nr > 0 { IORING_ENTER_GETEVENTS } else { 0 };
        // SAFETY: `fd` is a live io_uring fd; the null argument pointer is allowed when no signal
        // mask is supplied.
        let ret = unsafe {
            libc::syscall(
                SYS_IO_URING_ENTER,
                self.fd.as_raw_fd() as libc::c_long,
                to_submit as libc::c_long,
                wait_nr as libc::c_long,
                flags as libc::c_long,
                ptr::null::<libc::c_void>(),
                0 as libc::c_long,
            )
        };
        if ret >= 0 {
            return Ok(ret as u32);
        }
        let err = io::Error::last_os_error();
        // A completion may have arrived while we were interrupted, so let the caller drain.
        if err.raw_os_error() == Some(libc::EINTR) {
            return Ok(0);
        }
        Err(err)
    }

    /// Pop one completion, or return `None` if the completion queue is empty.
    pub fn pop(&mut self) -> Option<Cqe> {
        // SAFETY: both pointers reference the live CQ mapping.
        let (head, tail) = unsafe {
            (
                (*self.cq_khead).load(Ordering::Relaxed),
                (*self.cq_ktail).load(Ordering::Acquire),
            )
        };
        if head == tail {
            return None;
        }
        let index = (head & self.cq_ring_mask) as usize;
        // SAFETY: `index` is within the CQE array, and the acquire load above guarantees the
        // kernel finished writing this entry. The release store hands the slot back.
        unsafe {
            let cqe = ptr::read(self.cqes.add(index));
            (*self.cq_khead).store(head.wrapping_add(1), Ordering::Release);
            Some(cqe)
        }
    }
}

/// Whether this kernel exposes the FUSE-over-io_uring feature and has it enabled.
pub fn fuse_uring_enabled() -> bool {
    match std::fs::read_to_string("/sys/module/fuse/parameters/enable_uring") {
        Ok(value) => matches!(value.trim(), "Y" | "y" | "1"),
        Err(_) => false,
    }
}
