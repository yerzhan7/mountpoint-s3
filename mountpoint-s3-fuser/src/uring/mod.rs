//! FUSE-over-io_uring request delivery (Linux 6.14+).
//!
//! The classic FUSE transport is a `read`/`writev` pair on `/dev/fuse`: a worker blocks in `read`,
//! the kernel wakes it when a request is queued, the worker replies with `writev`. Every request
//! therefore costs two syscalls plus a scheduler wakeup on a thread that is very unlikely to be the
//! one that submitted the I/O.
//!
//! The io_uring transport replaces that with a set of pre-registered per-CPU ring entries. Each
//! entry pins a header buffer and a payload buffer, and the kernel hands a request to an entry by
//! completing the entry's outstanding `IORING_OP_URING_CMD`. Because the kernel delivers via
//! `io_uring_cmd_complete_in_task`, the copy happens as task work on the thread that registered the
//! entry, on the CPU the request was created on. Replying and asking for the next request are a
//! single `COMMIT_AND_FETCH` command, so the steady state costs one `io_uring_enter` per request
//! instead of two syscalls.
//!
//! # Topology
//!
//! The kernel creates `num_possible_cpus()` queues and routes each request to `task_cpu(current)`.
//! It only switches the connection over to io_uring once *every* queue has at least one available
//! entry (`is_ring_ready`), so we must cover all of them or the feature silently stays off.
//!
//! We give every worker thread its own private ring, so submission needs no locking, and register
//! `entries_per_thread` entries into the queue for the CPU that thread serves. A thread cannot
//! service its entries while it is blocked inside a filesystem operation, so concurrency within one
//! queue comes from `threads_per_queue` rather than from extra entries per thread; extra entries per
//! thread would instead cause head-of-line blocking.

mod abi;
mod sys;

use std::alloc::{Layout, alloc_zeroed, dealloc};
use std::io;
use std::mem;
use std::os::fd::{AsFd, AsRawFd, RawFd};
use std::ptr;
use std::slice;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use log::{debug, error, info, warn};

use crate::Filesystem;
use crate::channel::ChannelSender;
use crate::request::Request;
use crate::session::Session;

use abi::{
    FUSE_IN_HEADER_SZ, FUSE_IO_URING_CMD_COMMIT_AND_FETCH, FUSE_IO_URING_CMD_REGISTER, FUSE_MIN_READ_BUFFER,
    FUSE_OUT_HEADER_SZ, FUSE_URING_OP_IN_OUT_SZ, FuseUringCmdReq, FuseUringReqHeader,
};
use sys::{IORING_OP_URING_CMD, IoUring, Sqe};

pub use sys::fuse_uring_enabled;

/// Bytes reserved in front of each payload buffer, used to rebuild the contiguous request layout
/// that [`Request`] expects. Must be at least `FUSE_IN_HEADER_SZ + FUSE_URING_OP_IN_OUT_SZ`; a page
/// is used so that the payload itself stays page aligned for the kernel's copy in and out.
const REQUEST_PREFIX: usize = 4096;

const _: () = assert!(REQUEST_PREFIX >= FUSE_IN_HEADER_SZ + FUSE_URING_OP_IN_OUT_SZ);

/// Sentinel stored in [`EntryBuffers::replied`] meaning "no reply has been written yet".
const NO_REPLY: u32 = u32::MAX;

/// How long a worker waits for the classic path to complete FUSE_INIT before giving up. The
/// negotiated `max_write`/`max_pages` are needed to size the payload buffers, and the kernel
/// rejects registration before init with `EAGAIN`.
const INIT_TIMEOUT: Duration = Duration::from_secs(30);

/// Tuning for the io_uring transport.
#[derive(Debug, Clone)]
pub struct UringConfig {
    /// Worker threads dedicated to each per-CPU queue.
    pub threads_per_queue: usize,
    /// Ring entries each worker thread registers. Values above 1 let a busy thread buffer requests
    /// but block them behind whatever it is currently dispatching.
    pub entries_per_thread: usize,
    /// Pin each worker to the CPU whose queue it serves.
    ///
    /// Off by default: it looks like the natural choice, since the kernel already routes each request
    /// to the queue for the CPU it originated on, but it makes the worker compete for that CPU with
    /// the application thread that is waiting on the reply. For a buffered read that thread still has
    /// to copy out of the page cache, so it needs the CPU too, and pinning cost 34% of the throughput
    /// of a 4-thread sequential read while cutting the reader's own share of CPU nearly in half.
    pub pin_threads: bool,
}

impl Default for UringConfig {
    fn default() -> Self {
        Self {
            threads_per_queue: 2,
            entries_per_thread: 1,
            pin_threads: false,
        }
    }
}

/// The buffers backing one ring entry.
///
/// The kernel writes requests into these and reads replies out of them, so they must stay at a
/// fixed address for as long as the entry is registered. The allocation is shared with the
/// [`ChannelSender`] handed to [`Request`] so that a reply arriving after the worker has moved on
/// still lands in live memory rather than freed memory.
#[derive(Debug)]
struct EntryBuffers {
    /// `iov[0]`: `fuse_in_header` on the way in, `fuse_out_header` on the way out.
    header: *mut FuseUringReqHeader,
    header_layout: Layout,
    /// `[REQUEST_PREFIX bytes | payload]`. `iov[1]` points at `data + REQUEST_PREFIX`.
    data: *mut u8,
    data_layout: Layout,
    payload_size: usize,
    /// Payload bytes written by the most recent reply, or [`NO_REPLY`].
    replied: AtomicU32,
}

// SAFETY: the pointers refer to allocations owned exclusively by this struct, which is only ever
// reached through an `Arc`. Concurrent access is possible in principle (a stale reply handle) but
// every such access is a plain write into the buffers, ordered by `replied`.
unsafe impl Send for EntryBuffers {}
unsafe impl Sync for EntryBuffers {}

impl EntryBuffers {
    fn new(payload_size: usize) -> io::Result<Arc<Self>> {
        let header_layout = Layout::from_size_align(mem::size_of::<FuseUringReqHeader>(), 4096)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        let data_layout = Layout::from_size_align(REQUEST_PREFIX + payload_size, 4096)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

        // SAFETY: both layouts have non-zero size.
        let (header, data) = unsafe { (alloc_zeroed(header_layout), alloc_zeroed(data_layout)) };
        if header.is_null() || data.is_null() {
            // SAFETY: deallocating only the pointer that did succeed, with its own layout.
            unsafe {
                if !header.is_null() {
                    dealloc(header, header_layout);
                }
                if !data.is_null() {
                    dealloc(data, data_layout);
                }
            }
            return Err(io::Error::from(io::ErrorKind::OutOfMemory));
        }

        Ok(Arc::new(Self {
            header: header.cast(),
            header_layout,
            data,
            data_layout,
            payload_size,
            replied: AtomicU32::new(NO_REPLY),
        }))
    }

    fn payload(&self) -> *mut u8 {
        // SAFETY: `data` has `REQUEST_PREFIX + payload_size` bytes.
        unsafe { self.data.add(REQUEST_PREFIX) }
    }
}

impl Drop for EntryBuffers {
    fn drop(&mut self) {
        // SAFETY: both pointers came from `alloc_zeroed` with the recorded layouts and are freed
        // exactly once.
        unsafe {
            dealloc(self.header.cast(), self.header_layout);
            dealloc(self.data, self.data_layout);
        }
    }
}

/// Reply target for a request that arrived on a ring entry.
///
/// Instead of a `writev` to `/dev/fuse`, a reply is copied into the entry's registered buffers; the
/// owning worker then submits one `COMMIT_AND_FETCH` to hand it to the kernel.
#[derive(Clone, Debug)]
pub(crate) struct UringSender(Arc<EntryBuffers>);

impl UringSender {
    pub(crate) fn send(&self, bufs: &[io::IoSlice<'_>]) -> io::Result<()> {
        let Some((out_header, args)) = bufs.split_first() else {
            return Err(io::Error::from(io::ErrorKind::InvalidInput));
        };
        debug_assert_eq!(out_header.len(), FUSE_OUT_HEADER_SZ);

        let payload_len: usize = args.iter().map(|b| b.len()).sum();
        if out_header.len() > FUSE_OUT_HEADER_SZ || payload_len > self.0.payload_size {
            return Err(io::Error::from_raw_os_error(libc::EOVERFLOW));
        }

        // SAFETY: `header` and `payload` are live for the lifetime of the `Arc`, and the bounds
        // above keep both copies inside their allocations.
        unsafe {
            ptr::copy_nonoverlapping(
                out_header.as_ptr(),
                (*self.0.header).in_out.as_mut_ptr(),
                out_header.len(),
            );
            let payload = self.0.payload();
            let mut offset = 0;
            for arg in args {
                ptr::copy_nonoverlapping(arg.as_ptr(), payload.add(offset), arg.len());
                offset += arg.len();
            }
        }

        self.0.replied.store(payload_len as u32, Ordering::Release);
        Ok(())
    }
}

/// Registration outcome counters shared by all workers, so we can tell whether the kernel actually
/// switched the connection over to io_uring.
///
/// A single rejected registration makes the kernel disable io_uring for the whole connection, so a
/// non-zero `failed` means everything silently fell back to `/dev/fuse`.
#[derive(Debug, Default)]
struct Registrations {
    /// Entries the kernel accepted.
    ok: AtomicUsize,
    /// Entries the kernel rejected.
    failed: AtomicUsize,
}

/// Start the io_uring workers for `session`.
///
/// The workers block until the classic path has completed FUSE_INIT, since the kernel refuses
/// registration before then and the negotiated `max_write` determines the payload buffer size.
pub fn spawn_workers<FS>(
    session: Arc<Session<FS>>,
    config: UringConfig,
) -> io::Result<Vec<JoinHandle<io::Result<()>>>>
where
    FS: Filesystem + Send + Sync + 'static,
{
    if config.threads_per_queue == 0 || config.entries_per_thread == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "fuse-io-uring needs at least one thread and one entry per queue",
        ));
    }
    if !fuse_uring_enabled() {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "fuse-io-uring is not enabled; write Y to /sys/module/fuse/parameters/enable_uring",
        ));
    }

    // The kernel sizes the ring by `num_possible_cpus()` and will not enable it until every queue
    // has an available entry.
    let queues = num_possible_cpus();
    let total = queues * config.threads_per_queue * config.entries_per_thread;
    info!(
        "starting fuse-io-uring: {queues} queues, {} thread(s) per queue, {} entr(y/ies) per thread",
        config.threads_per_queue, config.entries_per_thread
    );

    let registrations = Arc::new(Registrations::default());
    let mut handles = Vec::with_capacity(total + 1);

    for qid in 0..queues {
        for slot in 0..config.threads_per_queue {
            let session = session.clone();
            let config = config.clone();
            let registrations = registrations.clone();
            let handle = thread::Builder::new()
                .name(format!("fuse-uring-{qid}-{slot}"))
                .spawn(move || {
                    if config.pin_threads {
                        pin_to_cpu(qid);
                    }
                    let result = run_worker(&session, qid as u16, &config, &registrations);
                    if let Err(err) = &result {
                        debug!("fuse-io-uring worker qid={qid} exiting: {err}");
                    }
                    result
                })?;
            handles.push(handle);
        }
    }

    // Report whether the kernel actually took the ring, since a single failed registration disables
    // io_uring for the whole connection and everything silently falls back to /dev/fuse.
    handles.push(
        thread::Builder::new()
            .name("fuse-uring-monitor".to_owned())
            .spawn(move || {
                let deadline = Instant::now() + INIT_TIMEOUT + Duration::from_secs(5);
                while Instant::now() < deadline {
                    let ok = registrations.ok.load(Ordering::Relaxed);
                    let failed = registrations.failed.load(Ordering::Relaxed);
                    if ok + failed >= total {
                        break;
                    }
                    thread::sleep(Duration::from_millis(20));
                }
                let ok = registrations.ok.load(Ordering::Relaxed);
                let failed = registrations.failed.load(Ordering::Relaxed);
                if failed > 0 || ok < total {
                    warn!(
                        "fuse-io-uring registered only {ok}/{total} entries ({failed} rejected); \
                         the kernel will keep serving this mount over /dev/fuse"
                    );
                } else {
                    info!("fuse-io-uring registered {ok}/{total} entries across {queues} queues");
                }
                Ok(())
            })?,
    );

    Ok(handles)
}

fn run_worker<FS: Filesystem>(
    session: &Session<FS>,
    qid: u16,
    config: &UringConfig,
    registrations: &Registrations,
) -> io::Result<()> {
    let payload_size = wait_for_payload_size(session)?;
    let fuse_fd = session.as_fd().as_raw_fd();

    let sq_entries = ((config.entries_per_thread + 2).next_power_of_two() as u32).max(4);
    let ring = IoUring::new(sq_entries)?;

    let mut entries = Vec::with_capacity(config.entries_per_thread);
    for _ in 0..config.entries_per_thread {
        let buffers = EntryBuffers::new(payload_size)?;
        let iov = Box::new([
            libc::iovec {
                iov_base: buffers.header.cast(),
                iov_len: mem::size_of::<FuseUringReqHeader>(),
            },
            libc::iovec {
                iov_base: buffers.payload().cast(),
                iov_len: payload_size,
            },
        ]);
        entries.push(Entry { buffers, iov });
    }

    Worker {
        session,
        ring,
        fuse_fd,
        qid,
        entries,
    }
    .run(registrations)
}

/// Block until FUSE_INIT has been negotiated on the classic path, then return the payload buffer
/// size the kernel will require at registration time.
fn wait_for_payload_size<FS: Filesystem>(session: &Session<FS>) -> io::Result<usize> {
    let deadline = Instant::now() + INIT_TIMEOUT;
    while !session.is_initialized() {
        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "timed out waiting for FUSE_INIT before registering io_uring entries",
            ));
        }
        thread::sleep(Duration::from_millis(1));
    }
    // Mirrors `fuse_uring_create`: max(FUSE_MIN_READ_BUFFER, fc->max_write, fc->max_pages * PAGE_SIZE).
    let size = session.uring_payload_size().max(FUSE_MIN_READ_BUFFER);
    Ok(size)
}

struct Entry {
    buffers: Arc<EntryBuffers>,
    /// The 2-element iovec array `sqe->addr` points at. The kernel imports it during registration,
    /// but we keep it alive for the entry's lifetime because submission is asynchronous.
    iov: Box<[libc::iovec; 2]>,
}

struct Worker<'a, FS: Filesystem> {
    session: &'a Session<FS>,
    ring: IoUring,
    fuse_fd: RawFd,
    qid: u16,
    entries: Vec<Entry>,
}

impl<FS: Filesystem> Worker<'_, FS> {
    fn run(&mut self, registrations: &Registrations) -> io::Result<()> {
        let mut live = self.register_all(registrations)?;

        while live > 0 {
            self.ring.submit_and_wait(1)?;
            while let Some(cqe) = self.ring.pop() {
                let index = cqe.user_data as usize;
                if cqe.res < 0 {
                    let errno = -cqe.res;
                    // A registration that lost the race with FUSE_INIT can still surface here if the
                    // kernel deferred it to a worker rather than failing it inline.
                    if errno == libc::EAGAIN {
                        thread::sleep(Duration::from_millis(1));
                        self.push_cmd(index, FUSE_IO_URING_CMD_REGISTER, 0);
                        continue;
                    }
                    if !matches!(
                        errno,
                        libc::ENOTCONN | libc::ECONNABORTED | libc::ECANCELED | libc::EINTR
                    ) {
                        warn!(
                            "fuse-io-uring qid={} entry failed: {}",
                            self.qid,
                            io::Error::from_raw_os_error(errno)
                        );
                    }
                    live -= 1;
                    continue;
                }
                self.handle_request(index);
            }
        }
        Ok(())
    }

    /// Register every entry, returning how many the kernel accepted.
    ///
    /// The kernel reports success by *not* completing the command (it returns `-EIOCBQUEUED` and
    /// holds the entry until a request arrives), so an entry is registered exactly when no completion
    /// shows up for it. Failures are returned inline from the submit, so draining the completion
    /// queue once afterwards is enough to see them.
    fn register_all(&mut self, registrations: &Registrations) -> io::Result<usize> {
        // The kernel only accepts registration once it has processed our FUSE_INIT reply, which lags
        // the point where we consider the session initialized, so EAGAIN here just means "too early".
        let deadline = Instant::now() + INIT_TIMEOUT;
        let mut pending: Vec<usize> = (0..self.entries.len()).collect();
        let mut failed = 0;

        while !pending.is_empty() {
            for &index in &pending {
                if !self.push_cmd(index, FUSE_IO_URING_CMD_REGISTER, 0) {
                    return Err(io::Error::other("submission queue too small to register entries"));
                }
            }
            self.ring.submit_and_wait(0)?;

            let mut retry = Vec::new();
            while let Some(cqe) = self.ring.pop() {
                let index = cqe.user_data as usize;
                let errno = if cqe.res < 0 { -cqe.res } else { libc::EIO };
                if errno == libc::EAGAIN && Instant::now() < deadline {
                    retry.push(index);
                    continue;
                }
                error!(
                    "fuse-io-uring qid={} failed to register entry: {}",
                    self.qid,
                    io::Error::from_raw_os_error(errno)
                );
                failed += 1;
            }

            pending = retry;
            if !pending.is_empty() {
                thread::sleep(Duration::from_millis(1));
            }
        }

        let live = self.entries.len() - failed;
        registrations.ok.fetch_add(live, Ordering::Relaxed);
        registrations.failed.fetch_add(failed, Ordering::Relaxed);
        Ok(live)
    }

    /// Dispatch the request the kernel left in entry `index`, then hand the reply back and ask for
    /// the next request in one command.
    fn handle_request(&mut self, index: usize) {
        let buffers = self.entries[index].buffers.clone();

        // SAFETY: the kernel filled this header before completing the fetch, and no one else
        // touches it while the entry is in userspace.
        let header = unsafe { &*buffers.header };
        let commit_id = header.ring_ent_in_out.commit_id;
        let payload_sz = header.ring_ent_in_out.payload_sz as usize;
        let total_len = u32::from_ne_bytes(header.in_out[..4].try_into().expect("4 bytes")) as usize;
        let unique = u64::from_ne_bytes(header.in_out[8..16].try_into().expect("8 bytes"));

        // `fuse_uring_args_to_ring` puts `in_args[0]` (the per-op header, sometimes empty) in
        // `op_in` and the remaining args in the payload, so the op header length is whatever the
        // request length has left over.
        let op_header_len = total_len
            .checked_sub(FUSE_IN_HEADER_SZ + payload_sz)
            .filter(|len| *len <= FUSE_URING_OP_IN_OUT_SZ)
            .filter(|_| payload_sz <= buffers.payload_size);

        let Some(op_header_len) = op_header_len else {
            error!(
                "fuse-io-uring qid={} malformed request: len={total_len} payload_sz={payload_sz}",
                self.qid
            );
            self.commit_error(index, unique, commit_id, libc::EIO);
            return;
        };

        // Rebuild the contiguous `fuse_in_header || op header || payload` layout that `Request`
        // parses, by copying the two headers into the prefix that sits immediately before the
        // payload. The payload itself is never copied.
        let start = REQUEST_PREFIX - FUSE_IN_HEADER_SZ - op_header_len;
        // SAFETY: `start >= 0` because `REQUEST_PREFIX` exceeds both header sizes, and the copies
        // stay inside the prefix. The resulting slice covers prefix bytes plus `payload_sz` bytes of
        // payload, all within the single `data` allocation.
        let request = unsafe {
            let base = buffers.data.add(start);
            ptr::copy_nonoverlapping(header.in_out.as_ptr(), base, FUSE_IN_HEADER_SZ);
            if op_header_len > 0 {
                ptr::copy_nonoverlapping(header.op_in.as_ptr(), base.add(FUSE_IN_HEADER_SZ), op_header_len);
            }
            slice::from_raw_parts(base, FUSE_IN_HEADER_SZ + op_header_len + payload_sz)
        };

        buffers.replied.store(NO_REPLY, Ordering::Relaxed);
        match Request::new(ChannelSender::uring(UringSender(buffers.clone())), request) {
            Some(req) => req.dispatch(self.session),
            None => {
                self.commit_error(index, unique, commit_id, libc::EIO);
                return;
            }
        }

        let payload_sz = buffers.replied.swap(NO_REPLY, Ordering::Acquire);
        if payload_sz == NO_REPLY {
            // No reply was produced. The kernel always expects one over io_uring: reply-less
            // operations (FORGET) are still delivered on the classic path.
            warn!("fuse-io-uring qid={} request produced no reply, replying empty", self.qid);
            self.write_out_header(index, unique, 0);
            self.commit(index, commit_id, 0);
            return;
        }
        self.commit(index, commit_id, payload_sz);
    }

    fn commit_error(&mut self, index: usize, unique: u64, commit_id: u64, errno: i32) {
        self.write_out_header(index, unique, -errno);
        self.commit(index, commit_id, 0);
    }

    fn write_out_header(&mut self, index: usize, unique: u64, error: i32) {
        let header = self.entries[index].buffers.header;
        let mut out = [0u8; FUSE_OUT_HEADER_SZ];
        out[..4].copy_from_slice(&(FUSE_OUT_HEADER_SZ as u32).to_ne_bytes());
        out[4..8].copy_from_slice(&error.to_ne_bytes());
        out[8..].copy_from_slice(&unique.to_ne_bytes());
        // SAFETY: `header` is live and `in_out` is larger than `FUSE_OUT_HEADER_SZ`.
        unsafe { ptr::copy_nonoverlapping(out.as_ptr(), (*header).in_out.as_mut_ptr(), out.len()) };
    }

    fn commit(&mut self, index: usize, commit_id: u64, payload_sz: u32) {
        // SAFETY: `header` is live; `ring_ent_in_out` is the only field the kernel reads back, and
        // only `payload_sz` within it.
        unsafe { (*self.entries[index].buffers.header).ring_ent_in_out.payload_sz = payload_sz };
        if !self.push_cmd(index, FUSE_IO_URING_CMD_COMMIT_AND_FETCH, commit_id) {
            error!("fuse-io-uring qid={} submission queue full, dropping reply", self.qid);
        }
    }

    /// Build and enqueue one `IORING_OP_URING_CMD` for entry `index`.
    fn push_cmd(&mut self, index: usize, cmd_op: u32, commit_id: u64) -> bool {
        let cmd_req = FuseUringCmdReq {
            flags: 0,
            commit_id,
            qid: self.qid,
            padding: [0; 6],
        };
        let mut sqe = Sqe {
            opcode: IORING_OP_URING_CMD,
            fd: self.fuse_fd,
            cmd_op,
            user_data: index as u64,
            ..Default::default()
        };
        // Registration passes the buffers as a 2-element iovec; `sqe->len` must be exactly 2.
        // COMMIT_AND_FETCH reuses the already-registered buffers and reads neither field.
        if cmd_op == FUSE_IO_URING_CMD_REGISTER {
            sqe.addr = self.entries[index].iov.as_ptr() as u64;
            sqe.len = 2;
        }
        // SAFETY: `FuseUringCmdReq` is a 24-byte plain-data struct and `sqe.cmd` is 80 bytes.
        unsafe {
            ptr::copy_nonoverlapping(
                (&raw const cmd_req).cast::<u8>(),
                sqe.cmd.as_mut_ptr(),
                mem::size_of::<FuseUringCmdReq>(),
            );
        }
        self.ring.push(&sqe)
    }
}

fn num_possible_cpus() -> usize {
    if let Ok(list) = std::fs::read_to_string("/sys/devices/system/cpu/possible") {
        if let Some(count) = parse_cpu_list(list.trim()) {
            return count;
        }
    }
    // SAFETY: `sysconf` with a valid name has no preconditions.
    let count = unsafe { libc::sysconf(libc::_SC_NPROCESSORS_CONF) };
    if count > 0 { count as usize } else { 1 }
}

/// Parse a kernel CPU list such as `0-95` or `0,2-3` into a count.
fn parse_cpu_list(list: &str) -> Option<usize> {
    let mut count = 0usize;
    for range in list.split(',') {
        let (first, last) = match range.split_once('-') {
            Some((first, last)) => (first.trim().parse::<usize>().ok()?, last.trim().parse::<usize>().ok()?),
            None => {
                let only = range.trim().parse::<usize>().ok()?;
                (only, only)
            }
        };
        count += last.checked_sub(first)? + 1;
    }
    (count > 0).then_some(count)
}

fn pin_to_cpu(cpu: usize) {
    // SAFETY: `set` is a zeroed `cpu_set_t` sized correctly for `sched_setaffinity`, and pid 0 means
    // the calling thread. A failure here is a performance hint only, so it is ignored.
    unsafe {
        let mut set: libc::cpu_set_t = mem::zeroed();
        libc::CPU_SET(cpu, &mut set);
        libc::sched_setaffinity(0, mem::size_of::<libc::cpu_set_t>(), &set);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_dense_cpu_list() {
        assert_eq!(parse_cpu_list("0-95"), Some(96));
        assert_eq!(parse_cpu_list("0"), Some(1));
        assert_eq!(parse_cpu_list("0,2-3"), Some(3));
        assert_eq!(parse_cpu_list(""), None);
        assert_eq!(parse_cpu_list("bogus"), None);
    }
}
