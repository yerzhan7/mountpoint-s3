//! The FUSE-over-io_uring wire structures, mirroring `include/uapi/linux/fuse.h` (ABI 7.42).

use std::mem;

/// Size of the `in_out` area of a ring entry header, which carries a `fuse_in_header` on the
/// request side and a `fuse_out_header` on the reply side.
pub const FUSE_URING_IN_OUT_HEADER_SZ: usize = 128;
/// Size of the `op_in` area of a ring entry header, which carries the operation-specific input
/// struct (for example `fuse_read_in`).
pub const FUSE_URING_OP_IN_OUT_SZ: usize = 128;

/// `cmd_op` values for `IORING_OP_URING_CMD` on a `/dev/fuse` fd.
pub const FUSE_IO_URING_CMD_REGISTER: u32 = 1;
pub const FUSE_IO_URING_CMD_COMMIT_AND_FETCH: u32 = 2;

/// Per-entry metadata exchanged in both directions.
///
/// On a request, the kernel sets `commit_id` and the number of payload bytes it wrote. On a reply,
/// we set `payload_sz` to the number of payload bytes we wrote, *excluding* the `fuse_out_header`
/// (the kernel treats the uring payload as arguments only; see `fuse_copy_out_args`).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct FuseUringEntInOut {
    pub flags: u64,
    pub commit_id: u64,
    pub payload_sz: u32,
    pub padding: u32,
    pub reserved: u64,
}

/// The header buffer registered as `iov[0]` of a ring entry.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FuseUringReqHeader {
    pub in_out: [u8; FUSE_URING_IN_OUT_HEADER_SZ],
    pub op_in: [u8; FUSE_URING_OP_IN_OUT_SZ],
    pub ring_ent_in_out: FuseUringEntInOut,
}

const _: () = assert!(mem::size_of::<FuseUringReqHeader>() == 288);

/// The 24-byte command payload written into the `cmd` area of an SQE. This is why the ring must be
/// created with `IORING_SETUP_SQE128`: the inline 16-byte `cmd` area of a normal SQE is too small.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct FuseUringCmdReq {
    pub flags: u64,
    pub commit_id: u64,
    pub qid: u16,
    pub padding: [u8; 6],
}

const _: () = assert!(mem::size_of::<FuseUringCmdReq>() == 24);

/// Size of `struct fuse_in_header`, which prefixes every request.
pub const FUSE_IN_HEADER_SZ: usize = 40;
/// Size of `struct fuse_out_header`, which prefixes every reply.
pub const FUSE_OUT_HEADER_SZ: usize = 16;
/// Smallest payload buffer the kernel will accept at registration time.
pub const FUSE_MIN_READ_BUFFER: usize = 8192;
