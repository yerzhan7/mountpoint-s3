# FUSE over io_uring

This describes the experimental io_uring transport for FUSE requests: what it changes, how to turn
it on, what it measured, and what is left unfinished. It is a prototype meant to answer whether the
transport is worth pursuing, not a supported feature.

## Why

On the classic transport a FUSE request costs a `read` on `/dev/fuse` to receive it and a `writev` to
reply. A worker thread blocks in `read`, the kernel queues a request and wakes it, the worker replies.
The waking thread is picked from a pool and has no relationship to the CPU the request came from, so
each request also pays a scheduler wakeup and whatever cache misses come with landing on a different
CPU.

Linux 6.14 added `FUSE_OVER_IO_URING`. Userspace pre-registers ring entries, each pinning a header
buffer and a payload buffer; the kernel delivers a request by completing that entry's outstanding
`IORING_OP_URING_CMD`, and userspace replies and asks for the next request with a single
`FUSE_IO_URING_CMD_COMMIT_AND_FETCH`. Delivery goes through `io_uring_cmd_complete_in_task`, so the
copy runs as task work on the thread that registered the entry. Steady state is one `io_uring_enter`
per request instead of two syscalls plus a wakeup.

## Enabling it

The kernel gates the feature behind a module parameter that is off by default:

```bash
# Requires Linux 6.14 or newer.
echo Y > /sys/module/fuse/parameters/enable_uring
```

Then set the environment variable at mount time:

| Variable | Default | Meaning |
|---|---|---|
| `UNSTABLE_MOUNTPOINT_FUSE_IO_URING` | off | Enable the transport |
| `UNSTABLE_MOUNTPOINT_FUSE_IO_URING_THREADS_PER_QUEUE` | 2 | Worker threads per CPU queue |
| `UNSTABLE_MOUNTPOINT_FUSE_IO_URING_ENTRIES_PER_THREAD` | 1 | Ring entries each worker registers |
| `UNSTABLE_MOUNTPOINT_FUSE_IO_URING_PIN_THREADS` | off | Pin each worker to the CPU whose queue it serves |
| `UNSTABLE_MOUNTPOINT_FUSE_IO_URING_MAX_WRITE_KIB` | 1024 | FUSE `max_write` to negotiate |

Confirm the kernel accepted the ring — with `--debug`, look for:

```
fuser::uring: fuse-io-uring registered 192/192 entries across 96 queues
```

This matters because the fallback is silent. The kernel only switches a connection to io_uring once
*every* per-CPU queue has an available entry, and one rejected registration sets `fc->io_uring = 0`
for the life of the connection. Either way the mount keeps working over `/dev/fuse`, so throughput is
the only symptom. A warning is logged if fewer than all entries register.

## Structure

The transport lives in `mountpoint-s3-fuser/src/uring/`:

- `sys.rs` — a hand-rolled io_uring binding. The `io-uring` crate cannot set `sqe->len`, which the
  kernel requires to be exactly 2 for registration, so a dependency would not have worked.
- `abi.rs` — the `fuse_uring_*` structures, with size assertions against the kernel's layout.
- `mod.rs` — worker threads, registration, and request dispatch.

`ChannelSender` became an enum over `{Classic, Uring}`, which is the only change to the existing
reply path; `Request` and `Reply` are untouched. A request arriving on a ring entry is reassembled
into the contiguous `fuse_in_header || op header || payload` layout that `Request` already parses, by
copying the two headers into a page-sized prefix that sits immediately before the payload buffer. The
payload itself is never copied on the way in.

### Topology

The kernel creates `num_possible_cpus()` queues and routes each request to `task_cpu(current)`. Every
queue must be covered or the feature stays off, so workers are laid out as `threads_per_queue` threads
per CPU queue, each owning a private ring — submission needs no locking — and registering
`entries_per_thread` entries into its queue.

`entries_per_thread` defaults to 1 on purpose. Mountpoint dispatches a request by blocking in
`block_on`, and a thread cannot service its other entries while it is blocked, so extra entries per
thread would only queue requests behind the one in flight. Concurrency within a queue comes from
`threads_per_queue`.

### Registration is racy against FUSE_INIT

The kernel rejects registration with `EAGAIN` until it has processed the FUSE_INIT reply, and it needs
the negotiated `max_write`/`max_pages` to size payload buffers. Workers therefore wait for the classic
path to complete init before registering. That is necessary but not sufficient: fuser marks the
session initialized *before* the reply is written, so `EAGAIN` is still possible and is retried.

Success is signalled by the *absence* of a completion — the kernel returns `-EIOCBQUEUED` and holds
the entry until a request arrives — so an entry counts as registered exactly when no completion shows
up for it after the submit.

## Results

Measured on an m5n.24xlarge-class host: 96 vCPUs (2 sockets, Xeon 8259CL), 373 GiB RAM, 100 Gbps
network, Linux 6.18, against 100 GiB objects in an S3 bucket in the same Region. Both arms are the
*same binary* with only the transport switched, five 30-second fio iterations per job, page cache
dropped before each, `--part-size=16777216`.

| Job | classic `/dev/fuse` | io_uring | Change |
|---|---|---|---|
| `seq_read_4t` | 6779 MiB/s | 7256 MiB/s | **+7.0%** |
| `seq_read_4t_direct` | 6322 MiB/s | 6575 MiB/s | **+4.0%** |
| `seq_read_16t` | 9927 MiB/s | 10061 MiB/s | +1.4% |
| `seq_read_16t_direct` | 10300 MiB/s | 10441 MiB/s | +1.4% |
| `rand_read_4t` | 9.57 MiB/s | 9.54 MiB/s | -0.3% (interleaved) |
| `rand_read_4t_direct` | 9.65 MiB/s | 9.67 MiB/s | +0.2% (interleaved) |

The random-read rows come from a separate interleaved A/B (`classic`, `uring`, `classic`, `uring`,
four iterations each), because measuring them back to back showed io_uring 6.7% slower and that
turned out to be drift over the 40 minutes separating the two arms. Alternating the arms removes it.

Latency and CPU, same runs:

| Job | p50 classic | p50 io_uring | reader sys% classic | reader sys% io_uring |
|---|---|---|---|---|
| `seq_read_4t` | 116 µs | 111 µs | 58.3 | 54.9 |
| `seq_read_4t_direct` | 136 µs | 124 µs | 9.1 | 9.3 |
| `seq_read_16t` | 258 µs | **125 µs** | 27.2 | 21.4 |
| `seq_read_16t_direct` | 159 µs | 145 µs | 4.6 | 4.1 |

### Reading the numbers

The 16-thread jobs sit at roughly 10 GiB/s in every configuration, which is about 86 Gbps and so
close to the network limit; there is no throughput for a cheaper transport to win there. What it wins
instead shows up in the other two columns: `seq_read_16t` p50 latency halves, from 258 µs to 125 µs,
and the reader spends a fifth less of its time in the kernel. That is the transport change visible on
its own, with throughput held fixed by the network.

The throughput headroom is in the 4-thread jobs, and both improve — 7.0% buffered, 4.0% direct.
Standard deviation across the five iterations is about 340 MiB/s for `seq_read_4t`, so the 477 MiB/s
gain is roughly 1.4σ on its own; an earlier independent 3-iteration run measured +8.4% with
non-overlapping ranges, which is what makes the direction credible rather than the single run.

The random-read jobs are latency-bound at about 40 IOPS with a ~96 ms p50, so the transport accounts
for microseconds out of each operation. Run-to-run drift on these jobs reached 9% between runs of the
*same* transport during this work, which is larger than any difference measured between transports;
an interleaved A/B was needed to see that the apparent gap was drift.

### Where the win comes from

Counting the Mountpoint process's syscalls with `perf stat` over a fixed 20-second window of
`seq_read_4t`, normalized by the data each window moved (this job issues 256 KiB reads, so about 4.1
FUSE requests per MiB):

| Per MiB read | classic | io_uring |
|---|---|---|
| `writev` (reply) | 4.09 | 0 |
| `io_uring_enter` | 0 | 4.14 |
| `read` (all fds) | 105.8 | 101.8 |
| context switches | 18.1 | 18.2 |

The reply syscall is replaced one-for-one. The `read` count covers the CRT's network sockets as well
as `/dev/fuse` so it cannot be split directly, but it falls by 4.03 per MiB — the receive-side `read`
per request, gone. Two syscalls per request became one.

Context switches per MiB are unchanged, which is worth stating because it contradicts the obvious
guess: the saving is not fewer wakeups. Delivery still wakes the worker thread blocked in
`io_uring_enter`. What is saved is the syscall pair and the cost of the request being handled on the
CPU it originated from.

### Pinning workers to their queue's CPU is a large loss

Pinning looks like the obvious thing to do — the kernel already routes each request to the queue for
the CPU it came from, so serving it there should keep the data local. Measured, it cost 34% of
`seq_read_4t`:

| Configuration | `seq_read_4t` MiB/s | fio's own sys% |
|---|---|---|
| classic `/dev/fuse` | 6864 | 58.4 |
| io_uring, pinned, 2 threads/queue | 4527 | 31.0 |
| io_uring, pinned, 4 threads/queue | 4729 | 31.8 |
| io_uring, unpinned, 2 threads/queue | 7442 | 58.2 |

The `sys%` column is the diagnostic: it is the reader's own kernel time, and pinning nearly halves it.
The reader is not doing less work per byte, it is getting less CPU. A buffered read leaves the
page-cache copy to the application thread, so that thread needs the CPU right when the reply arrives —
and pinning puts the worker that produces the reply in direct competition with it on the same CPU.
Unpinned, the scheduler is free to place the worker elsewhere and both make progress. Pinning is
therefore off by default.

## Known limitations

- **`max_write` is capped at 1 MiB when enabled.** Every ring entry needs its own payload buffer of
  `max(8 KiB, max_write, max_pages × PAGE_SIZE)`, and there are `num_possible_cpus() ×
  threads_per_queue` of them. At Mountpoint's default 16 MiB `max_write` that is tens of gigabytes,
  so `max_write` drops to 1 MiB, which is what `max_pages` (clamped to 256 by
  `/proc/sys/fs/fuse/max_pages_limit`) allows a READ to be anyway. Reads are unaffected; writes are
  split into more, smaller requests, which has not been measured.
- **The reply path copies once more than the classic path.** `writev` hands the filesystem's buffer
  straight to the kernel, whereas io_uring requires the reply to be in the registered payload buffer,
  so the data is copied there first and the kernel copies it again to its destination. Avoiding this
  means letting the filesystem write directly into the ring buffer, which reaches into the prefetcher
  and the buffer pool.
- **Memory scales with CPU count**, at roughly `num_possible_cpus() × threads_per_queue × max_write`
  — about 200 MiB at the defaults on a 96-CPU host. This was evaluated only in the
  non-memory-limited case and is not integrated with `PagedPool` or `--memory-target`.
- **Writes, FUSE passthrough, and interrupts are untested or unsupported.** `open_backing` returns
  `ENOTSUP` on a uring reply, so passthrough cannot be combined with it. FORGET, INTERRUPT,
  FUSE_INIT, and notifications keep using `/dev/fuse` by kernel design, so the classic worker pool
  still runs.
- **`num_possible_cpus()`, not online CPUs.** Queues are created for possible CPUs, so a host that
  reports far more possible than online CPUs allocates threads and buffers it will never use.
