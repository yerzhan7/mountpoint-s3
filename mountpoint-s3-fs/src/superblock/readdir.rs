//! Utilities for implementing the `readdir` operation on a directory inode.
//!
//! `readdir` is conceptually simple ("just call ListObjectsV2") but has some subtleties that make
//! it complicated:
//!
//! 1. It's possible for a directory to contain both a subdirectory and a file of the same name, in
//!    which case we should implement shadowing in the right way where possible.
//! 2. A directory can also contain local files/subdirectories, which we want to have appear in the
//!    readdir stream, but be shadowed by remote files/subdirectories.
//! 3. ListObjectsV2 returns common prefixes including the trailing '/', which messes up ordering
//!    of entries in the readdir stream. For example, `a-/` < `a/`, but `a-` > `a` in lexicographic
//!    order, so we need to re-sort the common prefixes rather than just streaming them directly.
//! 4. We want to do large ListObjectsV2 calls (i.e. with a large page size), but `readdir` calls
//!    typically are much smaller, so we need to hold onto ListObjectsV2 results for a while. But we
//!    don't want them to expire while we're holding onto them.
//! 5. FUSE's `readdir` design makes it hard to know in advance exactly how many entries we'll be
//!    able to return in a single request (fixed-size buffer but names are variable size), so we
//!    need to be able to "peek" the next entry in the stream in case it won't fit.
//!
//! This module tries to decouple each of these requirements by building a hierarchy of iterators
//! to implement the `readdir` stream:
//!
//! * [ReaddirHandle] is the top-level iterator, and the only public struct in this module. Its
//!   results can be directly returned to `readdir`. It takes results from [ReaddirIter] and creates
//!   inodes for them, achieving point 4. It also has a [ReaddirHandle::readd] method to handle
//!   point 5.
//! * [ReaddirIter] is an iterator over [ReaddirEntry]s, which are entries that may not yet have
//!   inodes created for them.
//!   Addressing point 2, [ReaddirIter] merges together entries from two sources:
//!   remotely from S3 using [RemoteIter] and locally from a snapshot of the parent's local children.
//!   While merging, [ReaddirIter] makes a best effort to deduplicate entries returned to address point 1.
//!   Notably, the [unordered] implementation does not address duplicate remote entries
//!   as reported in [#725](https://github.com/awslabs/mountpoint-s3/issues/725).
//!   [ReaddirIter] itself delegates to two different iterator implementations,
//!   depending on if the S3 implementation returns ordered or unordered list results.
//! * [RemoteIter] is an iterator over [ReaddirEntry]s returned by paginated calls to ListObjectsV2.
//!   Rather than directly streaming the entries out of the list call, it collects them in memory
//!   and re-sorts them to handle point 3.
//! * A collection or iterator of [ReaddirEntry]s is built up and used by [ReaddirIter],
//!   representing the local children of the directory.
//!   These children are listed only once, at the start of the readdir operation, and so are a
//!   snapshot in time of the directory.

use std::collections::VecDeque;
use std::ffi::OsString;
use std::time::Duration;

use super::{Inode, InodeKindData, LookedUpInode, RemoteLookup, SuperblockInner};
use crate::metablock::{InodeError, InodeKind, InodeNo, InodeStat};
use crate::sync::atomic::{AtomicI64, Ordering};
use crate::sync::{AsyncMutex, Mutex};
use mountpoint_s3_client::ObjectClient;
use mountpoint_s3_client::types::RestoreStatus;
use time::OffsetDateTime;
use tracing::{error, trace, warn};

/// Handle for an inflight directory listing
#[derive(Debug)]
pub struct ReaddirHandle {
    dir_ino: InodeNo,
    parent_ino: InodeNo,
    iter: AsyncMutex<ReaddirIter>,
    readded: Mutex<Option<LookedUpInode>>,
}

impl ReaddirHandle {
    pub(super) fn new<OC: ObjectClient + Send + Sync>(
        inner: &SuperblockInner<OC>,
        dir_ino: InodeNo,
        parent_ino: InodeNo,
        full_path: String,
        page_size: usize,
    ) -> Result<Self, InodeError> {
        let inode = inner.get(dir_ino)?;
        
        // Check if we have a valid complete listing cache
        let use_cached_listing = inode.has_valid_complete_listing()?;
        println!("READDIR CACHE DEBUG: Directory {} will use cached listing: {}", dir_ino, use_cached_listing);
        
        let local_entries = {
            let kind_data = &inode.get_inode_state()?.kind_data;
            let local_files = match kind_data {
                InodeKindData::File { .. } => return Err(InodeError::NotADirectory(inode.err())),
                InodeKindData::Directory { writing_children, .. } => writing_children.iter().map(|ino| {
                    let inode = inner.get(*ino)?;
                    let stat = inode.get_inode_state()?.stat.clone();
                    Ok(ReaddirEntry::LocalInode {
                        lookup: LookedUpInode {
                            inode,
                            stat,
                            path: inner.s3_path.clone(),
                        },
                    })
                }),
            };

            match local_files.collect::<Result<Vec<_>, _>>() {
                Ok(mut new_results) => {
                    new_results.sort();
                    new_results
                }
                Err(e) => {
                    error!(error=?e, "readdir failed listing local files");
                    return Err(e);
                }
            }
        };

        let iter = if inner.config.s3_personality.is_list_ordered() {
            if use_cached_listing {
                ReaddirIter::cached_ordered(&inner.s3_path.bucket, &full_path, page_size, local_entries.into(), &inode)
            } else {
                ReaddirIter::ordered(&inner.s3_path.bucket, &full_path, page_size, local_entries.into())
            }
        } else {
            if use_cached_listing {
                ReaddirIter::cached_unordered(&inner.s3_path.bucket, &full_path, page_size, local_entries.into(), &inode)
            } else {
                ReaddirIter::unordered(&inner.s3_path.bucket, &full_path, page_size, local_entries.into())
            }
        };

        Ok(Self {
            dir_ino,
            parent_ino,
            iter: AsyncMutex::new(iter),
            readded: Default::default(),
        })
    }

    /// Return the next inode for the directory stream. If the stream is finished, returns
    /// `Ok(None)`. Does not increment the lookup count of the returned inodes: the caller
    /// is responsible for calling [`remember()`] if required.
    pub(super) async fn next<OC: ObjectClient + Send + Sync>(
        &self,
        inner: &SuperblockInner<OC>,
    ) -> Result<Option<LookedUpInode>, InodeError> {
        if let Some(readded) = self.readded.lock().unwrap().take() {
            return Ok(Some(readded));
        }

        // Loop because the next entry from the [ReaddirIter] may be hidden from the file system,
        // if it has an invalid name.
        loop {
            let next = {
                let mut iter = self.iter.lock().await;
                let next_entry = iter.next(&inner.client).await?;
                
                // Check if the remote iteration has completed and populate cache if needed
                if next_entry.is_none() && iter.is_remote_complete() {
                    if let Ok(dir_inode) = inner.get(self.dir_ino) {
                        let ttl = inner.config.cache_config.dir_ttl;
                        let _ = dir_inode.set_complete_listing_cache(true, None, ttl);
                        println!("READDIR CACHE DEBUG: Populated cache for directory {} after remote iteration completed, TTL: {:?}", 
                                 self.dir_ino, ttl);
                    }
                }
                
                next_entry
            };

            if let Some(next) = next {
                let Ok(name) = next.name().try_into() else {
                    // Short-circuit the update if we know it'll fail because the name is invalid
                    warn!("{} has an invalid name and will be unavailable", next.description());
                    continue;
                };
                let remote_lookup = self.remote_lookup_from_entry(inner, &next);
                let lookup = inner.update_from_remote(self.dir_ino, name, remote_lookup)?;
                return Ok(Some(lookup));
            } else {
                return Ok(None);
            }
        }
    }

    /// Re-add an entry to the front of the queue if the consumer wasn't able to use it
    pub fn readd(&self, entry: LookedUpInode) {
        let old = self.readded.lock().unwrap().replace(entry);
        assert!(old.is_none(), "cannot readd more than one entry");
    }

    /// Return the inode number of the parent directory of this directory handle
    pub fn parent(&self) -> InodeNo {
        self.parent_ino
    }

    /// Create a [RemoteLookup] for the given ReaddirEntry if appropriate.
    fn remote_lookup_from_entry<OC: ObjectClient + Send + Sync>(
        &self,
        inner: &SuperblockInner<OC>,
        entry: &ReaddirEntry,
    ) -> Option<RemoteLookup> {
        match entry {
            // If we made it this far with a local inode, we know there's nothing on the remote with
            // the same name, because [LocalInode] is last in the ordering and so otherwise would
            // have been deduplicated by now.
            ReaddirEntry::LocalInode { .. } => None,
            ReaddirEntry::RemotePrefix { .. } => {
                let stat = InodeStat::for_directory(inner.mount_time, inner.config.cache_config.dir_ttl);
                Some(RemoteLookup {
                    stat,
                    kind: InodeKind::Directory,
                })
            }
            ReaddirEntry::RemoteObject {
                size,
                last_modified,
                etag,
                storage_class,
                restore_status,
                ..
            } => {
                let stat = InodeStat::for_file(
                    *size as usize,
                    *last_modified,
                    Some(etag.as_str().into()),
                    storage_class.as_deref(),
                    *restore_status,
                    inner.config.cache_config.file_ttl,
                );
                Some(RemoteLookup {
                    stat,
                    kind: InodeKind::File,
                })
            }
        }
    }
}

/// A single entry in a readdir stream. Remote entries have not yet been converted to inodes -- that
/// should be done lazily by the consumer of the entry.
#[derive(Debug, Clone)]
enum ReaddirEntry {
    RemotePrefix {
        name: String,
    },
    RemoteObject {
        /// Last component of the S3 Key.
        name: String,
        /// S3 Key for this object.
        full_key: String,
        /// Size of this object in bytes.
        size: u64,
        /// The time this object was last modified.
        last_modified: OffsetDateTime,
        /// Storage class for this object. Optional because this information may not be available when
        /// [ReaddirEntry] is loaded from disk.
        storage_class: Option<String>,
        /// Objects in flexible retrieval storage classes (such as GLACIER and DEEP_ARCHIVE) are only
        /// accessible after restoration.
        restore_status: Option<RestoreStatus>,
        /// Entity tag of this object.
        etag: String,
    },
    LocalInode {
        lookup: LookedUpInode,
    },
}

// This looks a little silly but makes the [Ord] implementation for [ReaddirEntry] a bunch clearer
#[derive(Debug, Clone, Copy, PartialOrd, Ord, PartialEq, Eq)]
enum ReaddirEntryKind {
    RemotePrefix,
    RemoteObject,
    LocalInode,
}

impl ReaddirEntry {
    fn name(&self) -> &str {
        match self {
            Self::RemotePrefix { name } => name,
            Self::RemoteObject { name, .. } => name,
            Self::LocalInode { lookup } => lookup.inode.name(),
        }
    }

    fn kind(&self) -> ReaddirEntryKind {
        match self {
            Self::RemotePrefix { .. } => ReaddirEntryKind::RemotePrefix,
            Self::RemoteObject { .. } => ReaddirEntryKind::RemoteObject,
            Self::LocalInode { .. } => ReaddirEntryKind::LocalInode,
        }
    }

    /// How to describe this entry in an error message
    fn description(&self) -> String {
        match self {
            Self::RemotePrefix { name } => {
                format!("directory '{name}'")
            }
            Self::RemoteObject { name, full_key, .. } => {
                format!("file '{name}' (full key {full_key:?})")
            }
            Self::LocalInode { lookup } => {
                let kind = match lookup.inode.kind() {
                    InodeKind::Directory => "directory",
                    InodeKind::File => "file",
                };
                format!("local {} '{}'", kind, lookup.inode.name())
            }
        }
    }
}

impl PartialEq for ReaddirEntry {
    fn eq(&self, other: &Self) -> bool {
        self.name() == other.name() && self.kind() == other.kind()
    }
}

impl Eq for ReaddirEntry {}

impl PartialOrd for ReaddirEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

// We sort readdir entries by name, and then by kind. So if two entries have the same name, a remote
// directory sorts before a remote object, which sorts before a local entry.
impl Ord for ReaddirEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.name()
            .cmp(other.name())
            .then_with(|| self.kind().cmp(&other.kind()))
    }
}

/// Iterator over [ReaddirEntry] items, which are entries that may not yet have inodes created for them.
///
/// This iterator delegates to one of two iterators,
/// depending on if the S3 implementation returns ordered results or not.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
enum ReaddirIter {
    Ordered(ordered::ReaddirIter),
    Unordered(unordered::ReaddirIter),
}

impl ReaddirIter {
    fn ordered(bucket: &str, full_path: &str, page_size: usize, local_entries: VecDeque<ReaddirEntry>) -> Self {
        Self::Ordered(ordered::ReaddirIter::new(bucket, full_path, page_size, local_entries))
    }

    fn unordered(bucket: &str, full_path: &str, page_size: usize, local_entries: VecDeque<ReaddirEntry>) -> Self {
        Self::Unordered(unordered::ReaddirIter::new(bucket, full_path, page_size, local_entries))
    }

    fn cached_ordered(bucket: &str, full_path: &str, page_size: usize, local_entries: VecDeque<ReaddirEntry>, inode: &Inode) -> Self {
        Self::Ordered(ordered::ReaddirIter::new_cached(bucket, full_path, page_size, local_entries, inode))
    }

    fn cached_unordered(bucket: &str, full_path: &str, page_size: usize, local_entries: VecDeque<ReaddirEntry>, inode: &Inode) -> Self {
        Self::Unordered(unordered::ReaddirIter::new_cached(bucket, full_path, page_size, local_entries, inode))
    }

    async fn next(&mut self, client: &impl ObjectClient) -> Result<Option<ReaddirEntry>, InodeError> {
        match self {
            Self::Ordered(iter) => iter.next(client).await,
            Self::Unordered(iter) => iter.next(client).await,
        }
    }

    /// Check if the remote iteration (S3 ListObjectsV2 calls) is complete
    fn is_remote_complete(&self) -> bool {
        match self {
            Self::Ordered(iter) => iter.is_remote_complete(),
            Self::Unordered(iter) => iter.is_remote_complete(),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum RemoteIterState {
    /// Next ListObjects call should use this continuation token
    InProgress(Option<String>),
    /// No more ListObjects calls to make
    Finished,
}

/// An iterator over [ReaddirEntry]s returned by paginated ListObjects calls to S3.
/// This iterator combines directories (common prefixes) and files (objects) into a single stream.
///
/// If the S3 implementation returns ordered results, this iterator will re-sort the stream to
/// account for common prefixes not being in lexicographic order (see the module comment).
#[derive(Debug)]
struct RemoteIter {
    /// Prepared entries in order to be returned by the iterator.
    entries: VecDeque<ReaddirEntry>,
    bucket: String,
    /// S3 prefix for the [RemoteIter], used when listing objects in S3.
    full_path: String,
    /// The maximum number of keys to be returned by a single S3 ListObjectsV2 request.
    page_size: usize,
    state: RemoteIterState,
    /// Does the S3 implementation return ordered results?
    ordered: bool,
}

impl RemoteIter {
    fn new(bucket: &str, full_path: &str, page_size: usize, ordered: bool) -> Self {
        Self {
            entries: VecDeque::new(),
            bucket: bucket.to_owned(),
            full_path: full_path.to_owned(),
            page_size,
            state: RemoteIterState::InProgress(None),
            ordered,
        }
    }

    async fn next(&mut self, client: &impl ObjectClient) -> Result<Option<ReaddirEntry>, InodeError> {
        if self.entries.is_empty() {
            let continuation_token = match &mut self.state {
                RemoteIterState::Finished => {
                    trace!(self=?self as *const _, prefix=?self.full_path, "remote iter finished");
                    return Ok(None);
                }
                RemoteIterState::InProgress(token) => token.take(),
            };

            trace!(self=?self as *const _, prefix=?self.full_path, ?continuation_token, "continuing remote iter");

            let result = client
                .list_objects(
                    &self.bucket,
                    continuation_token.as_deref(),
                    "/",
                    self.page_size,
                    self.full_path.as_str(),
                )
                .await
                .map_err(|e| InodeError::client_error(e, "ListObjectsV2 failed", &self.bucket, &self.full_path))?;

            self.state = match result.next_continuation_token {
                Some(token) => RemoteIterState::InProgress(Some(token)),
                None => RemoteIterState::Finished,
            };

            let prefixes = result
                .common_prefixes
                .into_iter()
                .map(|prefix| ReaddirEntry::RemotePrefix {
                    name: prefix[self.full_path.len()..prefix.len() - 1].to_owned(),
                });

            let objects = result
                .objects
                .into_iter()
                .map(|object_info| ReaddirEntry::RemoteObject {
                    name: object_info.key[self.full_path.len()..].to_owned(),
                    full_key: object_info.key,
                    size: object_info.size,
                    last_modified: object_info.last_modified,
                    storage_class: object_info.storage_class,
                    restore_status: object_info.restore_status,
                    etag: object_info.etag,
                });

            if self.ordered {
                // ListObjectsV2 results are sorted, so ideally we'd just merge-sort the two streams.
                // But `prefixes` isn't quite in sorted order any more because we trimmed off the
                // trailing `/` from the names. There's still probably a less naive way to do this sort,
                // but this should be good enough.
                let mut new_entries = prefixes.chain(objects).collect::<Vec<_>>();
                new_entries.sort();

                self.entries.extend(new_entries);
            } else {
                self.entries.extend(prefixes.chain(objects));
            }
        }

        Ok(self.entries.pop_front())
    }
}

/// Iterator implementation for S3 implementations that provide lexicographically ordered LIST.
///
/// See [self::ReaddirIter] for exact behavior differences.
mod ordered {
    use super::*;

    /// An iterator over [ReaddirEntry]s for a directory. This merges iterators of remote and local
    /// [ReaddirEntry]s, returning them in name order, and filtering out entries that are shadowed by
    /// other entries of the same name.
    #[derive(Debug)]
    pub struct ReaddirIter {
        remote: RemoteIter,
        local: LocalIter,
        next_remote: Option<ReaddirEntry>,
        next_local: Option<ReaddirEntry>,
        last_entry: Option<ReaddirEntry>,
    }

    impl ReaddirIter {
        pub(super) fn new(
            bucket: &str,
            full_path: &str,
            page_size: usize,
            local_entries: VecDeque<ReaddirEntry>,
        ) -> Self {
            Self {
                remote: RemoteIter::new(bucket, full_path, page_size, true),
                local: LocalIter::new(local_entries),
                next_remote: None,
                next_local: None,
                last_entry: None,
            }
        }

        pub(super) fn new_cached(
            _bucket: &str,
            _full_path: &str,
            _page_size: usize,
            local_entries: VecDeque<ReaddirEntry>,
            inode: &Inode,
        ) -> Self {
            // Create a cached iterator that uses the children from the inode instead of S3
            let cached_entries = Self::extract_cached_entries(inode, local_entries);
            
            // Create a finished remote iterator that won't make any S3 calls
            let mut remote = RemoteIter::new("", "", 0, true);
            remote.state = RemoteIterState::Finished; // Ensure no S3 calls
            
            Self {
                remote,
                local: LocalIter::new_with_cached(cached_entries),
                next_remote: None,
                next_local: None,
                last_entry: None,
            }
        }

        fn extract_cached_entries(inode: &Inode, mut local_entries: VecDeque<ReaddirEntry>) -> VecDeque<ReaddirEntry> {
            if let Ok(state) = inode.get_inode_state() {
                if let InodeKindData::Directory { children, .. } = &state.kind_data {
                    // Convert cached children to ReaddirEntry items, but skip local entries
                    let mut cached_entries: Vec<_> = children
                        .values()
                        .filter_map(|child_inode| {
                            // Skip children that are in writing_children (local entries)
                            if local_entries.iter().any(|local| {
                                if let ReaddirEntry::LocalInode { lookup } = local {
                                    lookup.inode.ino() == child_inode.ino()
                                } else {
                                    false
                                }
                            }) {
                                return None;
                            }

                            let child_state = child_inode.get_inode_state().ok()?;
                            match child_inode.kind() {
                                InodeKind::Directory => Some(ReaddirEntry::RemotePrefix {
                                    name: child_inode.name().to_string(),
                                }),
                                InodeKind::File => Some(ReaddirEntry::RemoteObject {
                                    name: child_inode.name().to_string(),
                                    full_key: child_inode.key().to_string(),
                                    size: child_state.stat.size as u64,
                                    last_modified: child_state.stat.mtime,
                                    storage_class: None,
                                    restore_status: None,
                                    etag: child_state.stat.etag.as_deref().unwrap_or("").to_string(),
                                }),
                            }
                        })
                        .collect();

                    cached_entries.sort();
                    cached_entries.append(&mut local_entries.into_iter().collect());
                    return cached_entries.into();
                }
            }
            local_entries
        }

        /// Return the next [ReaddirEntry] for the directory stream. If the stream is finished, returns
        /// `Ok(None)`.
        pub(super) async fn next(&mut self, client: &impl ObjectClient) -> Result<Option<ReaddirEntry>, InodeError> {
            // The only reason to go around this loop more than once is if the next entry to return is
            // a duplicate, in which case it's skipped.
            loop {
                // First refill the peeks at the next entries on each iterator
                if self.next_remote.is_none() {
                    self.next_remote = self.remote.next(client).await?;
                }
                if self.next_local.is_none() {
                    self.next_local = self.local.next();
                }

                // Merge-sort the two iterators, preferring the remote iterator if the two entries are
                // equal (i.e. have the same name)
                let next = match (&self.next_remote, &self.next_local) {
                    (Some(remote), Some(local)) => {
                        if remote <= local {
                            self.next_remote.take()
                        } else {
                            self.next_local.take()
                        }
                    }
                    (Some(_), None) => self.next_remote.take(),
                    (None, _) => self.next_local.take(),
                };

                // Deduplicate the entry we want to return
                match (next, &self.last_entry) {
                    (Some(entry), Some(last_entry)) => {
                        if last_entry.name() == entry.name() {
                            warn!(
                                "{} is omitted because another {} exist with the same name",
                                entry.description(),
                                last_entry.description(),
                            );
                        } else {
                            self.last_entry = Some(entry.clone());
                            return Ok(Some(entry));
                        }
                    }
                    (Some(entry), None) => {
                        self.last_entry = Some(entry.clone());
                        return Ok(Some(entry));
                    }
                    _ => return Ok(None),
                }
            }
        }

        /// Check if the remote iteration is complete
        pub(super) fn is_remote_complete(&self) -> bool {
            self.remote.state == RemoteIterState::Finished
        }
    }

    /// An iterator over local [ReaddirEntry]s listed from a directory at the start of a [ReaddirHandle]
    #[derive(Debug)]
    struct LocalIter {
        entries: VecDeque<ReaddirEntry>,
    }

    impl LocalIter {
        fn new(entries: VecDeque<ReaddirEntry>) -> Self {
            Self { entries }
        }

        fn new_with_cached(entries: VecDeque<ReaddirEntry>) -> Self {
            Self { entries }
        }

        fn next(&mut self) -> Option<ReaddirEntry> {
            self.entries.pop_front()
        }
    }
}

/// Iterator implementation for S3 implementations that do not provide lexicographically ordered
/// LIST (i.e., S3 Express One Zone).
///
/// See [self::ReaddirIter] for exact behavior differences.
mod unordered {
    use std::collections::HashMap;

    use super::*;

    /// An iterator over [ReaddirEntry]s for a directory, where the remote entries are not available
    /// in order. This implementation returns all the remote entries first, and then returns the
    /// local entries that have not been shadowed.
    #[derive(Debug)]
    pub struct ReaddirIter {
        remote: RemoteIter,
        /// Local entries to be returned.
        /// Entries may be removed from this collection if entries of the same name are returned by [Self::remote].
        local: HashMap<String, ReaddirEntry>,
        /// Queue of local entries to be returned, prepared based on the contents of [Self::local].
        local_iter: VecDeque<ReaddirEntry>,
    }

    impl ReaddirIter {
        pub(super) fn new(
            bucket: &str,
            full_path: &str,
            page_size: usize,
            local_entries: VecDeque<ReaddirEntry>,
        ) -> Self {
            let local_map = local_entries
                .into_iter()
                .map(|entry| {
                    let ReaddirEntry::LocalInode { lookup } = &entry else {
                        unreachable!("local entries are always LocalInode");
                    };
                    (lookup.inode.name().to_owned(), entry)
                })
                .collect::<HashMap<_, _>>();

            Self {
                remote: RemoteIter::new(bucket, full_path, page_size, false),
                local: local_map,
                local_iter: VecDeque::new(),
            }
        }

        pub(super) fn new_cached(
            _bucket: &str,
            _full_path: &str,
            _page_size: usize,
            local_entries: VecDeque<ReaddirEntry>,
            inode: &Inode,
        ) -> Self {
            // Create a cached iterator that uses the children from the inode instead of S3
            let cached_entries = Self::extract_cached_entries(inode, local_entries);
            
            // For unordered cached entries, we need to separate remote entries from local entries
            let mut remote_iter = RemoteIter::new("", "", 0, false);
            let mut local_map = HashMap::new();
            
            // Populate the remote iterator with cached entries that are remote
            for entry in cached_entries {
                match &entry {
                    ReaddirEntry::LocalInode { lookup } => {
                        local_map.insert(lookup.inode.name().to_owned(), entry);
                    },
                    ReaddirEntry::RemotePrefix { .. } | ReaddirEntry::RemoteObject { .. } => {
                        remote_iter.entries.push_back(entry);
                    },
                }
            }
            // Mark the remote iterator as finished since we've populated it with cached data
            remote_iter.state = RemoteIterState::Finished;

            Self {
                remote: remote_iter,
                local: local_map,
                local_iter: VecDeque::new(),
            }
        }

        fn extract_cached_entries(inode: &Inode, mut local_entries: VecDeque<ReaddirEntry>) -> VecDeque<ReaddirEntry> {
            if let Ok(state) = inode.get_inode_state() {
                if let InodeKindData::Directory { children, .. } = &state.kind_data {
                    // Convert cached children to ReaddirEntry items, but skip local entries
                    let mut cached_entries: Vec<_> = children
                        .values()
                        .filter_map(|child_inode| {
                            // Skip children that are in writing_children (local entries)
                            if local_entries.iter().any(|local| {
                                if let ReaddirEntry::LocalInode { lookup } = local {
                                    lookup.inode.ino() == child_inode.ino()
                                } else {
                                    false
                                }
                            }) {
                                return None;
                            }

                            let child_state = child_inode.get_inode_state().ok()?;
                            match child_inode.kind() {
                                InodeKind::Directory => Some(ReaddirEntry::RemotePrefix {
                                    name: child_inode.name().to_string(),
                                }),
                                InodeKind::File => Some(ReaddirEntry::RemoteObject {
                                    name: child_inode.name().to_string(),
                                    full_key: child_inode.key().to_string(),
                                    size: child_state.stat.size as u64,
                                    last_modified: child_state.stat.mtime,
                                    storage_class: None,
                                    restore_status: None,
                                    etag: child_state.stat.etag.as_deref().unwrap_or("").to_string(),
                                }),
                            }
                        })
                        .collect();

                    cached_entries.append(&mut local_entries.into_iter().collect());
                    return cached_entries.into();
                }
            }
            local_entries
        }

        /// Return the next [ReaddirEntry] for the directory stream. If the stream is finished, returns
        /// `Ok(None)`.
        pub(super) async fn next(&mut self, client: &impl ObjectClient) -> Result<Option<ReaddirEntry>, InodeError> {
            if let Some(remote) = self.remote.next(client).await? {
                self.local.remove(remote.name());
                return Ok(Some(remote));
            }

            if !self.local.is_empty() {
                self.local_iter.extend(self.local.drain().map(|(_, entry)| entry));
            }

            Ok(self.local_iter.pop_front())
        }

        /// Check if the remote iteration is complete
        pub(super) fn is_remote_complete(&self) -> bool {
            self.remote.state == RemoteIterState::Finished
        }
    }
}

#[derive(Debug, Clone)]
pub struct DirectoryEntryReaddir {
    pub lookup: LookedUpInode,
    pub offset: i64,
    pub name: OsString,
    pub generation: u64,
}

#[derive(Debug)]
pub struct DirHandle {
    #[allow(unused)]
    ino: InodeNo,
    pub handle: AsyncMutex<ReaddirHandle>,
    offset: AtomicI64,
    pub last_response: AsyncMutex<Option<(i64, Vec<DirectoryEntryReaddir>)>>,
}

impl DirHandle {
    pub fn new(ino: InodeNo, readdir_handle: ReaddirHandle) -> Self {
        Self {
            ino,
            handle: AsyncMutex::new(readdir_handle),
            offset: AtomicI64::new(0),
            last_response: AsyncMutex::new(None),
        }
    }
    pub fn offset(&self) -> i64 {
        self.offset.load(Ordering::SeqCst)
    }

    pub fn next_offset(&self) {
        self.offset.fetch_add(1, Ordering::SeqCst);
    }

    pub fn rewind_offset(&self) {
        self.offset.store(0, Ordering::SeqCst);
    }
}
