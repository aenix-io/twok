// Package walops changes the membership and the identity of a stopped etcd
// member by rewriting its WAL, so that etcd, restarted afterwards with its
// original flags by whoever runs it, comes up in the new configuration.
//
// twok runs next to etcd and cannot change etcd's flags. Once a data
// directory holds a WAL, etcd ignores --initial-cluster and
// --initial-cluster-state, and everything it needs to know about the
// cluster comes from the WAL, the snapshot files and its backend. These
// operations therefore work on the WAL, with etcd's own WAL package, and
// touch nothing else in the data directory.
//
//   - Inspect reads the WAL and reports the member's identity, its hard
//     state, its last index and the members the log knows about. It takes no
//     lock and works while etcd runs.
//   - Standalone turns a member of a two-voter cluster into a cluster of
//     one. It appends the conf changes that etcd's --force-new-cluster
//     appends, and marks them committed, but keeps every entry of the log,
//     where --force-new-cluster discards the entries above the member's
//     commit index.
//   - Reidentify gives the member a new member ID, so that it can rejoin the
//     survivor's cluster as the learner the survivor added, without losing
//     its data. It is experimental.
//
// # Why the whole log is kept
//
// A follower appends an entry to its WAL, fsyncs it and acknowledges it to
// the leader. The leader commits the entry, answers the client and tells the
// follower about the new commit index in its next message. The follower
// records that index in a hard state record, but raft does not ask for a
// sync when only the commit index changed (raft.MustSync), so the record
// stays in the WAL encoder's buffer until the next record that does need a
// sync. A follower that is killed, crashes or is cut off at that point holds
// acknowledged entries above its on-disk commit index.
//
// With exactly two voters it is safe to treat all of them as committed. A
// commit needs both acknowledgements, so every committed entry is in the
// survivor's log, and no entry of that log can have been contradicted by a
// committed one. The survivor's log is the committed log plus a tail of
// proposals whose clients got no answer, and committing that tail is what a
// newly elected raft leader does with its own log. The argument is in
// docs/design.md, "The follower's last entries". It does not hold for three
// or more voters, and it requires that the other member's data is never used
// again.
//
// # Versions
//
// The WAL format is the same in etcd 3.5, 3.6 and 3.7, and newer releases
// read older data directories for the in-place upgrade. This package is built
// with the WAL package of etcd 3.6 and reads and writes data directories of
// 3.5 and 3.6 members: it writes only metadata, snapshot, entry and hard state
// records, whose encoding has not changed since etcd 3.0, and conf change
// entries in the v1 format that etcd itself writes.
//
// # Locking
//
// etcd holds an exclusive lock on its WAL files while it runs. The
// operations that write take the same lock and fail with ErrLocked instead
// of writing under a running member.
package walops
