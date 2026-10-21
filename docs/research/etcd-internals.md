# etcd internals relevant to twok (force-new-cluster / learner rejoin)

Sources (shallow clones under <workdir>/):

- `etcd-3.5.12/`: tag v3.5.12 (the operator's version). Paths below without a prefix refer to this tree for 3.5.
- `etcd-3.5/`: branch release-3.5 head (v3.5.34, 2026-09-22).
- `etcd-3.6/`: branch release-3.6 head (v3.6.15, 2026-09-22).
- `raft-3.6/`: go.etcd.io/raft v3.6.0 (the raft module pinned by etcd 3.6.x server/go.mod). In 3.5 raft is in-tree at `etcd-3.5.12/raft/`.
- CHANGELOGs fetched from main: `cl35.md`, `cl36.md`.

etcd v3.7.x also exists now (latest v3.7.2), see section 10.

---

## 1. `--force-new-cluster` implementation

### 3.5 (v3.5.12): `restartAsStandaloneNode`, `server/etcdserver/raft.go:563-628`

```go
w, id, cid, st, ents := readWAL(cfg.Logger, cfg.WALDir(), walsnap, cfg.UnsafeNoFsync)   // :568

// discard the previously uncommitted entries                                           // :570
for i, ent := range ents {
    if ent.Index > st.Commit {
        cfg.Logger.Info("discarding uncommitted WAL entries", ...)
        ents = ents[:i]
        break
    }
}

// force append the configuration change entries                                        // :584
toAppEnts := createConfigChangeEnts(cfg.Logger, getIDs(cfg.Logger, snapshot, ents),
    uint64(id), st.Term, st.Commit)
ents = append(ents, toAppEnts...)

// force commit newly appended entries
err := w.Save(raftpb.HardState{}, toAppEnts)                                            // :595
...
if len(ents) != 0 {
    st.Commit = ents[len(ents)-1].Index
}
...
cl := membership.NewCluster(cfg.Logger)
cl.SetID(id, cid)
```

Discarding entries above HardState.Commit: YES, before appending the conf changes. The conf-change entries are numbered from `st.Commit+1` (`createConfigChangeEnts(..., index=st.Commit)`, `next := index + 1`, raft.go:680-735), so on disk they overwrite the discarded tail. WAL `ReadAll` semantics: a later entry with a lower index truncates (`ents = append(ents[:up], e)`, comment "potentially overriding some 'uncommitted' entries", wal/wal.go:446-456). The discard is permanent, not just in memory.

v3.5.12 uses the raw `HardState.Commit` from the WAL. Newer releases changed this. release-3.5 head (`etcd-3.5/server/etcdserver/raft.go:581-593`) and 3.6 (`etcd-3.6/server/etcdserver/bootstrap.go:572-584`) do:

```go
consistentIndex := ci.ConsistentIndex()
oldCommitIndex := st.Commit
// If only `HardState.Commit` increases, HardState won't be persisted
// to disk, even though the committed entries might have already been
// applied. This can result in consistent_index > CommitIndex.
...
st.Commit = max(oldCommitIndex, consistentIndex)
```

Verified by fetching tags: present in v3.5.22+ (absent in v3.5.21) and v3.6.1+ (absent in v3.6.0). CHANGELOG v3.5.22 "Fix `--force-new-cluster` can't remove all other members in a corner case" (etcd PR #20339, backport of #20055, issue #20009); v3.6.1 same (PR #20071). Root cause is described in etcd-io/raft PR #300 (closed, not merged): `MustSync` does not fsync a commit-only HardState change, so the on-disk Commit can lag the applied index.

Cluster ID and member ID are both kept. They come from the WAL metadata (`readWAL` returns `id, cid`; `cl.SetID(id, cid)`). 3.6: `meta.nodeID`, `meta.clusterID` from `openWALFromSnapshot` (bootstrap.go:622-627). No new IDs are generated.

Which members get removed: `getIDs` (raft.go:641-672) = `snapshot.Metadata.ConfState.Voters` plus AddNode/AddLearnerNode minus RemoveNode from the remaining WAL entries. Every ID other than self gets a `ConfChangeRemoveNode` entry. If self is not in the set, a `ConfChangeAddNode` for self is prepended with `PeerURLs: http://localhost:2380` (raft.go:696).

Caveat for 3.5.12: only `ConfState.Voters` from the snapshot are considered, not `ConfState.Learners`. A learner whose AddLearner entry is already compacted into the snapshot is NOT removed. Fixed in 3.6 (`etcd-3.6/server/storage/util.go:132-141` also iterates `ConfState.Learners`) and in release-3.5 head (`etcd-3.5/server/etcdserver/raft.go:670-673`); CHANGELOG v3.5.25 / v3.6.x "Fix `--force-new-cluster can't clean up learners after creating snapshot`" (PR #20896).

Snapshot / bbolt / v2store: nothing is rewritten at bootstrap. In 3.5 the server recovers v2store and the backend from the snapshot as usual (server.go:496-531), then `cl.SetStore(st); cl.SetBackend(be); cl.Recover(...)` (server.go:540-543) loads the OLD membership (still 2 members). Membership is rewritten only when the raft loop applies the appended conf-change entries through the normal path `apply` -> `applyConfChange` -> `cluster.RemoveMember(id, shouldApplyV3)` (server.go:2162-2178, 2352-2358). That deletes from v2store and, if `e.Index > consistent_index`, from bbolt, and records the ID as removed. In 3.5.12, if `consistent_index > HardState.Commit`, the new conf-change entries get indices <= consistent_index, so `shouldApplyV3 = ApplyV2storeOnly` and bbolt keeps the removed member. This is exactly the corner-case bug fixed in 3.5.22 / 3.6.1. In 3.5 v2store is the source of truth for membership, in 3.6 bbolt (v3store) is.

Is the survivor immediately leader? No, but within about 1 to 2 election timeouts. `raft.RestartNode(c)` (raft.go:625-627) starts as follower. Raft refuses to campaign while committed conf changes are unapplied (`raft/raft.go:768-769`: "cannot campaign at term %d since there are still %d pending configuration changes to apply"). Once the RemoveNode entries are applied the node is a single voter and wins (Pre-)Vote alone. `adjustTicks` (server.go:737-754) fast-forwards `ElectionTicks-1` only if `len(s.cluster.Members()) == 1` at the moment it runs. It runs in a goroutine at `Start()`, racing the application of the conf changes; otherwise it waits for peer connections (there are none) and does nothing. Expect leadership after a randomized election timeout (default `--election-timeout=1000ms`, randomized in [1x, 2x)). The supervisor must poll `Status` until `Leader == self`.

3.6 structure: `bootstrapWALFromSnapshot` (bootstrap.go:560-605) -> `CommitedEntries()` (697-711, same "discarding uncommitted WAL entries" loop) -> `NewConfigChangeEntries()` -> `AppendAndCommitEntries()` (722-731: `wal.w.Save(raftpb.HardState{}, ents)` then `st.Commit = last index`). Semantics are identical to 3.5 except for the `max(Commit, consistent_index)` fix and learner handling.

## 2. Loss of an acknowledged entry on the survivor

Confirmed from code. If follower F has E durably in its WAL but F's `HardState.Commit < E.Index`, `--force-new-cluster` on F truncates E (section 1). In 3.5.22+/3.6.1+ the `max(Commit, consistent_index)` only saves entries that F has already applied. F can only apply E after learning it is committed, so the fix does not help in this case.

How a follower's commit index advances (raft 3.5 `raft/raft.go`, raft 3.6 `raft-3.6/raft.go`): only from the leader's `Commit` field.

- MsgApp: `handleAppendEntries` -> `r.raftLog.maybeAppend(m.Index, m.LogTerm, m.Commit, m.Entries...)` (3.5 raft.go:1475) -> `l.commitTo(min(committed, lastnewi))` (raft/log.go:100).
- MsgHeartbeat: `handleHeartbeat` -> `r.raftLog.commitTo(m.Commit)` (3.5 raft.go:1507-1508; 3.6 raft.go:1830-1831). The leader attaches `commit := min(r.prs.Progress[to].Match, r.raftLog.committed)` (3.5 raft.go:502).
- MsgSnap restores to the snapshot index (3.6 raft.go:1913).

The window is not "up to one heartbeat interval". When the leader's commit advances on the follower's MsgAppResp it broadcasts at once: `if r.maybeCommit() { releasePendingReadIndexMessages(r); r.bcastAppend() }` (3.5 raft.go:1253-1257). `bcastAppend` -> `sendAppend` -> `maybeSendAppend(to, true /*sendIfEmpty*/)` (raft.go:423-425, 515-521) sends an empty MsgApp carrying the new commit. On a healthy network the window is about one RTT plus processing; heartbeats (every `--heartbeat-interval`, default 100ms) are only the fallback. In the partition scenario the window is unbounded: if the partition hits after F's ack and before the commit-carrying message arrives, F never learns about the commit, while the leader may already have applied E and answered the client.

A second, independent source of lag: even when F has learned the commit, the commit-only HardState is not fsynced, and may not even be flushed from the WAL's user-space buffer.

- `MustSync` (3.5 `raft/node.go:583-590`; raft 3.6 `rawnode.go:193-200`; unchanged in raft v3.7.0):

  ```go
  return entsnum != 0 || st.Vote != prevst.Vote || st.Term != prevst.Term
  ```

- `WAL.Save` (wal/wal.go:915-951) writes the state record via the encoder (an `ioutil.PageWriter`, wal/encoder.go:35-46) and only calls `w.sync()` (flushes the encoder and fdatasyncs, wal.go:789-797) when `mustSync`. A commit-only update is therefore lost on SIGKILL or crash. It is flushed by the next Save that carries entries, by a segment cut, or by `WAL.Close()` (calls `w.sync()`, wal.go:868-880) on graceful shutdown.

Consequence for twok: before `--force-new-cluster`, F's on-disk Commit may legitimately be behind entries that the old leader committed and acknowledged. With 2 voters (quorum = 2), every committed entry is in F's WAL (F's ack is required, and etcd followers persist before replying), so F's log has everything committed and only the commit marker lags. That motivates section 3.

## 3. Raising HardState.Commit externally before force-new-cluster

Safety argument. Setting `Commit = lastIndex(F's WAL)` on F, with F the only survivor and the loser wiped, is equivalent to what Raft does if F wins an election with its own log: a new leader keeps its whole log and commits the tail by committing an entry of its term. All committed entries are in F's log (quorum 2), so none is lost. The extra tail entries were proposed by clients that either got no acknowledgement (indeterminate outcome, committing is allowed) or did get one because they were in fact committed. Raft's Log Matching property guarantees F's log is consistent. Preconditions: the loser's data must never be used again (it is wiped and rejoins as a learner), and etcd on F must be stopped while editing. WAL files are flock'ed (`fileutil.TryLockFile(p, os.O_RDWR, ...)` in `openWALFiles`, wal.go:388), so `wal.Open` fails while etcd runs.

API:

|  | 3.5 | 3.6 |
| --- | --- | --- |
| package | `go.etcd.io/etcd/server/v3/wal` (+ `wal/walpb`) | `go.etcd.io/etcd/server/v3/storage/wal` (+ `storage/wal/walpb`); CHANGELOG 3.6 "Package `wal` was moved to `storage/wal`" |
| raftpb | `go.etcd.io/etcd/raft/v3/raftpb` | `go.etcd.io/raft/v3/raftpb` |

- `func Open(lg *zap.Logger, dirpath string, snap walpb.Snapshot) (*WAL, error)` (wal.go:310) opens read+write at the given snapshot. Doc: "The WAL cannot be appended to before reading out all of its previous records." `OpenForRead` (wal.go:323) is read-only.
- `func (w *WAL) ReadAll() (metadata []byte, state raftpb.HardState, ents []raftpb.Entry, err error)` (wal.go:432) returns the entries after `snap.Index`, the last HardState record seen, and the metadata (`etcdserverpb.Metadata{NodeID, ClusterID}`). In write mode it zeroes a torn tail and creates the encoder. It returns `ErrSnapshotNotFound` if no snapshot record matching `snap` was found (wal.go:520-523) and `ErrSnapshotMismatch` on a term mismatch. A partially written last record yields `io.ErrUnexpectedEOF`; etcd then runs `wal.Repair` (`readWAL` loop in storage.go:87+, 3.6 bootstrap.go:604-619). A tool should do the same or refuse.
- The WAL has to be opened at a snapshot in practice. `selectWALFiles`/`searchIndex` needs a segment whose start index is <= snap.Index, and ReadAll must see a snapshot record equal to `snap`. `walpb.Snapshot{}` (index 0) only works while the first segment, with the record written by `Create` (wal.go:172 `w.SaveSnapshot(walpb.Snapshot{})`), is still on disk; after a WAL purge it is gone. Do what etcd does: `walSnaps, _ := wal.ValidSnapshotEntries(lg, walDir)` (wal.go:555; keeps snapshot records with index <= the last HardState.Commit) and open at the newest one. etcd itself picks the newest one that also has a `.snap` file, via `snap.Snapshotter.LoadNewestAvailable(walSnaps)`; for this tool any valid one works, since it only needs the last index. If `ents` is empty, the last index is the snapshot index.
- `func (w *WAL) Save(st raftpb.HardState, ents []raftpb.Entry) error` (wal.go:915). A HardState with no entries is allowed. The only shortcut is `if raft.IsEmptyHardState(st) && len(ents) == 0 { return nil }`, and `saveState` skips only an empty HardState (wal.go:905-913). fsync: `mustSync := raft.MustSync(st, w.state, len(ents))`. After `ReadAll`, `w.state` is still zero (ReadAll never sets it, only `saveState` does, wal.go:909), so a Save of a HardState with non-zero Term will be synced. Do not rely on that: call `w.Sync()` (wal.go:816) explicitly, then `w.Close()` (which also syncs).
- Sketch:

  ```go
  snaps, _ := wal.ValidSnapshotEntries(lg, walDir)
  w, _ := wal.Open(lg, walDir, snaps[len(snaps)-1])
  _, st, ents, err := w.ReadAll()          // handle ErrUnexpectedEOF via wal.Repair + retry
  last := snaps[len(snaps)-1].Index
  if len(ents) > 0 { last = ents[len(ents)-1].Index }
  if last > st.Commit { st.Commit = last; _ = w.Save(st, nil); _ = w.Sync() }
  _ = w.Close()
  ```

  Keep `Term` and `Vote` unchanged.
- Startup validation if Commit > last index: raft `loadState` panics (3.5 `raft/raft.go:1696-1699`, 3.6 `raft-3.6/raft.go:2033-2036`):

  ```go
  if state.Commit < r.raftLog.committed || state.Commit > r.raftLog.lastIndex() {
      r.logger.Panicf("%x state.commit %d is out of range [%d, %d]", ...)
  ```

  and `commitTo` panics with "tocommit(%d) is out of range [lastIndex(%d)]. Was the raft log corrupted, truncated, or lost?" (3.5 raft/log.go:233-241, 3.6 log.go:320-328). Never set Commit beyond the last entry. With `--force-new-cluster` the in-memory Commit is reset anyway to the last appended conf-change index; the panic matters for a normal restart.
- Format 3.5 vs 3.6: identical record types (`metadataType..snapshotType = 1..5`: 3.5 wal.go:39-43; 3.6 storage/wal/wal.go:39-43, exported as `MetadataType..SnapshotType`), same `ReadAll`/`Save` logic (the diff shows only renames, plus 3.6 returning partial entries with `ErrSliceOutOfRange`). 3.6 must read 3.5 data dirs for the in-place upgrade path, so the 3.6 package can read and write a 3.5 WAL; writing only a HardState record introduces nothing 3.6-specific. Practical caveat: a tool cannot import both server/v3 3.5 and 3.6 in one module, pick one (the 3.6 wal works for both). 3.6 also has `wal.ReadWALVersion`/`MinimalEtcdVersion` (storage/wal/version.go) used by `etcdutl migrate`; irrelevant for this edit.

## 4. Removed-member behaviour

Survivor side (3.5.12 `server/etcdserver/api/rafthttp/http.go:393-401`, 3.6 http.go:411):

```go
if h.r.IsIDRemoved(uint64(from)) {
    h.lg.Warn("rejected stream from remote peer because it was removed", ...)
    http.Error(w, "removed member", http.StatusGone)   // 410 on stream GET
```

Raft message POST (pipeline): `EtcdServer.Process` returns `httptypes.NewHTTPError(http.StatusForbidden, "cannot process message from removed member")` (3.5 server.go:977-986; 3.6 server.go:719-723), i.e. 403.

Removed member side:

- Stream reader: `case http.StatusGone: ... reportCriticalError(errMemberRemoved, cr.errorc); return nil, errMemberRemoved` (rafthttp/stream.go:616-620).
- Pipeline: `checkPostResponse` has `case http.StatusForbidden: return errMemberRemoved` (rafthttp/util.go:110-111), then `if err == errMemberRemoved { reportCriticalError(err, p.errorc) }` (pipeline.go:166-169). Same in snapshot_sender.go:112-115.
- `errMemberRemoved = fmt.Errorf("the member has been permanently removed from the cluster")` (rafthttp/util.go:35).
- etcdserver run loop (3.5 server.go:1132-1135, 3.6 server.go:859-862):

  ```go
  case err := <-s.errorc:
      lg.Warn("server error", zap.Error(err))
      lg.Warn("data-dir used by this member must be removed")
      return
  ```

  The server then stops. `etcdmain` waits on `e.Server.StopNotify()` and calls `osutil.Exit(0)` (3.5 etcdmain/etcd.go:216-223; 3.6 etcdmain/etcd.go:196-202). The exit code is 0, indistinguishable from a clean stop. No marker file is written and nothing changes in the data dir. The supervisor has to detect this from the log lines above or from membership knowledge (arbiter state).
- A member that applies its own RemoveNode entry stops via `stopWithDelay(..., "the member has been permanently removed from the cluster")` (server.go:1391). That is not the twok path, since the loser never receives the survivor's conf changes.
- Restart with the same data dir: its local membership still lists itself and the survivor (the removal was never applied locally), so it starts normally and exits again only after it reaches the survivor's peer port and gets 410/403. The stream dialer starts right away, so in practice it exits moments after connecting. It does not exit on its own at startup.
- Never reaches the survivor: it runs indefinitely as a leaderless member of a 2-member cluster (Pre-Vote campaigns fail, `Status.Leader == 0`). Linearizable requests fail, but serializable reads keep serving its stale data. The supervisor must not leave it serving clients.
- The cluster ID is unchanged by force-new-cluster, so there is no `ErrClusterIDMismatch`; the 410/403 path is what fires.

## 5. Two voters, one unreachable

(a) CheckQuorum is hard-coded on in etcd, not a flag: `CheckQuorum: true` in `raft.Config` (3.5 raft.go:508, 551, 625; 3.6 bootstrap.go:532). Leader step-down: `tickHeartbeat` steps `MsgCheckQuorum` every `electionTimeout` ticks (3.5 raft/raft.go:655-663), and

```go
case pb.MsgCheckQuorum:
    ...
    if !r.prs.QuorumActive() {
        r.logger.Warningf("%x stepped down to follower since quorum is not active", r.id)
        r.becomeFollower(r.Term, None)
```

(3.5 raft.go:991-1004; 3.6 raft.go:1273-1277). Within 1 to 2 election timeouts the leader becomes a follower with `Lead = None`. etcd updates its lead from `rd.SoftState.Lead` (etcdserver/raft.go:178-190), so `Status.Leader = 0`. PreVote defaults to true (3.5 embed/config.go:504, 3.6 embed/config.go:695), so the isolated node does not inflate terms.

(b) No write can commit, the quorum of 2 voters is 2. Proposals without a leader are dropped by raft, "no leader at term %d; dropping proposal" returning `ErrProposalDropped` (3.5 raft/raft.go:1382-1383, 1419-1420; 3.6 raft.go:1680, 1717), or time out. etcd maps a context timeout to `ErrTimeout` / `ErrTimeoutDueToLeaderFail` / `ErrTimeoutDueToConnectionLost` (server.go:2685-2715). With `clientv3.WithRequireLeader(ctx)` the gRPC interceptor fails fast with `rpctypes.ErrGRPCNoLeader` (v3rpc/interceptor.go:64-66, 230-232).

(c) Linearizable reads fail, serializable reads work. `Range` calls `linearizableReadNotify` only `if !r.Serializable` (v3_server.go:120-121). ReadIndex without a leader is dropped by raft, "no leader at term %d; dropping index reading msg" (3.5 raft.go:1454; 3.6 raft.go:1761). etcd retries every 500ms and finally returns `ErrTimeout` "etcdserver: request timed out" (v3_server.go:857-914). A leader that has not yet stepped down also cannot answer ReadIndex without a quorum heartbeat round.

(d) The Status RPC is served locally with no quorum (3.5 v3rpc/maintenance.go:216-237; 3.6 maintenance.go:258-293):

```go
Leader:           uint64(ms.rg.Leader()),
RaftIndex:        ms.rg.CommittedIndex(),
RaftAppliedIndex: ms.rg.AppliedIndex(),
RaftTerm:         ms.rg.Term(),
...
IsLearner:        ms.cs.IsLearner(),
}
if resp.Leader == raft.None {
    resp.Errors = append(resp.Errors, etcdserver.ErrNoLeader.Error())   // "etcdserver: no leader"
}
for _, a := range ms.a.Alarms() { resp.Errors = append(resp.Errors, a.String()) }
```

3.6 adds `DbSizeQuota`, `StorageVersion`, `DowngradeInfo`. Status is also allowed on learners (`isRPCSupportedForLearner`: StatusRequest returns true, RangeRequest only if Serializable; v3rpc/util.go:140-149).

(e) MemberList.

- The server side has `MemberListRequest.Linearizable` in both versions. 3.5 `ClusterServer.MemberList` does `if r.Linearizable { LinearizableReadNotify }` (v3rpc/member.go:90-98); 3.6 `EtcdServer.MemberList` is the same (server.go:1674-1685).
- clientv3 3.5 (v3.5.12 and release-3.5 head): `func (c *cluster) MemberList(ctx context.Context) (*MemberListResponse, error)` always sends `&pb.MemberListRequest{Linearizable: true}` (client/v3/cluster.go:125-127). There is no `WithSerializable` option, so it fails without a leader. Workarounds: call the raw gRPC stub `pb.NewClusterClient(cli.ActiveConnection()).MemberList(ctx, &pb.MemberListRequest{Linearizable: false})`, or use the 3.6 client against a 3.5 server (the field exists server-side in 3.5).
- clientv3 3.6: `MemberList(ctx context.Context, opts ...OpOption)` with `Linearizable: !opt.serializable` (client/v3/cluster.go:125-127), so `cli.MemberList(ctx, clientv3.WithSerializable())` works without a leader. CHANGELOG 3.6 "Support serializable `MemberList` operation" (PR #15261), plus etcdctl `member list --consistency`. Note the 3.6 interface signature change.
- MemberList is not allowed on a learner (not in `isRPCSupportedForLearner`), so a supervisor on a learner node gets `ErrGRPCNotSupportedForLearner`.
- Use a client with only the local endpoint, otherwise the balancer may route the call elsewhere.

(f) `StatusResponse.raftTerm = 6`, `isLearner = 10`, `leader = 4`, `errors = 8` (api/etcdserverpb/rpc.proto:1185-1198), present in both versions.

## 6. Learner join flow

`MemberAddAsLearner` goes to `EtcdServer.AddMember` -> `mayAddMember` (only with `--strict-reconfig-check`, default true).

- 3.5.12 (server.go:1600-1628): the quorum check `IsReadyToAddVotingMember` is skipped for learners. However `isConnectedFullySince(s.r.transport, time.Now().Add(-HealthInterval), s.ID(), s.cluster.VotingMembers())` applies to learners too: the local member must have been connected to ALL voting members for `HealthInterval` (5s). With one voter (after force-new-cluster) this is trivially true, self counts (util.go:53-60). With 2 voters and one down it fails with `ErrUnhealthy`.
- 3.6 (server.go:1414-1443) replaces it with `isConnectedToQuorumAfterAddingNewMemberSince` (util.go:38-44), which returns true for a single-member cluster and otherwise needs `numConnected >= quorum(len(members)+1)`.
- The conf change itself then needs a raft commit (the quorum of voters is the single survivor after force-new-cluster).

Max learners: `ValidateConfigurationChange` does `if numLearners+1 > maxLearners { return ErrTooManyLearners }` (3.5 membership/cluster.go:333-343) with `const maxLearners = 1` (cluster.go:46, not configurable in 3.5). 3.6 has the `--max-learners` flag (embed/config.go:953) with `membership.DefaultMaxLearners = 1` (api/membership/cluster_opts.go:17).

Learner start: a fresh data dir with `--initial-cluster-state=existing` takes the `!haveWAL && !cfg.NewCluster` branch (3.5 server.go:410-436; 3.6 `bootstrapExistingClusterNoWAL`, bootstrap.go:290+). Steps: `VerifyJoinExisting` (config.go:218-231: local member present in `--initial-cluster`, no duplicate URLs, no discovery), `GetClusterFromRemotePeers` from the other URLs in `--initial-cluster`, then `ValidateClusterAndAssignIDs` (membership/cluster.go:808-835). The last one requires `len(existing) == len(local)` ("member count is unequal") and every existing member's PeerURLs to match an entry. So `--initial-cluster` must list exactly the survivor (with its current peer URLs as stored in membership) and the learner itself (with the peer URLs given to `MemberAddAsLearner`). The member ID comes from the existing cluster: the new learner gets a new ID from MemberAdd, and the old removed ID stays in the removed set.

Promote: `PromoteMember` -> `promoteMember` -> `mayPromoteMember` -> `isLearnerReady` (3.5 server.go:1726-1768; 3.6 server.go:1566-1610). It is forwarded over HTTP to the leader if the local node is not the leader.

```go
leaderMatch := rs.Progress[leaderID].Match
// the learner's Match not caught up with leader yet
if float64(learnerMatch) < float64(leaderMatch)*readyPercent {   // readyPercent = 0.9 (server.go:103)
    return ErrLearnerNotReady
```

3.6: `learnerReadyPercent := float64(learnerMatch) / float64(leaderMatch); if learnerReadyPercent < readyPercentThreshold` (0.9, server.go:111). The error is `ErrLearnerNotReady = "etcdserver: can only promote a learner member which is in sync with leader"`, mapped to gRPC `ErrGRPCLearnerNotReady` with `codes.FailedPrecondition` (api/v3rpc/rpctypes/error.go:46). With strict reconfig, `IsReadyToPromoteMember` (membership/cluster.go:670-694) counts the learner as started: 1 started voter + learner = 2 >= quorum(2), OK.

Known 3.5 bug relevant to twok: in v3.5.1 through v3.5.19 learner promotion is persisted only to v2store, not to bbolt (issue #19557, fixed in v3.5.20 by PR #19563; follow-ups v3.5.24 #20797, 3.6 auto-sync #19636 and `SyncLearnerPromotionIfNeeded`, 3.6 bootstrap.go:455). Harmless while staying on 3.5 (v2store is the 3.5 source of truth), but a member promoted on 3.5.12 reverts to learner, or fails with "too many learner members", after upgrading to 3.6. Upgrade 3.5.12 to >= 3.5.20 before going to 3.6.

Also v3.5.31/3.6.12 fixed "MemberUpdate implicitly and unexpectedly promotes a learner" (PR #21737/#21736). Do not call MemberUpdate on a learner on older versions.

## 7. Stale `--initial-cluster` / `--initial-cluster-state` with an existing data dir

These are ignored once a WAL exists.

- embed (3.5 embed/etcd.go:151-158; 3.6 embed/etcd.go:162-169): `if !isMemberInitialized(cfg) { urlsmap, token, err = cfg.PeerURLsMapAndToken("etcd") }`, where `isMemberInitialized` is `wal.Exist(waldir)` (embed/util.go:23-29). The initial cluster is not even parsed into the server config.
- etcdserver: 3.5 `case haveWAL:` (server.go:456-549) never touches `InitialPeerURLsMap`/`NewCluster`; membership comes from v2store/bbolt + WAL, IDs from WAL metadata. 3.6 `bootstrapCluster` goes to `case bwal != nil && bwal.haveWAL: bootstrapClusterWithWAL` (bootstrap.go:273-283, 365-388).
- The flags are still validated syntactically by `Config.Validate` (`--initial-cluster-state` must be `new` or `existing`, embed/config.go:700-701; only one of discovery/initial-cluster/DNS, :694). Discovery flags log "discovery token is ignored since cluster already initialized".

Restarting with the same flags after force-new-cluster is fine. The learner is the converse case: a learner whose data dir was wiped DOES use these flags.

## 8. `--force-new-cluster` on a data dir that is already a single-member cluster

`getIDs` returns `{self}`. `createConfigChangeEnts` produces no entries (self found, so no AddNode; the loop does `if id == self { continue }`, raft.go:680-735). `w.Save(raftpb.HardState{}, nil)` hits the `IsEmptyHardState && len(ents)==0` shortcut. Membership is untouched.

It is not strictly harmless. The "discard uncommitted WAL entries" step still runs and truncates any entries above the on-disk Commit. On a single node these may include entries that were committed, applied and acknowledged, because a commit-only HardState is not fsynced (section 2). In 3.5.12 that can leave bbolt (consistent_index) ahead of the raft log.

Recommendations:

- Never keep `--force-new-cluster` in the persistent flag set, run it exactly once. If the supervisor re-ran it after the learner was added (2 members), it would remove the new member again.
- Apply the Commit-raising step (section 3) before any force-new-cluster run.
- Prefer >= v3.5.22 (or >= v3.6.1) for the `max(Commit, consistent_index)` fix, and >= v3.5.25 for the learner-in-snapshot fix.

## 9. 3.6 changes relevant to twok

- `--force-new-cluster` is not deprecated in 3.6 or 3.7 (3.6 embed/config.go:959 "Force to create a new one member cluster."; v3.7.2 embed/config.go:768). The implementation moved to `bootstrap.go` + `server/storage/util.go` with the fixes above.
- No etcdutl/etcdctl command does offline reconfiguration. etcdutl 3.6 has `defrag`, `snapshot status|restore`, `migrate`, `hashkv`, `version`, `completion`; `etcdutl backup` was removed; `etcdctl snapshot restore/status` were removed in favour of etcdutl. `etcdutl snapshot restore` (a new cluster from a db snapshot, with new IDs) is the alternative to force-new-cluster, but it loses everything after the snapshot.
- The membership source of truth moves from v2store to bbolt (v3store), hence the importance of the 3.5 promotion-persistence bugs.
- `--max-learners` (default 1); serializable `MemberList` and `etcdctl member list --consistency=s`; `Status` adds `StorageVersion`, `DbSizeQuota`, `DowngradeInfo`.
- The learner-add health check is relaxed (quorum instead of full connectivity; a single member is always allowed).
- "v3 discovery" is only for bootstrapping new clusters (v2 discovery deprecated), not relevant.
- The `--snapshot-count` default was lowered to 10,000 (its deprecation was later revoked). raft is a separate module `go.etcd.io/raft/v3` v3.6.0, `MustSync` unchanged.
- Upgrade: 3.6 needs a data dir written by 3.5; go via >= 3.5.20 because of #19557.

## 10. Module versions (checked 2026-10-21)

From `go list -m -versions` and `gh api repos/etcd-io/etcd/releases`:

- `go.etcd.io/etcd/server/v3` and `client/v3`: latest v3.7.2 (2026-09-22). Latest 3.6 is v3.6.15 and latest 3.5 is v3.5.34 (both 2026-09-22). There is also `v3.8.0-alpha.0`.
- `go.etcd.io/raft/v3`: latest v3.7.0. etcd 3.6.x pins v3.6.0, etcd 3.7.2 pins v3.7.0. In raft v3.7.0 the protos became pointer-based (`MustSync(st, prevst *pb.HardState, ...)`, `GetVote()`), an API change for direct users. etcd 3.7 keeps the same force-new-cluster logic with `max(oldCommitIndex, consistentIndex)`.
- Go requirement (go.mod at tag): v3.5.12 server and client `go 1.20`; v3.5.34, v3.6.15 and v3.7.2 server and client `go 1.26` with `toolchain go1.26.8`.

## Summary of version differences

| Item | v3.5.12 | release-3.5 head (3.5.34) | 3.6.x |
| --- | --- | --- | --- |
| force-new-cluster Commit base | WAL HardState.Commit | max(Commit, consistent_index) (>= 3.5.22) | same (>= 3.6.1) |
| learners from snapshot ConfState removed | no | yes (>= 3.5.25) | yes |
| learner promotion persisted to bbolt | no (3.5.1 to 3.5.19 bug) | yes (>= 3.5.20) | yes |
| clientv3 serializable MemberList | no (raw gRPC only) | no | `WithSerializable()` |
| learner-add connectivity check | all voters connected | all voters connected | quorum; single member always OK |
| max learners | 1, fixed | 1, fixed | `--max-learners` (default 1) |
| WAL package | `server/v3/wal` | `server/v3/wal` | `server/v3/storage/wal` |
| removed member exit | log "server error" + "data-dir used by this member must be removed", exit 0 | same | same |
