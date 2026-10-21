# Research: how Two-Node OpenShift with Fencing (TNF) handles etcd

Scope: input for designing `twok` (two-node etcd with an external arbiter instead of BMC fencing).

## Sources (all read locally, shallow clones under `<workdir>/`)

| Alias | Source | Revision |
| --- | --- | --- |
| **ENH** | `enh/enhancements/two-node-fencing/tnf.md` (github.com/openshift/enhancements), plus its two SVG flowcharts | shallow clone, 2026-10-21, `last-updated: 2026-01-16` |
| **TNA-ENH** | `enh/enhancements/arbiter-clusters.md` | same clone |
| **RA** | `ra/heartbeat/podman-etcd` (github.com/ClusterLabs/resource-agents), 3719 lines, read in full | `14710fe0779d6826091d0b6a0996107e7bfa69be` (2026-10-20) |
| **CEO** | `ceo/` (github.com/openshift/cluster-etcd-operator): `pkg/tnf/**`, `pkg/cmd/rev/rev.go`, `bindata/etcd/pod.gotpl.yaml`, `bindata/etcd/tnf/cluster-restore-tnf.sh` | `88ac66d6714baa24466adbf697a75b0781b1362c` (2026-10-21) |
| **DOC-TNA** | <https://docs.okd.io/4.22/installing/installing_two_node_cluster/about-two-node-arbiter-installation.html> | fetched 2026-10-21 |
| **DOC-TNF-PREP** | <https://docs.okd.io/4.22/installing/installing_two_node_cluster/installing_tnf/installing-two-node-fencing.html> | fetched 2026-10-21 |
| **DOC-TNF-DEGRADED** | <https://docs.okd.io/4.22/installing/installing_two_node_cluster/installing_tnf/operating-a-degraded-tnf.html> | fetched 2026-10-21 |
| **DOC-TNF-POST** | <https://docs.okd.io/4.22/installing/installing_two_node_cluster/installing_tnf/install-post-tnf.html> | fetched 2026-10-21 |
| **BLOG** | https://www.redhat.com/en/blog/two-node-openshift-fencing-improves-reliability-edge | local copy `redhat.html` |

Note on docs naming: there is no page called `about-two-node-fencing-installation`. The fencing pages are under `installing_tnf/`, listed above.

### Architecture in one paragraph

Pacemaker/Corosync run on both nodes. They manage `kubelet` (systemd resource) and `etcd`. etcd is a promotable-less **clone** of the OCF agent `ocf:heartbeat:podman-etcd`, created with `notify=true interleave=true migration-threshold=60` (CEO `pkg/tnf/pkg/pcs/etcd.go:32`). It is ordered and colocated after `kubelet-clone` (`etcd.go:59`). Each node has one `fence_redfish` STONITH device (CEO `pkg/tnf/pkg/pcs/fencing.go:239`). The RA keeps all of its coordination state in **Pacemaker CIB node attributes**, both permanent and reboot-lifetime ones. It reads etcd freshness from `/var/lib/etcd/revision.json`, which an `etcd-rev` sidecar writes (CEO `bindata/etcd/pod.gotpl.yaml:360-375`). CEO stops managing membership once the transition to Pacemaker is done (`pkg/operator/clustermembercontroller/clustermembercontroller.go:119-124`: "skipping member management: DualReplica cluster has completed transition to external etcd (Pacemaker)").

---

## a) On partition or node loss, who survives and how is the tie broken?

**The answer is fencing-race only, with a static priority by node name.** The raft leader is not preferred, and neither is data freshness.

- ENH:503-504: "This RHEL-HA initialization script will also configure a fencing priority for the nodes - alphabetically by name. The priority takes the form of a delay, where the second node will wait 20 seconds to prevent parallel fencing operations…"
- ENH:609-610 (Network Failure timeline): "Both sides retain C-quorum and initiate fencing of the other side. RHEL-HA's fencing priority avoids parallel fencing operations… One side wins, pre-configured as Node1".
- The code differs from the enhancement on the delay value. CEO `pkg/tnf/pkg/pcs/fencing.go:19-22` sets `firstPcmkDelayBase = "10s"` and `defaultPcmkDelayBase = "1s"`. Lines 87-91 apply the 10s value to `nodeNames[0]`. Nodes come from `config.getClusterConfig`, which sorts them by name (`pkg/tnf/pkg/config/cluster.go:51-53`, `nodes.Items[i].Name < nodes.Items[j].Name`). The device is created with `pcmk_host_list=<that node> pcmk_delay_base=<delay>` (`fencing.go:239`). The delay therefore applies to the device that fences the alphabetically first node, which **protects** that node: it is the one that wins a symmetric partition. So the code uses 10s vs 1s, not the 20s in ENH.
- BLOG: "This is done heuristically by establishing different delay times between failure detection and the start of the fencing operation on each node."
- The RA also starts recovery from etcd itself. On each monitor (every 30s, RA:298), `check_peer` runs `member list`. If that fails, `detect_cluster_leadership_loss` runs (RA:2114-2117, 1888-1930). If the single reachable endpoint reports `"no leader"`, the RA calls `try_set_force_new_cluster`. On success it calls `set_standalone_node` and returns `OCF_ERR_GENERIC`, so Pacemaker restarts the resource and `start` performs the force-new-cluster.
- If both nodes claim `force_new_cluster` (FNC) at once, `fnc_conflict_verdict` (RA:1443-1551) decides. It checks, in order:
  1. `member_removal_lock`: a node that gracefully left the member list loses.
  2. Earlier claim epoch wins.
  3. Fallback: the higher `revision` attribute wins.
  4. Equal revision and equal cluster_id gives `EQUAL`: both withdraw and start normally.
  5. Otherwise `ADMIN`: refuse and require an operator.
- **Leader preference: not found.** Neither ENH nor RA nor CEO uses the etcd raft leader identity in survivor selection. The only "leader" logic is detecting that there is *no* leader (RA:1897).
- **Skeptical notes:**
  - The RA has no check of its own that the peer was fenced. `grep -i 'stonith\|fenc'` in RA matches only comments (RA:1614, 1913). Safety relies on Pacemaker ordering: Pacemaker will not run the start/notify cycle with an UNCLEAN peer until fencing succeeds.
  - ENH:749 states the invariant: "RHEL-HA requires fencing of a presumed dead peer before restarting etcd as a cluster-of-one".
  - Under a true partition, both nodes' monitors can set FNC. Each node sees its own CIB replica, and the CIB is not shared across the partition. The claim-time verdict therefore cannot really arbitrate across a partition. Fencing is what makes it safe: the loser is powered off.

## b) How the survivor reconfigures etcd into a cluster of one

**Mechanism: etcd's `force-new-cluster: true` on the existing data dir, with a revision bump. The dead peer is not removed with `member remove`. Afterwards the peer is re-added as a learner.**

Steps in `podman_start`, in order:

1. Refresh the node's CIB attributes: `node_ip`, `cluster_id` and `revision` (RA:2594-2596). The last two are read from `/var/lib/etcd/revision.json`.
2. Wait until CEO's static etcd pod is gone (RA:2603-2616).
3. **FNC panic rescue** (RA:2630-2718). If the previous container log contains both `panic: removed all voters` and `membership: ID removed`, this node's WAL ends with a self-removal. The node gives up FNC and records `member_removal_lock`. If the peer also holds that lock, start fails with "operator intervention required".
4. Startup routing decides between FNC, learner and normal start (RA:2720-2998):
   - If this node holds `member_removal_lock`, it becomes a learner.
   - If the peer holds it, this node claims FNC.
   - Otherwise the decision uses FNC holders, notify vars (active and starting resource counts) and `compare_revision` (local `revision.json` vs the peer's CIB `revision`, RA:2432-2459).
   - With 0 active peers and 1 starting (only us), we may FNC only if our revision is not `older` (RA:2923-2949). Otherwise: "local revision is older and peer is not starting: cannot start".
5. **Post-set guard** (RA:3000-3049). Wait up to 10s until our own FNC write is visible in the local CIB replica and we are the sole holder. If `learner_node` says we are a learner, drop FNC.
6. `prepare_env` (RA:541-592). With FNC set, `ETCD_INITIAL_CLUSTER_STATE="new"` and `ETCD_INITIAL_CLUSTER="$NODENAME=$NODEIPURL:2380"`.
7. `generate_etcd_configuration` (RA:621-668) writes a YAML config containing:

   ```text
   force-new-cluster: true
   force-new-cluster-bump-amount: $BUMP_REV
   ...
   experimental-initial-corrupt-check: true
   experimental-max-learners: 1
   ```

   `compute_bump_revision` (RA:594-619) uses 20% of `maxRaftIndex` from revision.json, capped at the 1e9 default (`ETCD_REVISION_BUMP_PERCENTAGE=0.2`, `ETCD_BUMP_REV_DEFAULT=1000000000`, RA:3667-3668). This mirrors CEO's quorum-restore pod.
8. **Data dir on the survivor:** reused as is. It is not wiped and not backed up on this path; `archive_data_folder` is called only on the learner path (RA:3106). The container may be recreated, in which case the old one is renamed `etcd-previous` and config files are tarred (RA:370-414).
9. Start the container and wait for the monitor command to pass (RA:3189-3225). Clear `member_removal_lock`.
10. Still holding FNC (RA:3230-3289): `add_member_as_learner <peer> <peer_ip>` runs `etcdctl member add <peer> --peer-urls=https://<ip>:2380 --learner` (RA:1124-1174). It then calls `clear_force_new_cluster` and `set_standalone_node`. If "Peer URLs already exists", the RA runs `remove_etcd_member_by_ip` and retries (RA:1162-1165). If the peer's Machine is being deleted, the learner add is skipped (RA:3236-3238).

ENH matches at a high level (ENH:582-583): "pacemaker restarting the etcd on the surviving node with a new cluster ID as a cluster-of-one". ENH:611: "Pacemaker on Node1 restarts etcd forcing a new cluster with old state to recover E-quorum. Node2 is added to etcd members list as learning member."

**Discrepancy.** DOC-TNF-DEGRADED says: "The etcd Operator demotes the etcd member of the failed node to a non-voting learner." That is not what the code does. CEO skips member management in DualReplica, and the RA force-new-clusters and then *adds* a fresh learner.

## c) How the fenced node rejoins

- **Who decides.** The rejoining node's own RA decides in `podman_start` that it must be a learner. Reasons: the peer holds FNC (RA:2857-2861), the peer is active standalone (RA:2883-2885), our revision is `older` (RA:2972-2974), or we hold `member_removal_lock` (RA:2744-2751).
- **Who adds it to the member list.** The **survivor's** RA, through `etcdctl member add --learner`. It does this right after FNC (RA:3239), from its monitor (`manage_peer_membership`, RA:2065-2073), or from `pre`-`start` notify (RA:3633-3636). Notify exists because "Pacemaker suppresses the peer's monitor during an active start/notify cycle" (RA:3627-3632). The survivor records the `learner_node` CIB attribute (RA:1147).
- **Waiting.** The joiner polls `learner_node` until it equals its own name, for up to 2 minutes (RA:3070-3104).
- **Data dir.** It is wiped. First `archive_data_folder` copies `member/snap/db` to `snapshot-<ts>.db` in `/var/lib/etcd` (reflink, keeping `max_backup_snapshots=3`, RA:755-834). Then `wipe_data_folder_for_learner` runs `rm -rf /var/lib/etcd/member` followed by `sync` (RA:701-710). The joiner starts with `initial-cluster-state: existing` and the full two-node `initial-cluster` (RA:551-564).
- **Promotion.** The survivor's RA monitor calls `reconcile_member_state` (RA:1735-1771). When exactly 2 started members exist, it runs `etcdctl member promote <id>`; failure is expected while the learner lags (RA:1712-1728). It then clears `standalone_node` and `learner_node`. Promotion is retried every monitor interval (30s). There is no explicit lag threshold: etcd's own learner-readiness check rejects early promotion.
- **ENH is wrong on this point.** ENH:604/621/637 say "CEO promotes the learning member as voting member". In code the RA promotes, and CEO's member controller is disabled in DualReplica (see the Architecture section).
- **Timing.** DOC-TNF-DEGRADED: "The returning member first rejoins as a learner (non-voting), receives a data snapshot, and replicates the log from the surviving member. After it has caught up, it is promoted to a voting member… This process typically takes about 15 to 25 minutes." This figure is end-to-end node recovery, not only etcd.
- **Gating by Corosync.** A rebooted node stays inert until it forms Corosync membership with its peer. ENH:614-616: "Node2 does not have C-quorum (requires forming a membership with its peer) … remains inert waiting for Node1".

## d) Preventing "a stale node restarts alone and serves"

**Primary mechanism:** Corosync two-node quorum. A node that boots alone gets no quorum, so Pacemaker starts nothing.

- ENH:145-146: "Upon rebooting, the RHEL-HA components ensure that a node remains inert (not running cri-o, kubelet, or etcd) until it sees its peer. If the failed peer is likely to remain offline for an extended period, admin confirmation is required…"
- ENH:649-652: "Node1 boots but can not gain quorum before Node2 joins the cluster due to a risk of fencing loop". Mitigation (Phase 1) is a manual script. Phase 2 is future work: "Node1 will gain quorum only if Node2 can be verified to be down by successfully querying its BMC status".
- DOC-TNF-POST: in a dual power loss with one recoverable node, `pcs quorum status` shows "Quorum: 1 Activity blocked". The fix is `sudo pcs quorum unblock`. A last resort is `pcs resource debug-start etcd` with `OCF_RESKEY_CRM_meta_notify_start_resource='etcd'`, under the warning "ensure that the node being replaced is inaccessible. Otherwise, you risk etcd corruption."
- BLOG (cold boot): "a single node returning online will not start workloads. Because it lacks quorum, it cannot know if the other node possesses a more recent state." BLOG (survivor reboot): "If the cluster is already degraded … and the surviving node reboots … you cannot safely guarantee consistency upon restart. The other node might have been active during the outage. … this requires the second node to start up to re-establish communication, or manual administrative intervention via SSH."
- **Corosync config:** `two_node`/`wait_for_all` are **not set explicitly** in CEO. `grep two_node|wait_for_all` finds nothing. The quorum behaviour comes from pcs defaults for a 2-node `pcs cluster setup` (`pkg/tnf/pkg/pcs/cluster.go:101`). This is an inference from pcs behaviour, not stated in sources.

**Persisted state used to pick the "last survivor" (all in Pacemaker CIB, replicated to both nodes):**

| Attribute | Lifetime | Written by | Meaning / use |
| --- | --- | --- | --- |
| `revision` | permanent node attr | each RA on start, on every monitor, and twice on stop (RA:953-983, 2315, 3497, 3557) | `maxRaftIndex` from revision.json. Compared against the peer's in `compare_revision`. |
| `cluster_id` | permanent node attr | RA start/stop (RA:911-943) | etcd cluster ID. Equal revision with a different cluster_id causes start to fail (RA:2976-2981). |
| `standalone_node` | cluster-wide attr (no `--node`) | survivor after FNC, and monitor while the peer is missing (RA:1649-1660, 2072, 3271) | "I am the sole voter". On a cold start with no peer running it allows FNC; leave-on-stop is skipped (RA:3311). |
| `learner_node` | cluster-wide attr | survivor after `member add --learner` | Joiner waits for it. Cleared on promotion and on stop. |
| `force_new_cluster` | **reboot** lifetime, per node, value is the claim epoch | `try_set_force_new_cluster` (RA:1193-1273) | Single-holder lock for "I will seed". Lost on reboot or membership loss. |
| `member_removal_lock` | permanent node attr, epoch | node that gracefully left the member list on stop (RA:3347-3473) | That node's WAL is voterless: it must rejoin as a learner and must never FNC. The peer becomes the seeder. |
| `restart_no_leave` | reboot | CEO cert watcher (CEO `pkg/tnf/certwatch/watcher.go:115-201`) | Restart etcd without leaving the member list (cert rotation). |
| `node_ip`, `member_id` | permanent | RA | Bookkeeping. |

Local files: `/var/lib/etcd/revision.json`, written every **10s** by the `etcd-rev` sidecar (CEO `pkg/cmd/rev/rev.go:70` default path, `:40` poll interval), and `${HA_RSCTMP}/podman-container-last-running`, a heartbeat file cleared on reboot (RA:3669-3671).

**Important limitation of this state.** The CIB is replicated by Corosync only while both nodes are members. After a partition, each node's CIB copy is its own view. A survivor that reboots alone has no authoritative record that it was the last writer. That is exactly why TNF falls back to "manual intervention". The `standalone_node` attribute does persist in the CIB on disk, but it is only consulted once Pacemaker has quorum (RA:2900-2910).

## e) Loss of acknowledged writes when the survivor was a follower

- **Not found.** Nothing in ENH, RA, CEO or the docs discusses commit-index lag, or losing writes acknowledged to clients when the surviving node was a follower.
- No raft-leader preference exists (see a). The survivor is fixed by name.
- **Why this is mostly benign in etcd, and why it is not fully benign.** etcd acknowledges a write only after it is committed, which in a 2-member cluster means persisted on **both** members. So any acknowledged write is in both WALs. The surviving follower may lack only the leader's *commit index advancement* for the last entries. With force-new-cluster, etcd commits all entries present in the local WAL. Writes in flight but not yet acknowledged may also survive, which is safe. The remaining gap concerns entries the leader acknowledged after the follower persisted them: those are in the follower's WAL, so they survive. This analysis is mine and not from TNF sources; verify it against etcd's `--force-new-cluster` WAL replay.
- **The "revision" signal is weak.** `revision.json.maxRaftIndex` is the **maximum raft index across all endpoints**, not the local node's own index. `trySaveRevision` queries `--endpoints=$(ALL_ETCD_ENDPOINTS)` and takes `max` (CEO `pkg/cmd/rev/rev.go:107-155`, pod template `pod.gotpl.yaml:371`). It is sampled every 10s. While both nodes are healthy, both record roughly the same number. After a split it says nothing reliable about which local WAL is longer. It is a staleness heuristic for "who ran alone longer after the split", not a raft-safety check.
- **Graceful-leave hazard that TNF did handle.** A node that removed itself from the member list has a WAL ending in a self-removal ConfChange. FNC on that WAL panics with `removed all voters`. TNF handles this with `member_removal_lock`, which is the primary fix (RA:1347-1367, 2720-2729), and with a log-grep "panic rescue" as a backstop (RA:2630-2718). They also note that two concurrent `member remove` calls could corrupt the WAL, so they delay the alphabetically second node by `DELAY_SECOND_NODE_LEAVE_SEC=10` (RA:3319-3336, 3672).

## f) Arbiter (TNA) vs fencing (TNF): latency and limitations

- **TNA is a plain 3-member etcd, with the arbiter as a full voting member.**
  - DOC-TNA: "The arbiter node stores the full etcd data, maintaining an etcd quorum and preventing split brain. The arbiter node does not run the additional control plane components kube-apiserver and kube-controller-manager, nor does it run workloads."
  - TNA-ENH:62-63: "HA for a 2+1 arbiter node should match the HA guarantees of a 3 Node Cluster deployment."
  - There is no special recovery logic: quorum is ordinary raft majority.
- **Latency and placement.**
  - TNA-ENH:580-587: risks are "improper disk speeds below that recommended for etcd, or a bigger problem being network latency. Since etcd is sensitive to latency between members … the arbiter node must meet minimum requirements for ETCD to function properly, in disk and network speeds."
  - DOC-TNA: "the same networking requirements as a regular cluster for connectivity between machines apply."
  - No numeric latency budget is given in either source.
  - TNA-ENH Non-Goals (lines 73-86): "Running the arbiter node offsite", "Having a single arbiter supporting multiple clusters", "Running a virtualized arbiter node on the same cluster", "Moving from 2 + 1 to a conventional 3 node cluster".
- **How TNF positions itself.**
  - ENH:114 non-goal: "Achieving the same level of resilience or guarantees provided by a cluster with 3 control-plane nodes or 2 nodes with an arbiter".
  - BLOG: "While two-node OpenShift with arbiter offers a path that uses a small arbiter device to maintain quorum, it technically remains a three-node architecture."
- **TNF limitations, explicit.**
  - Cold boot of one node and reboot of a lone survivor need manual action (see d).
  - BMC/Redfish is mandatory. The fencing network should be isolated (ENH:526-527).
  - Fencing must be tested regularly (ENH:533-537, 785-786). `fencing_validator --disruptive` exists (DOC-TNF-POST).
  - Recovery target: "Recover the API server in less than 120s, as measured by the surviving node's detection of a failure" (ENH:106).
  - Degraded mode has zero fault tolerance and blocks upgrades, CA rotation and reboot-requiring MachineConfigs (DOC-TNF-DEGRADED, "Capabilities during degraded TNF operation" table).
  - A failed BMC means "manual recovery will be required by SSH-ing to the node" (ENH:959-960).

## g) Workloads and storage: what can be declared out of scope

- ENH:116-117 Non-Goals: "Workload resilience - see related Pre-DRAFT enhancement" and "Resilient storage - see future enhancement".
- ENH:121-123: "Adding worker nodes", "Supporting upgrade/downgrade paths between 2-node and other topologies", "Cluster upgrades when only a single node is available".
- ENH:767-768 and the Node Health Check section (ENH:462-465): other power managers, such as BMO, NHC, SNR and FAR, are disabled on this topology. Pacemaker is the only fencer.
- Workload-side behaviour that TNF does provide is outside etcd. Automatic tainting of the fenced node (`taint-fenced-node` and related, DOC-TNF-DEGRADED) speeds up eviction and PV reattachment. Third-party shared storage (DRBD/LINBIT, InfoScale, Ceph-based) relies on the fencing: "storage operations will completely halt upon a node failure and will only resume after a successful fencing operation" (BLOG).
- **For `twok`:** without fencing we cannot offer the "PVs and pods safe to reschedule" guarantee. Workload and storage failover can be declared out of scope, the same way TNF's enhancement does. This should be stated explicitly: without node isolation, the old node may still be running pods and writing to local or shared storage.

---

## Mechanisms worth copying

1. **Force-new-cluster on the survivor, with no `member remove` of the dead peer.** Use `force-new-cluster: true` plus `force-new-cluster-bump-amount` (20% of the last known raft index, capped at 1e9; RA:594-626). Then immediately `member add --learner` for the peer (RA:3239). The bump keeps watchers and caches monotonic after the reset.
2. **The loser always rejoins as a learner with a wiped `member/` dir**, backed up first (RA:3106-3109, 701-834). It never tries to merge or reuse a diverged WAL. Promotion is retried periodically and relies on etcd's learner-readiness check (RA:1712-1771). Set `experimental-max-learners: 1`.
3. **One seeder at a time, enforced by a claim lock.** The CIB claim pattern is: pre-check, write an epoch, settle, re-read, earlier claim wins, then a post-set guard waiting until our write is visible and sole (RA:1193-1273, 3000-3028). With an arbiter this becomes a single **compare-and-swap lease** on the arbiter: an etcd txn with lease, a K8s `Lease` with `resourceVersion`, or an S3 conditional put (`If-None-Match`/`If-Match`). That is strictly better than the CIB, because the CAS is linearizable and reachable from both sides of a partition.
4. **Record "who left gracefully".** The `member_removal_lock` claim is taken *before* `member remove` and kept on ambiguous failure (RA:3347-3473). Without it, FNC on a self-removed WAL panics. Store this record in the arbiter.
5. **Persist "last survivor / standalone" and "epoch"** so a cold-booting node can decide without its peer. TNF keeps `standalone_node` and `revision` in the CIB, which is useless across a partition. With an arbiter, the record (`{seeder, cluster_id, epoch/term, last_known_raft_index}`) lives off-box. That turns TNF's "manual intervention" cases (cold boot alone, survivor reboot) into automatic ones: the node may seed only if the arbiter says it holds the current seeder record.
6. **Refuse to guess.** The `ADMIN` verdict appears when data is insufficient (RA:1497-1550, 1579-1582) and fails loudly with a recovery hint. Equal revision with a different cluster_id is a hard error (RA:2976-2981).
7. **Leaving the member list on graceful stop**, with serialization so both nodes never leave at once (RA:3304-3474). It makes planned reboots cheap: the survivor keeps quorum without FNC.
8. **Stale-heartbeat escalation and the "wait for peer-initiated recovery" state** in monitor (RA:2166-2275). These avoid restart storms while the peer is the one seeding.
9. **Operational surface:** a health CR showing status (`PacemakerCluster`, ENH:669-705), a validator with a disruptive mode, and a restore script that clears all agent state then seeds via a single claim (CEO `bindata/etcd/tnf/cluster-restore-tnf.sh:104-121, 253-266`).

## Mechanisms that do not apply without BMC fencing

1. **Fencing as the safety proof.** TNF's whole split-brain argument is "RHEL-HA requires fencing of a presumed dead peer before restarting etcd as a cluster-of-one" (ENH:749). Without power-off, the old node may still be running etcd and serving reads and writes on its side of the partition. Its etcd loses quorum by itself (2 members, 1 reachable), so it stops accepting writes, but it can still serve serializable and stale reads, and kube-apiserver can serve from its watch cache. `twok` needs a replacement guarantee, for example:
   - the survivor must hold the arbiter lease;
   - the loser must have *provably* lost the lease (lease TTL plus clock-skew margin, or self-fencing with a watchdog/kill of etcd and apiserver when the lease is not renewed);
   - only then may the survivor FNC.

   This is the classic "lease + self-fence" pattern. It does not exist in TNF.
2. **Static fencing delays as the tie-breaker** (`pcmk_delay_base` 10s/1s by name, CEO `fencing.go:19-22, 87-91`). With an arbiter, the lease decides. A static name preference can stay as an optional bias, for example a delay before contending. Better: prefer the node that was raft leader or has the higher local applied index, if it can reach the arbiter. TNF does neither.
3. **Corosync quorum / `pcs quorum unblock` / `pcs stonith confirm`** (DOC-TNF-POST). These are the manual escape hatches for cases the arbiter should cover automatically. An equivalent manual override is still needed for "arbiter permanently lost and peer lost".
4. **Pacemaker clone notify vars** (`notify_active_resource`, `notify_start_resource`) used to infer "is the peer starting/active" (RA:1819-1854, 2863-2996). Without Pacemaker, we need our own peer-liveness view. It must not be trusted for safety decisions, only for liveness, because a partitioned view is unreliable.
5. **CIB as the coordination store.** It is replicated only inside the Corosync membership, so it partitions together with the nodes. Its "totally ordered" guarantee (RA:3000-3006) holds only inside one partition. Replace it with the arbiter.
6. **`revision.json.maxRaftIndex` as the freshness signal.** It is a cluster-wide max, sampled every 10s (CEO `rev.go:107-155`). Even in TNF it is only a heuristic. `twok` should use each node's **own** on-disk raft state, such as the last WAL index and term (readable offline with etcdutl/WAL tooling), recorded in the arbiter at seed time.
7. **The workload/storage safety that fencing provides** (taints, PV reattach after power-off, DRBD/SDS resuming only after fencing). We cannot offer it without node isolation. Declare it out of scope (see g).

## Discrepancies found between sources (worth remembering)

- Fencing delay: ENH says 20s, code says 10s/1s (CEO `fencing.go:19-22`).
- Learner promotion: ENH says CEO promotes. Code: the RA promotes (RA:1712-1771), and CEO member management is skipped in DualReplica.
- DOC-TNF-DEGRADED says "The etcd Operator demotes the etcd member of the failed node to a non-voting learner". Code: the RA force-new-clusters, then adds a new learner. There is no demotion.
- DOC-TNF-POST, dual power loss: "If one node has a more up-to-date etcd, Pacemaker attempts to fence the lagging node and start it as a learner." The RA has no fencing call. The lagging node just starts as a learner and wipes `member/` (RA:2972-2974, 3106-3109).
