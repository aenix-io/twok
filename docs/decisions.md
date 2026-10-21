# Decisions

Decisions taken while building twok, with the reasoning, so that they can be revisited with the reasoning in view. Newest last.

## 1. An external arbiter with upstream etcd, not a witness inside Raft

The original idea was a Raft witness: a third voter that stores no data, only terms and votes. That needs a fork of the raft library and of etcd, and the witness still has to persist a vote per election and be reachable in every election. The arbiter model gets the same tie-break with upstream etcd unmodified, and the arbiter is contacted only when the cluster has lost its leader. threek already showed that staying outside etcd is the cheaper path to maintain.

## 2. Generations instead of leases

A lease with a TTL is the common arbiter design, but a lease answers "who holds it now", not "whose data is current". A lease expires while the holder still serves, and the other member could take it over with stale data. twok stores a monotonic generation in the arbiter and in the cluster's own keyspace. A member may claim only the generation its own data belongs to, so stale data disqualifies itself. No clock takes part in a safety decision.

## 3. Raft fences the loser, not a BMC

Red Hat's two-node OpenShift fences the losing node through its BMC before the survivor goes on alone. For etcd alone this is not needed: a voter in a two-voter cluster cannot commit without the other, so the loser stops committing the moment the link breaks, whether or not it knows about the arbiter. twok therefore makes no attempt to stop the node, and says so. Workloads and storage on the losing node are out of scope. Stale serializable reads and stale watch caches on the loser are possible and documented.

## 4. The whole log of the survivor counts as committed

`--force-new-cluster` discards WAL entries after the member's own commit index. A follower learns the commit index from the leader's next message, so at the moment of a partition it may hold an entry that the leader committed and acknowledged but that the follower does not yet know is committed. twok rewrites the hard state so that the commit index equals the last log index before it forces the new cluster. The argument that this is safe with exactly two voters is in [design.md](design.md#the-followers-last-entries), and a test reproduces the situation. The last leader is still preferred as survivor through a delay, so that the rewrite usually has nothing to do.

## 5. twok is a daemon next to etcd, not etcd's parent

threek observes etcd through its API and never restarts it. The first twok drafts made twok etcd's parent process, first in its own image with the etcd binary copied in, then inside an image that stands in for etcd, because a takeover seemed to need etcd restarted with `--force-new-cluster` and a rejoin seemed to need a different initial cluster. Both are avoidable: the configuration changes that `--force-new-cluster` writes can be written into the log by twok itself, and a member can keep its log under a new identity instead of being rebuilt. With that, twok needs the same things threek has, plus the data directory and a way to stop the etcd process, and etcd's image and command line stay with the platform. The decision follows threek's final shape on purpose: one mechanism for etcd-operator and Talos, settings changed without restarting etcd, and etcd's security fixes not tied to twok's releases.

## 6. The returning member drives its own rejoin, the survivor promotes

Red Hat's resource agent has the survivor add the learner and promote it. twok lets the returning member ask the survivor to add it and rewrite its own log, because that member is the one that knows its data directory; the survivor's twok only answers the API calls, writes the generation once the learner is up, and promotes the learner when it is caught up. A half-finished rejoin is resumed from what the survivor's member list and the returning member's WAL say, not from a file.

## 7. Startup does not block on the arbiter

etcd is started by whoever runs it, before twok has looked at anything, and twok never holds it back. With two voters in its data a member cannot commit alone. With one voter in its data it is the survivor, and nobody can have claimed a newer generation without first being promoted into its cluster, which is in its own log. The arbiter is still read at startup when reachable, and a stale member found that way is taken through the rejoin.

## 8. All transitions go through the arbiter, including rejoin

A member whose etcd exits as "removed from the cluster" could deduce that the peer became the survivor, but twok still confirms it with the arbiter before changing the member's identity. The rejoin is not availability-critical, the survivor already serves, and one rule for every transition is easier to reason about than two.

## 9. The arbiter is verified at startup

Some S3-compatible stores accept `If-Match` and ignore it. twok writes to a probe key next to the record at startup and checks that a second conditional create fails. A backend that passes the write is refused with a clear error. The probe is the only write twok does to the arbiter outside a claim.

## 10. Research status, two members only

The project is published as research so that the failure model can be discussed on a running system. Exactly two voting members are supported, learners are ignored.

## 11. No state file: the generation lives in the cluster

A member needs to know which generation its data belongs to, so that it never claims a generation it was not a voter in. A file next to the data directory can disagree with the data after a crash in the wrong place. The key `/twok/generation`, written by the survivor right after its takeover, reaches the other member only through the Raft log, in the same stream as the membership changes of that takeover, so it cannot run ahead of or behind the data. twok keeps nothing on disk of its own.

## 12. Rejoin by keeping the log under a new member ID

Red Hat's rejoin discards the loser's data directory and starts it as a fresh learner, which copies the whole store and needs a different initial cluster on the next start. twok instead rewrites the loser's WAL so that it belongs to the new member ID the survivor assigned, and lets etcd start with its old flags: Raft's log matching lets the member catch up from where it was, and the removal of its old identity and its addition as a learner arrive as ordinary entries. This was recorded before the experiment ran. The experiment is positive on etcd 3.5.34 and 3.6.15, including a member that needs a snapshot and one with a diverged tail of its own; see [testing.md](testing.md).

## 14. The survivor's new entries carry the next term

etcd's `--force-new-cluster` appends its removals in the current term. twok appends them in the next term, with a vote for the survivor itself. The reason is the loser's own tail: a loser that was the leader may hold uncommitted entries of the current term at the same indexes, and Raft treats an entry with the same index and term as the same entry, so the loser would keep its own and never apply the removal; the experiment shows it failing with "member unknown" in that case. Nobody can have been elected in the next term without the survivor's vote, which it persists before giving, so no entry anywhere carries that term and the loser's tail is overwritten when it catches up.

## 13. Talos by design, etcd-operator without changes

Talos runs etcd as a system service and restarts it when it exits, so a static Pod with host PID and `/var/lib/etcd` mounted has what twok needs. This is taken from the Talos code and not tested here; the test cluster is not touched with `talosctl`. On etcd-operator the sidecar is injected by an admission policy as threek's is; the policy also mounts the data volume and sets `shareProcessNamespace`, both of which are Pod fields the policy may set. The operator needs no change to run twok. The changes proposed for threek in [cozystack/etcd-operator#379](https://github.com/cozystack/etcd-operator/pull/379) and [#380](https://github.com/cozystack/etcd-operator/pull/380) are not needed either. What the operator does badly with a cluster whose membership twok changes is listed in [etcd-operator.md](etcd-operator.md).
