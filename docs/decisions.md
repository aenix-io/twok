# Decisions

Decisions taken while building twok, with the reasoning, so that they can be revisited with the reasoning in view. Newest last.

## 1. An external arbiter with upstream etcd, not a witness inside Raft

The original idea was a Raft witness: a third voter that stores no data, only terms and votes. That needs a fork of the raft library and of etcd, and the witness still has to persist a vote per election and be reachable in every election. The arbiter model gets the same tie-break with upstream etcd unmodified, and the arbiter is contacted only when the cluster has lost its leader. threek already showed that staying outside etcd is the cheaper path to maintain.

## 2. Generations instead of leases

A lease with a TTL is the common arbiter design, but a lease answers "who holds it now", not "whose data is current". A lease expires while the holder still serves, and the other member could take it over with stale data. twok stores a monotonic generation in the arbiter and in each member's state file. A member may claim only the generation its own data belongs to, so stale data disqualifies itself. No clock takes part in a safety decision.

## 3. Raft fences the loser, not a BMC

Red Hat's two-node OpenShift fences the losing node through its BMC before the survivor goes on alone. For etcd alone this is not needed: a voter in a two-voter cluster cannot commit without the other, so the loser stops committing the moment the link breaks, whether or not it knows about the arbiter. twok therefore makes no attempt to stop the node, and says so. Workloads and storage on the losing node are out of scope. Stale serializable reads and stale watch caches on the loser are possible and documented.

## 4. The whole log of the survivor counts as committed

`--force-new-cluster` discards WAL entries after the member's own commit index. A follower learns the commit index from the leader's next message, so at the moment of a partition it may hold an entry that the leader committed and acknowledged but that the follower does not yet know is committed. twok rewrites the hard state so that the commit index equals the last log index before it forces the new cluster. The argument that this is safe with exactly two voters is in [design.md](design.md#the-followers-last-entries), and a test reproduces the situation. The last leader is still preferred as survivor through a delay, so that the rewrite usually has nothing to do.

## 5. twok supervises the etcd process

threek observes etcd through its API and never restarts it. A takeover needs etcd restarted with different flags, so twok has to be etcd's parent process. The consequences: twok has to be in the container's command, the etcd binary has to be next to it, and etcd-operator cannot run twok without changes to the operator. This is accepted for a research project; what the operator would need is written down separately.

## 6. The joining member drives its own rejoin

Red Hat's resource agent has the survivor add the learner and promote it. twok lets the returning member do both through the survivor's client API. The survivor's twok then has no state about the peer at all, and a half-finished rejoin is resumed by the member that has the `joining` state file, which is the one that needs it.

## 7. Startup does not block on the arbiter

A member whose state file says `member` starts etcd whether or not the arbiter answers. With two voters in its data it cannot commit alone. With one voter in its data it is the survivor, and nobody can have claimed a newer generation without first being promoted into its cluster, which is in its own log. The arbiter is still read at startup when reachable, and a stale member found that way does not start.

## 8. All transitions go through the arbiter, including rejoin

A member whose etcd exits as "removed from the cluster" could deduce that the peer became the survivor, but twok still confirms it with the arbiter before discarding data. The rejoin is not availability-critical, the survivor already serves, and one rule for every transition is easier to reason about than two.

## 9. The arbiter is verified at startup

Some S3-compatible stores accept `If-Match` and ignore it. twok writes to a probe key next to the record at startup and checks that a second conditional create fails. A backend that passes the write is refused with a clear error. The probe is the only write twok does to the arbiter outside a claim.

## 10. Research status, two members only, no Talos, no operator

The project is published as research so that the failure model can be discussed on a running system. Exactly two voting members are supported, learners are ignored. Talos runs etcd itself and cannot hand the process to twok. etcd-operator builds member Pods with no way to change the command, so the operator integration is a design note, not code, until the operator gains the hooks.
