# Research: cozystack/etcd-operator v0.5.4 and S3 conditional writes

All file:line references in Topic A are against `https://github.com/cozystack/etcd-operator` tag `v0.5.4` (commit `a60dd56`), cloned at `workdir/etcd-operator`.

## Topic A: etcd-operator v0.5.4 (etcd-operator.cozystack.io/v1alpha2)

### A1. How member Pods are built

- Renderer: `controllers/etcdmember_controller.go:631` `func (r *EtcdMemberReconciler) buildPod(member *lll.EtcdMember, clusterFormed bool) *corev1.Pod`, called from `ensurePod` (`:443-499`), which only creates a Pod if absent; an existing Pod is only checked for ownership and has its `role` label patched (`:446-455`, `reconcileRoleLabel` `:538-554`). The Pod spec is never re-templated or compared.
- Image: `controllers/helpers.go:121-127` `resolveEtcdImage` returns `repo + ":v" + member.Spec.Version`, where `repo` = operator-wide flag `--etcd-image-repository` / `ETCD_IMAGE_REPOSITORY`, else built-in `EtcdImage = "quay.io/coreos/etcd"` (`helpers.go:31`). Comment: "there is no per-cluster tag override that could disagree with it" (`helpers.go:119-120`). Note: the field doc at `etcdmember_controller.go:55` mentions "spec.image.repository", but no such field exists in the API (stale comment).
- Command (`Command:` not `Args:`, so it overrides the image entrypoint), `etcdmember_controller.go:705-723`: `etcd --name=<member> --data-dir=<dir> --listen-peer-urls=<scheme>://0.0.0.0:2380 --listen-client-urls=<scheme>://0.0.0.0:2379 --advertise-client-urls=<cAddr> --initial-advertise-peer-urls=<pAddr> --initial-cluster=<spec.initialCluster> --initial-cluster-token=<spec.clusterToken> --initial-cluster-state=<new|existing> --listen-metrics-urls=http://0.0.0.0:2381`, plus `optionFlags` (`:608-626`: only `--quota-backend-bytes`, `--auto-compaction-mode`, `--auto-compaction-retention`, `--snapshot-count`), plus TLS flags (`:729-779`, incl. `--peer-auto-tls` for adopted legacy clusters).
- `--initial-cluster-state=new` only if `member.Spec.Bootstrap && member.Status.MemberID == "" && !clusterFormed` (`:651-654`), otherwise `existing`.
- Env: the etcd container has no env at all. Only the restore initContainer has env (`:881-910`).
- Volumes: `data` (PVC `data-<member>` or tmpfs emptyDir) at `/var/lib/etcd`; `--data-dir` is `/var/lib/etcd` or `/var/lib/etcd/<subpath>` from annotation `etcd-operator.cozystack.io/data-dir-subpath` (`helpers.go:107-113`, `etcdmember_controller.go:703`); optional `tls-client`, `tls-peer` secrets (`:740-778`); `restore-src` PVC for restores (`:917-924`).
- Pod security: runAsNonRoot uid/gid 65532, drop ALL caps, no SA token (`:810-828`). Probes: liveness TCP 2380, readiness HTTP `/health` on 2381 (`:844-864`).
- The only initContainer is the restore agent (`/manager restore-agent`, operator image), added only when `member.Spec.Restore != nil` (`:784-789`, `:879-939`).

Override surface in v1alpha2 (`api/v1alpha2/etcdcluster_types.go:441-572`, `etcdmember_types.go:76-181`): `EtcdClusterSpec` has `replicas, version, storage, progressDeadlineSeconds, tls, auth, bootstrap, resources, additionalMetadata (labels/annotations only), affinity, topologySpreadConstraints, options (4 typed etcd flags), imagePullSecrets`. A grep for `podTemplate|extraArgs|initContainers|sidecar|image|env` over `api/v1alpha2/*_types.go` matches nothing relevant. **There is no way in the v1alpha2 API to override the image (only the operator-wide repository flag, tag pinned to `v<version>`), the command/entrypoint, add init containers or sidecars, add extra args, or add env.** The reserved annotations (`helpers.go:44-80`: `headless-service-name`, `data-dir-subpath`, `peer-auto-tls`) are migration plumbing, not user extension points.

### A2. Membership management

Bootstrap, always single-seed (`etcdcluster_controller.go:217-252`, `bootstrap` `:402-505`): "Bootstrap creates a single-member etcd cluster (member -0 only) with --initial-cluster-state=new. Once ClusterID is latched, scale-up adds the remaining members via MemberAdd." The seed (GenerateName `<cluster>-`, `Spec.Bootstrap=true`, `IsVoter` pre-stamped true `:467-471`) gets `InitialCluster` = only itself (`:489`). `tryDiscoverCluster` (`:509-611`) latches `status.clusterID` only when `MemberList` returns exactly one member matching the seed (`:580-603`). ClusterID is latched once and never re-checked afterwards.

Scale-up, one member at a time, as a learner (`:285-312`, `scaleUp` `:746-852`): this waits until every member is Ready (`allMembersReady`, `:306-309`). It then creates the EtcdMember CR, calls `MemberAddAsLearner([peerURL])` (`completePendingMember` `:893`), and builds the member's frozen `--initial-cluster` from etcd's member list verbatim (`:910-925`). Promotion goes through `tryPromoteLearner` (`:949-972`), which **promotes the first learner in etcd's MemberList, whoever added it** (`for _, m := range listResp.Members { if !m.IsLearner {continue}; ... MemberPromote(m.ID)`). It runs during scale-up (step 3, `:816`) and on every steady-state reconcile when current==desired (`:324-356` via `promotePendingLearner` `:998-1020`). The operator never calls the plain `MemberAdd`, even though the interface has it (`etcd_client.go:33`).

Scale-down (`scaleDown` `:1277-1314`): this deletes the newest EtcdMember CR. The member finalizer then calls `MemberRemove` (`handleDeletion` `:195-231`, `removeMemberFromEtcd` `:243-310`). Only one mutation runs at a time: an in-flight deletion blocks all scaling (`:261-266`). Going from 1 to 0 sets `Dormant` instead of deleting (`:1298-1307`).

Observed etcd membership vs EtcdMember objects. No code path diffs the two sets. The only places that read etcd's membership are: discovery (bootstrap only), `completePendingMember` (is this peer URL already added?), `tryPromoteLearner`, `syncIsVoter` (`:1034-1080`, which patches `IsVoter` for CRs whose peer URL appears and `continue`s for CRs that don't), `discoverMemberID` (only while `Status.MemberID==""`, `etcdmember_controller.go:1106-1123`), and `resolveMemberID` at deletion.
- (a) A member present in etcd with no EtcdMember: **ignored**. It is never removed. There is no orphan sweep, and `MemberRemove` is only called from the finalizer. If it is a learner, it **gets promoted** (see `tryPromoteLearner` above).
- (b) An EtcdMember whose member ID is no longer in etcd: **not detected**. `syncIsVoter` skips it (`default: continue`, `:1067-1068`), so `IsVoter` stays stale. `Status.MemberID` is written once (`etcdmember_controller.go:1111`) and never refreshed. Readiness stays tied to Pod readiness (`:1124-1129`). Consequences:
  - A removed member that restarts on its old data dir exits ("member removed") and crash-loops. Self-heal (`etcdContainerStuck` + `clusterHasQuorumWithout`, `:1093-1101`, `:997-1020`) requires `readyOthers >= desired/2+1`. With `desired=2` that means 2 ready *others*, which is impossible. **So a 2-replica cluster never self-heals, and the crash-looping member stays forever.**
  - If the member is re-added under a new ID by something else, the CR keeps the stale ID. On deletion, `resolveMemberID` trusts it (`:320-326`), and `MemberRemove(staleID)` returns "member not found". There is no `ErrMemberNotFound` handling (grep confirms), so the finalizer requeues every 5 s forever (`:220-223`). The CR stays Terminating, which blocks all further scaling (`:261-266`).
- (c) A learner present: promoted on the next reconcile by `tryPromoteLearner` if `MemberPromote` succeeds. Otherwise it logs "learner not yet promotable" and requeues 5 s.

Would the operator fight a twok-style reconfiguration? The scenario is: the survivor runs `--force-new-cluster` and becomes a cluster of one, then re-adds the peer as a learner and promotes it.
- It would not undo the force-new-cluster. It never compares membership and never calls `MemberAdd`/`MemberRemove` on its own initiative. During the outage it reports `ReadyMembers=1` and, for desired=2, `QuorumLost` (`updateStatus` `:1421-1436`: `ready > desired/2` is `1 > 1`, false). The PDB `minAvailable = 2/2+1 = 2` (`pdbMinAvailable` `:1499-1505`), so it blocks every voluntary eviction even when healthy.
- It would race twok on promotion. `tryPromoteLearner` promotes any learner, which is benign: promote is idempotent in effect, and a premature attempt fails with "not in sync".
- It cannot execute the re-join. The rejoining member's Pod has a frozen `--initial-cluster` and no hook to wipe the data dir. If the rejoiner is the former seed, its `--initial-cluster` lists only itself with `=existing`, so etcd refuses (count mismatch). The operator would recreate any deleted Pod with the same frozen command.
- It leaves stale `Status.MemberID` behind. That wedges later deletion and scale-down, as described in (b).
- It creates no new EtcdMember for the replacement (count is still 2 == desired), and a crash-looping member is never replaced at desired=2.

### A3. `spec.replicas: 2` and `progressDeadlineSeconds`

- Accepted. The only validation is `+kubebuilder:validation:Minimum=0`, `default=3` (`etcdcluster_types.go:450-453`). The "Should be odd." text (`:442`) is documentation only. No CEL rule touches parity (`:428-440`), and no admission webhook is registered: `main.go:172` only creates a webhook server, and there are no `kubebuilder:webhook` markers or `ValidateCreate` anywhere. The member CR's own `replicas` is locked to 1 (`etcdmember_types.go:146-154`).
- With 2 replicas: bootstrap creates a seed, then one learner is added and promoted. The PDB floor is 2. Self-heal is impossible (A2b).
- `progressDeadlineSeconds` (default 600, `:467-475`; const `etcdcluster_controller.go:47`): the deadline is stamped when a spec is snapshotted into `status.observed` (`:168-213`, `setProgressDeadline` `:2105-2112`). If `!reconciliationComplete` (member count != observed replicas, or any member not Ready, `:2075-2103`) when it expires, `handleDeadlineExceeded` (`:1959-2020`) runs:
  - before ClusterID: terminal `BootstrapFailed` ("delete the cluster and recreate");
  - after ClusterID with unchanged spec: `Available=False/DeadlineExceeded`, and the reconcile **returns early** (`:198-200`), so the scale, promote and auth steps no longer run. The cluster sits until the user edits spec, at which point the new spec is adopted.

  The deadline is cleared once reconciliation completes (`:1448-1457`). In steady state, `ProgressDeadline` is nil, so a later member going unready does not trigger it. It is purely a convergence timer for an in-flight target. It does not restart or replace members.

### A4. Leader and health detection; self-initiated disruptive actions

- No leader detection. There are no `Leader`, `MoveLeader` or `Defragment` calls (grep over `controllers/`, `internal/`). The etcd client interface is `MemberList/MemberAdd/MemberAddAsLearner/MemberPromote/MemberRemove/Status/Auth*` (`etcd_client.go:31-55`).
- Health is the kubelet readiness probe on `/health` :2381 (Pod Ready), plus member-ID discovery (`updateStatus` `etcdmember_controller.go:1024-1172`). `Status(endpoint)` is used only to record the running version (`observeVersion` `:1306-1333`, best-effort, informational `VersionDrifted`).
- Endpoints dialled: the per-member headless DNS client URLs of voters (`memberEndpoints` `helpers.go:354-370`), or the seed only during discovery.
- Actions the operator takes on its own that touch Pods:
  1. Deleting the Pod for `Dormant` members (`:108-134`, `ensurePodAbsent`).
  2. Self-deleting an EtcdMember whose memory-backed Pod was lost (`:149-162`).
  3. Self-deleting a crash-looping member (≥5 restarts, not OOM) when the others hold quorum (`:1093-1101`; never at replicas=2).
  4. Recreating a missing Pod with the original frozen command (`ensurePod`).

  There are no rolling restarts: version and resources changes are not re-templated onto existing members (`etcdmember_types.go:91-97`, `:118-123`; the only use of `Spec.Version` is image and restore env). It also enables auth once on convergence (`reconcileAuth` `:1119+`), which is not disruptive to Pods.

### A5. `bootstrap.restore` and `EtcdMember.spec.restore`

- `EtcdCluster.spec.bootstrap.restore.source` (S3 exact key or PVC exact subPath; `etcdcluster_types.go:312-335`) is immutable and consulted only at first bootstrap (`:438-440`). It is copied only onto the seed (`restoreForSeed` `helpers.go:264`; `etcdcluster_controller.go:441`). The member controller then adds the restore initContainer, which runs `etcdutl snapshot restore` with the member's name, initial-cluster, token and peer URL (`internal/agent/restore.go:29-37`). The agent is a no-op if `member/` already exists ("Inert once the data dir is initialized", `etcdmember_types.go:175-180`). Restore requires the `etcdutl` minor (3.6.x) to match `spec.version` (`docs/concepts.md:498`). It is rejected with Memory medium (`etcdcluster_types.go:440`).
- So it allows exactly one thing: seed a brand-new single-member cluster from a snapshot file. It always creates a new cluster ID and membership, then grows by learner-add. It cannot "create a member from an existing data dir":
  - the input is a snapshot, not a data dir;
  - it only applies to the seed;
  - it is immutable post-create.
- Closest existing mechanism for reusing a data dir: `etcd-migrate` adoption (`internal/migrate/adopt.go`, annotation `data-dir-subpath`, `helpers.go:64`). It adopts legacy PVCs and Pods in place. It does not join a live cluster.

### A6. Minimal operator changes for a twok-supervised 2-member cluster (from what the code shows)

1. **An entrypoint/supervisor hook.** The container `Command` is hard-coded to `etcd ...` (`etcdmember_controller.go:705-723`), with no args, env, initContainer or sidecar extension. Needed: either run `twok` as PID 1 (wrapping etcd and receiving the computed flags), or an API field, e.g. `spec.twok`/`spec.arbiter`, that switches `buildPod` to a supervisor image/command. The supervisor must be able to override `--initial-cluster`, `--initial-cluster-state`, `--force-new-cluster`, and to wipe the data dir on re-join. It also needs arbiter config (endpoint and credentials Secret via env/volume). The image is currently fixed to `<repo>:v<version>`, so the supervisor binary must either be in that image or come from a separate image field or initContainer that copies the binary.
2. **Tolerating external membership changes:**
   - (a) Refresh `EtcdMember.Status.MemberID`/`IsVoter` from MemberList by peer URL on every reconcile, instead of write-once (`etcdmember_controller.go:1106-1111`, `syncIsVoter` `:1067`).
   - (b) Treat `MemberRemove` → member-not-found as success in `removeMemberFromEtcd` (`:308`), and fall back to the name/peer-URL lookup, so deletion does not wedge.
   - (c) Do not promote learners the operator did not add when twok owns membership, or accept the race explicitly (`tryPromoteLearner` `:956-969`).
   - (d) Leave the self-heal and replace paths disabled for twok clusters. They are already inert at desired=2 because of `clusterHasQuorumWithout`, but a twok mode should say so explicitly rather than rely on arithmetic.
3. **Replicas=2 semantics.** The API already accepts 2. What is needed:
   - make Available/Degraded twok-aware: `QuorumLost` is reported for 1/2 ready even though twok serves writes (`etcdcluster_controller.go:1421-1436`);
   - decide the PDB: with `minAvailable=2` (`:1499-1505`) no voluntary eviction is ever allowed. Probably `minAvailable=1` under twok;
   - optionally add CEL to require `replicas==2` when twok mode is set.
4. **Readiness probe.** `/health` on :2381 (`:854-864`) returns non-200 without quorum. During the twok failover window that is fine, but readiness gates `allMembersReady` and the progress deadline. No change is strictly required if twok's single-member mode makes `/health` pass.
5. **Bootstrap.** The single-seed-then-learner flow (`:217-252`, `:746-852`) is compatible with twok's model as is. No change needed beyond (1).

---

## Topic B: Conditional writes (CAS) on S3-compatible stores

Research date: 2026-10-21. Web research only. Where a fact comes from a community report rather than vendor docs, it is marked **(community)**. "Not found" means I looked and found no authoritative statement.

### B. TL;DR

- AWS S3 offers a real single-object CAS. `If-None-Match: *` on PutObject shipped on 2024-08-20, and `If-Match: <etag>` on PutObject shipped on 2024-11-25. A failed condition returns `412 PreconditionFailed`. A request that loses a race returns `409 ConditionalRequestConflict`, and the client should re-read and retry. Conditional DeleteObject (`If-Match`) reached general purpose buckets on 2025-09-16.
- Among S3-compatible stores, support ranges from correct, to correct only in recent releases, to **silently ignoring the header**. Silent ignoring is the dangerous case: the write returns 200 and overwrites. Garage, the GCS XML API and (per community reports) DigitalOcean Spaces all do this.
- Ceph RGW (and Hetzner, which runs on Ceph) has accepted the headers since 2014. Fixes for races are still open upstream as of today: in **versioning-enabled buckets two `If-None-Match: *` writers can both get 200** (ceph/ceph#72118, open), and a loser can be told 200 for a write that never happened (ceph/ceph#72109, open).
- aws-sdk-go-v2 `service/s3`: `PutObjectInput.IfNoneMatch` arrived in v1.60.0, `DeleteObjectInput.IfMatch` in v1.68.0 and `PutObjectInput.IfMatch` in v1.69.0. The latest version today is v1.114.4. The SDK has no modeled error type for these failures, so match on `smithy.APIError.ErrorCode()` or on the HTTP status.
- minio-go v7 has `PutObjectOptions.SetMatchETag` / `SetMatchETagExcept`. They were added in v7.0.49, and `*` is handled correctly only from v7.0.72. The latest version today is v7.4.1. RemoveObject has no conditional option.
- I found no safe CAS substitute on a store that lacks conditional PUT. The arbiter should **refuse such a store** and run a startup self-test that fails closed.

---

### B. 1. AWS S3 semantics

#### 1.1 Timeline

| Date | Feature | Source |
| --- | --- | --- |
| 2024-08-20 | `If-None-Match` (create-only) on PutObject / CompleteMultipartUpload | https://aws.amazon.com/about-aws/whats-new/2024/08/amazon-s3-conditional-writes — "Amazon S3 adds support for conditional writes that can check for the existence of an object before creating it." |
| 2024-11-25 | `If-Match: <etag>` on PutObject / CompleteMultipartUpload | https://aws.amazon.com/about-aws/whats-new/2024/11/amazon-s3-functionality-conditional-writes — "Amazon S3 can now perform conditional writes that evaluate if an object is unmodified before updating it." and "S3 then evaluates if the object's ETag matches the value provided in the API request before committing the write" |
| 2024-11-25 | Bucket policy enforcement through `s3:if-match` / `s3:if-none-match` | https://aws.amazon.com/about-aws/whats-new/2024/11/amazon-s3-enforcement-conditional-write-operations-general-purpose-buckets |
| 2024-11-21 (SDK) | Conditional DeleteObject/DeleteObjects in the API model (directory buckets first) | aws-sdk-go-v2 s3 CHANGELOG v1.68.0: "Add support for conditional deletes for the S3 DeleteObject and DeleteObjects APIs." |
| 2025-09-16 | Conditional deletes in **general purpose** buckets | <https://aws.amazon.com/about-aws/whats-new/2025/09/amazon-s3-conditional-deletes-s3-general-purpose-buckets> — "only allow your delete request to succeed if the Etag provided matches that of the object" |
| 2025-10-28 (SDK) | Conditional CopyObject on the destination key | aws-sdk-go-v2 s3 CHANGELOG v1.89.0 |

#### 1.2 Exact semantics (User Guide: <https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html>)

- `If-None-Match: *`: "If there's no existing object with the same key name in the bucket, the write operation succeeds, resulting in a `200 OK` response. If there's an existing object, the write operation fails, resulting in a `412 Precondition Failed` response."
- Concurrency: "If multiple conditional writes or copies occur for the same object name, the first write operation to finish succeeds. Amazon S3 then fails subsequent writes with a `412 Precondition Failed` response."
- 409 with `If-None-Match`: "You can also receive a `409 Conflict` response in the case of concurrent requests if a delete request to an object succeeds before a conditional write operation on that object completes. When using conditional writes with `PutObject`, uploads may be retried after receiving a `409 Conflict` error."
- `If-Match`: "If there's an existing object with the same key name and matching ETag, the write operation succeeds, resulting in a `200 OK` response. If the ETag doesn't match, the write operation fails with a `412 Precondition Failed` response." and "You can also receive a `409 Conflict` response in the case of concurrent requests."
- `If-Match` on a missing key: "If there's no current object version with the same name, or if the current object version is a delete marker, the operation fails with a `404 Not Found` error." A concurrent delete also gives 404.
- Signing: "To use conditional writes, you must use AWS Signature Version 4 to sign the request."
- Permissions: `If-None-Match` needs `s3:PutObject`. `If-Match` "you must have the `s3:PutObject` and `s3:GetObject` permissions".
- In-progress multipart uploads are not considered. An MPU completed later with a condition fails with 412.

API reference (https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html), which gives the retry guidance word for word:
- If-Match: "If a conflicting operation occurs during the upload S3 returns a `409 ConditionalRequestConflict` response. On a 409 failure you should fetch the object's ETag and retry the upload."
- If-None-Match: "If a conflicting operation occurs during the upload S3 returns a `409 ConditionalRequestConflict` response. On a 409 failure you should retry the upload." and "Expects the '\*' (asterisk) character." and "This functionality is not supported for S3 on Outposts."

**Error codes for the arbiter:**
- HTTP 412 / `PreconditionFailed`: the CAS was lost cleanly.
- HTTP 409 / `ConditionalRequestConflict`: a concurrent operation interfered. Re-read the state and decide again. Never treat 409 as success.
- HTTP 404 on `If-Match`: the key is gone.

#### 1.3 Versioning

- "The HTTP `If-None-Match` header only applies to the current version of an object in a version bucket."
- "For buckets with versioning enabled, if there's no current object version with the same name, or if the current object version is a delete marker, the write operation succeeds."
- Conditional deletes: "Conditional delete evaluations only apply to the current version of the object." (https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-deletes.html)

So on AWS, `If-Match` is checked against the **current** version. Versioning does not break CAS on AWS, although a delete marker counts as "absent".

#### 1.4 Conditional DeleteObject (https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-deletes.html)

- "If there's an existing object with the same key name and matching `ETag`, the `DeleteObject` requests succeeds, and returns a `204 No content` response. If the `ETag` doesn't match, the delete operation fails with a `412 Precondition Failed` response."
- `If-Match: *` on delete checks that the object exists: "If the latest version of the object is a delete marker, the object doesn't exist and the `DeleteObject` API will fail and return a `412 Precondition Failed` response."
- 409: "You can also receive a `409 Conflict` error response in the case of concurrent requests if a `DELETE` or `PUT` request to an object succeeds before a conditional delete operation on that object completes."
- `x-amz-if-match-last-modified-time` and `x-amz-if-match-size` are "only supported for directory buckets" (aws-sdk-go-v2 `DeleteObjectInput` doc comments).

#### 1.5 ETag semantics (https://docs.aws.amazon.com/AmazonS3/latest/API/API_Object.html)

- "The ETag may or may not be an MD5 digest of the object data."
- "Objects created by the PUT Object, POST Object, or Copy operation ... and are encrypted by SSE-S3 or plaintext, have ETags that are an MD5 digest of their object data."
- "Objects created by the PUT Object ... and are encrypted by SSE-C or SSE-KMS, have ETags that are not an MD5 digest of their object data."
- "If an object is created by either the Multipart Upload or Part Copy operation, the ETag is not an MD5 digest, regardless of the method of encryption."
- "**Directory buckets** - MD5 is not supported by directory buckets."

**Implication:** the arbiter must treat the ETag as an opaque token taken from the PutObject response (or from Head/Get). It must never compute an MD5 locally and compare. Keep the CAS object small and always single-part.

#### 1.6 Bucket policy enforcement (https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes-enforce.html)

- "You can use the condition keys `s3:if-match` or `s3:if-none-match` as the optional `Condition` element ..."
- For multipart uploads "you must specify the `s3:ObjectCreationOperation` condition key to exempt the `CreateMultipartUpload`, `UploadPart`, and `UploadPartCopy` operations".
- The examples use `"Null": {"s3:if-none-match": "false"}` to allow, or a Deny with `"Null": {"s3:if-match": "true"}`, for example.
- Caveat: "If you use a bucket policy to enforce conditional writes, you can't perform copy operations to the bucket or prefix ... `CopyObject` requests made with those HTTP headers fail with a `501 Not Implemented` response."
- Deletes can be enforced the same way with `s3:if-match` (https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-delete-enforce.html). Requests without the header get 403.
- This is optional defense in depth for an AWS-backed arbiter: Deny `s3:PutObject` on the arbiter prefix when both `s3:if-match` and `s3:if-none-match` are Null. That makes an unconditional overwrite by a buggy client impossible.

---

### B. 2. Support matrix of S3-compatible stores

Legend:
- ✅ documented or verified as enforced.
- ⚠️ accepted, but with known correctness gaps.
- ❌ not supported. "ignored" means 200 plus an overwrite. "501" means an error is returned.
- ? not found.

| Provider | `If-None-Match: *` on PUT | `If-Match: <etag>` on PUT | Notes + source |
| --- | --- | --- | --- |
| **AWS S3** | ✅ since 2024-08-20 | ✅ since 2024-11-25 | Conditional DELETE since 2025-09-16 (GP buckets). See §1. |
| **MinIO** (community, AGPL) | ✅ since RELEASE.2023-02-09T05-16-53Z; `*` handled AWS-style since RELEASE.2024-05-07T06-41-25Z | ✅ since RELEASE.2023-02-09T05-16-53Z | See the MinIO notes below the table. |
| **Ceph RGW** | ⚠️ accepted for a long time; correct on unversioned buckets apart from race-reporting bugs; **not atomic on versioning-enabled buckets (open)** | ⚠️ same | See the Ceph notes below the table. |
| **Hetzner Object Storage** (Ceph-based) | ? undocumented for unversioned buckets; **not supported on versioned buckets** | same | The docs say "Hetzner Object Storage technology is based on the Ceph open-source distributed storage system" (https://docs.hetzner.com/storage/general/which-storage-is-right-for-me/). The "Not supported" list on <https://docs.hetzner.com/storage/object-storage/supported-actions/> (last changed 2024-09-23) contains "Conditional PUT/DELETE operations on versioned Buckets". The RGW version is not disclosed, so the Ceph caveats probably apply. Self-test required. |
| **SeaweedFS S3** | ⚠️ / ✅ correct only in recent versions: headers since PR #7154 (merged 2025-08-22); single-filer atomic since **4.18** (2026-04-02); multi-filer atomic since **4.46** (2026-09-08) | same | See the SeaweedFS notes below the table. |
| **Garage** | ❌ **ignored (silent overwrite)** | ❌ ignored | See the Garage notes below the table. |
| **Cloudflare R2** | ✅ atomic | ✅ | https://developers.cloudflare.com/r2/platform/s3-compatibility/api/: conditional headers on PutObject "are evaluated atomically against the committed bucket state, as part of the same write that commits the object" and "at most one write succeeds and the others fail with `412 PreconditionFailed`". DeleteObject has **no** conditional headers listed. **(community)** "R2 silently drops `If-Match` on `DeleteObject`" (https://github.com/stowage-js/stowage/issues/436, measured 2026-10-11). Introduction date not found. |
| **Google Cloud Storage, XML API** | ❌ **ignored**. Use `x-goog-if-generation-match: 0` instead | ❌ **ignored**. Use `x-goog-if-generation-match: <generation>` | See the GCS notes below the table. |
| **Backblaze B2 (S3 API)** | ❌ rejected with an error (501 reported) | ? not documented; assume ❌ | Proxmox patch (2025-07): "Backblaze B2 does not allow to set the `If-None-Match` http header for put requests, failing with an error" (https://lists.proxmox.com/pipermail/pbs-devel/2025-July/014234.html). **(community)** "Backblaze B2 answers 501 to `If-None-Match: *`" (https://github.com/harochell-tech/ERP/pull/112, observed 2026-09-29/30). No official Backblaze statement found. |
| **Scaleway** | ✅ documented | ✅ documented | https://www.scaleway.com/en/docs/object-storage/api-cli/using-conditional-writes/ (posted/validated 2026-07-03): "The `If-None-Match` header prevents overwrites of existing data" and "The `If-Match` header checks an object's entity tag (ETag) before writing." Applies to put-object, copy-object and complete-multipart-upload. Concurrency/atomicity is not described. Introduction date not found. |
| **Wasabi** | ✅ **(community)** single sequential test only | ? not found | No Wasabi documentation found. **(community)** "Wasabi rejects an If-None-Match: * overwrite with 412 PreconditionFailed and leaves the object unchanged" (https://github.com/haileyok/engram-garden/pull/12, s3.us-west-2.wasabisys.com, 2026-10-08). Racing writers were not tested. |
| **DigitalOcean Spaces** | ❌ / inconsistent **(community)** | ⚠️ **(community)** a quoted ETag is refused and a bare ETag is accepted | Not mentioned in https://docs.digitalocean.com/products/spaces/reference/s3-compatibility/. **(community)** "Verified 2026-09-30: DigitalOcean Spaces accepts a conditional `PUT` with `If-None-Match: *` over an existing object (HTTP 200, not 412)" (https://github.com/Goldberry-Playground/odoocker-goldberrygrove/issues/798). **(community)** "DigitalOcean Spaces refuses a quoted ETag in If-Match but accepts it bare, and answers 412 (not 404)" (https://github.com/RichardMcQuiston01/storage-adapters/pull/35, 2026-10-15). Treat as unsupported. |

**MinIO.** Headers were added in minio/minio#16551 ("support if-match/if-none-match with s3 uploads", merged 2023-02-07) and AWS-style `*` handling in #19682 ("support ETag value to be '*'", merged 2024-05-07). Two fixes matter for the CAS:
- #21550/#21567 (in RELEASE.2025-09-07T16-13-09Z) make If-Match on a missing object fail (issue #21526: "PutObject Conditional Write is ignored if the object does not exist").
- #21653 (merged 2025-10-24): "conditional PUT operations (`if-match`/`if-none-match`) when read quorum could not be reached ... allowing operations to proceed even when conditions couldn't be verified". This fix is **not in any community release**: the last release, RELEASE.2025-10-15T17-29-55Z, does not contain it.

The repo is now archived: "THIS REPOSITORY IS NO LONGER MAINTAINED." (https://github.com/minio/minio). Accept the last release only with caution, and only on a healthy cluster.

**Ceph RGW.** Conditional PUT on ETag dates from 2014 (ceph/ceph#3160). Tracker #68183 ("conditional write doesn't work in certain scenarios": versioned bucket and MPU returned success) was fixed in main by #63348 (merged 2025-07-28; in v21.x). It was backported to tentacle in **v20.2.1** and to squid in **v19.2.7**. Tracker #80325 (versioning-*suspended* buckets) was fixed by #71584 and backported 2026-10 (#72049/#72050). Two fixes are **still open on main**:
- #72118: "In a versioning-enabled bucket a conditional write is not atomic ... two PutObjects with If-None-Match: * on a new key both succeed ... Applications that use If-None-Match: * for a lock or to create exactly once get two winners."
- #72109: "With `If-Match: *`, `-ECANCELED` becomes success, though the write never happened ... `If-None-Match: *` answers `-ENOENT` the same way. With If-Match on an ETag, `-ECANCELED` ... is answered 500 UnknownError."

Also open, #72117: the ETag is compared as a *prefix* of the header value.

**SeaweedFS.** Issue #8800 (v4.16): three concurrent `If-None-Match: *` PUTs all returned 200. It was fixed by #8802 "s3api: make conditional mutations atomic and AWS-compatible" (merged 2026-03-28, first release 4.18). #11109 "filer: route exclusive and conditional creates to the entry's ring owner" (merged 2026-09-03, first release 4.46) fixed races across several filers: "with several filers two callers both pass the existence check and both report success". Fixes for suspended versioning (#11780, #11806) and 404-vs-412 (#11882) were merged in 2026-10 and are **not yet released**; latest release is 4.48 (2026-09-28). Require ≥ 4.46 and an unversioned bucket.

**Garage.** Issue #1326 (closed as a duplicate of #1052): "Uploading one object with a precondition If-None-Match='*' overwrites any existing object with the same key" (https://git.deuxfleurs.fr/Deuxfleurs/garage/issues/1326). Issue #1052 is still open. A maintainer (lx) wrote on 2025-05-29: "adding this to Garage is not possible with our weak-consistency replication model, it would require a full rearchitecturing to use a strong consensus algorithm" (https://git.deuxfleurs.fr/Deuxfleurs/garage/issues/1052). This is the canonical "header ignored" case.

**Google Cloud Storage, XML API.** Docs (https://docs.cloud.google.com/storage/docs/request-preconditions): `If-Match` / `If-None-Match` are "Applicable for requests that retrieve data". Write preconditions are `x-goog-if-generation-match` / `x-goog-if-metageneration-match`. "When a generation-match precondition with a value of `0` is included in a request, the request only proceeds if no object with the specified name exists in the bucket" and otherwise "fails with a status code of `412 Precondition Failed`". "Preconditions cannot be used in XML API multipart uploads." **(community)** ClickHouse #115266 found that a second PUT with `If-None-Match: *` returned "**200**, and the object is overwritten", while `x-goog-if-generation-match: 0` returned 412 (https://github.com/ClickHouse/ClickHouse/issues/115266). stowage-js #436 (2026-10-11): "The XML API ignores `If-Match` and `If-None-Match` on every write and delete". **A GCS backend needs a dedicated code path using generations** (the generation is returned in `x-goog-generation`), not the S3 headers.

---

### B. 3. Go client libraries

#### 3.1 aws-sdk-go-v2 (`github.com/aws/aws-sdk-go-v2/service/s3`)

- Current version: **v1.114.4** (published 2026-10-21T18:18:48Z, per proxy.golang.org `@latest`). The core module `github.com/aws/aws-sdk-go-v2` is at v1.47.3, and `github.com/aws/smithy-go` at v1.28.6.
- Fields, verified in source at `service/s3/v1.114.4`:
  - `PutObjectInput.IfMatch *string` and `PutObjectInput.IfNoneMatch *string`.
  - `DeleteObjectInput.IfMatch *string`, `IfMatchLastModifiedTime *time.Time` and `IfMatchSize *int64` (the last two are directory buckets only).
  - `CompleteMultipartUploadInput` also has `IfMatch` / `IfNoneMatch`, which the CHANGELOG lists under v1.60.0/v1.69.0.
- Versions that introduced them, found by checking `api_op_*.go` at each tag and confirmed in `service/s3/CHANGELOG.md`:
  - **v1.60.0** (2024-08-20): `PutObjectInput.IfNoneMatch`. "Add support for conditional writes for PutObject and CompleteMultipartUpload APIs."
  - **v1.68.0** (2024-11-21): `DeleteObjectInput.IfMatch` etc. "Add support for conditional deletes for the S3 DeleteObject and DeleteObjects APIs."
  - **v1.69.0** (2024-11-25): `PutObjectInput.IfMatch`. "Add support for ETag based conditional writes in PutObject and CompleteMultiPartUpload APIs"
  - v1.89.0 (2025-10-28): conditional CopyObject destination.
- There are **no modeled error types** for these failures: `service/s3/types/errors.go` has no PreconditionFailed or ConditionalRequestConflict type. Detect them generically:

```go
import (
    "errors"
    awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
    "github.com/aws/smithy-go"
)

func classify(err error) string {
    var ae smithy.APIError
    if errors.As(err, &ae) {
        switch ae.ErrorCode() {
        case "PreconditionFailed":
            return "lost" // 412
        case "ConditionalRequestConflict":
            return "conflict" // 409: re-read, retry
        case "NoSuchKey", "NotFound":
            return "absent" // 404 on If-Match
        case "NotImplemented":
            return "unsupported" // 501
        }
    }
    var re *awshttp.ResponseError
    if errors.As(err, &re) {
        switch re.HTTPStatusCode() {
        case 412:
            return "lost"
        case 409:
            return "conflict"
        case 404:
            return "absent"
        case 501:
            return "unsupported"
        }
    }
    return "unknown" // includes 5xx/timeouts: outcome indeterminate
}
```

Check the status code as the primary signal. Error-code strings vary between compatible stores (for example, DigitalOcean answers 412 where AWS answers 404).

**Retry caveat:** the default retryer (`aws/retry`) retries HTTP 500/502/503/504, `RequestTimeout*`, throttling codes and transport errors. It does **not** retry 409/412 (`DefaultRetryableHTTPStatusCodes = {500,502,503,504}`; `DefaultRetryableErrorCodes = {"RequestTimeout","RequestTimeoutException"}`; source https://github.com/aws/aws-sdk-go-v2/blob/v1.47.3/aws/retry/retryable_error.go, in standard.go). This creates an ambiguity:
- The first attempt of a conditional PUT may commit while its response is lost.
- The automatic retry then gets **412 for its own successful write**.

The arbiter must therefore embed a unique writer nonce in the object body. On 412, 5xx or timeout it re-reads the object (GET) and decides by content, not by status alone. The simplest approach is `retry.AddWithMaxAttempts(..., 1)` for CAS calls and doing the retry and read-back logic explicitly.

#### 3.2 minio-go v7 (`github.com/minio/minio-go/v7`)

- Current version: **v7.4.1** (2026-10-12, per proxy.golang.org).
- `PutObjectOptions` has two methods. Source: https://github.com/minio/minio-go/blob/v7.4.1/api-put-object.go lines 124–150.
  - `(*PutObjectOptions).SetMatchETag(etag string)` sets `If-Match`. It sends `*` unquoted, and otherwise `"\""+etag+"\""`.
  - `(*PutObjectOptions).SetMatchETagExcept(etag string)` sets `If-None-Match` in the same way.
  - The comment calls them "a MinIO specific extension to support optimistic locking semantics". They work against AWS too, because they set the standard headers through `customHeaders` and `Header()`.
- Introduced in **v7.0.49** (2023-02-18); v7.0.40–v7.0.48 lack them. Until **v7.0.71**, `SetMatchETagExcept("*")` sent `If-None-Match: "*"` (quoted). Plain `*` handling arrived in **v7.0.72** (2024-06-18). Require ≥ v7.0.72.
- `RemoveObjectOptions` has **no** If-Match option in v7.4.1; only `ForceDelete` etc. exist. A conditional delete would need the AWS SDK or a raw request.
- Error detection: `minio.ToErrorResponse(err)` returns a struct with `.StatusCode` (412/409) and `.Code` ("PreconditionFailed" / "ConditionalRequestConflict").
- minio-go always quotes a non-`*` ETag. That is RFC-correct, but DigitalOcean Spaces reportedly refuses quoted ETags (see the matrix).
- PutObject with a known small size is single-part. A conditional header on an automatically chosen multipart path depends on the server applying it at CompleteMultipartUpload. Keep the CAS object small and pass an exact size.

**Recommendation:** for a backend that must also handle GCS generations and precise error classification, aws-sdk-go-v2 (≥ v1.69.0) is the better fit. minio-go is acceptable for the PUT path only.

---

### B. 4. Without conditional PUT: is there any safe CAS?

**Object Lock / retention.** No CAS here. Object Lock requires versioning and protects *versions* from deletion or overwrite. A new PUT to a locked key simply creates a new current version, so it gives no mutual exclusion on the key. Legal hold works the same way.

**Versioning + ListObjectVersions ("first version wins").** Each contender PUTs, then lists versions and treats the oldest as the winner. This is unsafe in general:
- It needs strongly consistent listing and a total, stable version order across racing writers. AWS has had strong consistency since 2020, but many compatible stores (Garage's CRDT model, multi-site Ceph, eventual-consistency gateways) do not provide this.
- Ties on LastModified, clock skew and version-ID ordering are implementation-defined.
- Even with perfect listing, it gives "create once" at best, not compare-and-swap on a value. Every later update is a new race that needs a full lock protocol on top.
- Ceph's own conditional writes on versioned buckets were found to be non-atomic (ceph/ceph#72118). That suggests the version index is a poor primitive to build on.

**Unique-key + list mutual exclusion (bakery-style).** Each writer creates its own `lock/<uuid>` key, lists the prefix and checks whether it is the only or lowest one. This is correct only with linearizable LIST-after-PUT and reliable lease expiry based on wall-clock time. The industry precedent is Delta Lake / delta-rs, which required an external DynamoDB lock for S3 commits until conditional writes existed, exactly because S3 alone could not provide this. Not recommended.

**Multipart tricks.** No CAS here either:
- CompleteMultipartUpload is unconditional unless it carries `If-None-Match`/`If-Match`, and a server that ignores those headers on PUT generally ignores them on Complete as well. On GCS XML, "Preconditions cannot be used in XML API multipart uploads."
- `x-amz-copy-source-if-match` conditions the *source* of a copy, not the destination. It does not prevent overwriting the destination.

**Read-check-write with HEAD + PUT.** This is a TOCTOU race by construction. It is acceptable only for a single writer, and an arbiter is by definition not a single-writer setting.

**Conclusion:** with no conditional PUT (or a conditional PUT that the server merely parses but does not enforce atomically) there is no safe single-object CAS using only S3 primitives. **The arbiter should refuse to start on such a store.** Supporting it would require an external linearizable store (etcd, a DynamoDB-like database, a Kubernetes Lease), which defeats the purpose of an object-store arbiter.

#### 4.1 Recommended startup self-test (fail closed)

Run it against a random probe key under the arbiter's prefix, for example `.<arbiter>/probe/<uuid>`. Disable SDK retries for these calls.

1. `PUT probe If-None-Match: *` (body = nonce A). Expect **2xx**. 501, 400 or NotImplemented means "conditional writes unsupported", so refuse (this catches B2).
2. `PUT probe If-None-Match: *` (body = nonce B). Expect **412** (accept 409 only if a retry then yields 412). **200 means the header is ignored, so refuse** (this catches Garage, GCS XML and DO Spaces). Then GET the probe and assert that the body is still nonce A.
3. `PUT probe If-Match: "<bogus etag>"`. Expect **412**. 200 means refuse.
4. `PUT probe If-Match: <etag from step 1 response>` (body = nonce C). Expect **2xx**, and a new ETag ≠ the old one. A 412 here means the store mis-compares quoted and unquoted ETags (the DO quirk): refuse or apply a quirk deliberately.
5. `PUT probe If-Match: <old etag from step 1>`. Expect **412** (this proves the stale-ETag check works).
6. `PUT probe-missing If-Match: *` on a non-existent key. Expect **404** (AWS) or 412. **2xx means refuse** (this catches the old MinIO #21526 behaviour and Ceph's suspended-versioning bug).
7. Race smoke test: N ≈ 8–16 parallel `PUT fresh-key If-None-Match: *`, each with a distinct nonce. Expect **exactly one** 2xx, then GET and assert that the body equals the winner's nonce. More than one 2xx means refuse (this catches SeaweedFS < 4.18 and Ceph on versioned buckets). Passing does not prove atomicity, but it catches gross failures cheaply.
8. `GetBucketVersioning`. If the status is `Enabled` or `Suspended`, warn or refuse unless an explicit override is set. Ceph #72118 is unfixed, and Hetzner documents conditional PUT on versioned buckets as unsupported. On AWS, versioning is safe (§1.3), so this could be gated on a provider allow-list. A versioning check alone is not a substitute for step 7.
9. Clean up the probe keys. Conditional DELETE should be probed separately (`If-Match: <bogus>` → expect 412) only if the arbiter relies on it. R2 reportedly ignores it, so prefer a design that never needs conditional DELETE, for example a tombstone value written with If-Match.

Rules that apply on every CAS, not just at startup:
- Embed `{writer-id, nonce, term}` in the body.
- After any 2xx, any 412/409 following a retried or timed-out attempt, or any 5xx, re-GET and decide by body content. This defends against SDK auto-retry ambiguity and against Ceph #72109, which answers 200 for a write that never happened.
- Treat 5xx and timeouts as "unknown", never as success.
- Use only ETags returned by the server, never computed MD5s (SSE-KMS / SSE-C make them non-MD5).

---

### B. Sources (primary)

- AWS: <https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html> · <https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes-enforce.html> · <https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-deletes.html> · <https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html> · <https://docs.aws.amazon.com/AmazonS3/latest/API/API_Object.html> · <https://aws.amazon.com/about-aws/whats-new/2024/08/amazon-s3-conditional-writes> · <https://aws.amazon.com/about-aws/whats-new/2024/11/amazon-s3-functionality-conditional-writes> · <https://aws.amazon.com/about-aws/whats-new/2024/11/amazon-s3-enforcement-conditional-write-operations-general-purpose-buckets> · <https://aws.amazon.com/about-aws/whats-new/2025/09/amazon-s3-conditional-deletes-s3-general-purpose-buckets>
- MinIO: github.com/minio/minio PRs #16551, #19448, #19682, #21550, #21567, #21653; issue #21526; repo README (archived).
- Ceph: github.com/ceph/ceph PRs #3160, #63348, #65932, #65949, #71584, #72049, #72050, #72109, #72117, #72118; <https://tracker.ceph.com/issues/68183> · <https://tracker.ceph.com/issues/80325>
- SeaweedFS: github.com/seaweedfs/seaweedfs issue #8800; PRs #7154, #8802, #11109, #11780, #11806, #11882; releases 4.18/4.46/4.48.
- Garage: <https://git.deuxfleurs.fr/Deuxfleurs/garage/issues/1052> · <https://git.deuxfleurs.fr/Deuxfleurs/garage/issues/1326> · <https://garagehq.deuxfleurs.fr/documentation/reference-manual/s3-compatibility/>
- R2: <https://developers.cloudflare.com/r2/platform/s3-compatibility/api/>
- GCS: <https://docs.cloud.google.com/storage/docs/request-preconditions> · <https://github.com/ClickHouse/ClickHouse/issues/115266> · <https://github.com/stowage-js/stowage/issues/436>
- B2: <https://lists.proxmox.com/pipermail/pbs-devel/2025-July/014234.html> · <https://github.com/harochell-tech/ERP/pull/112>
- Hetzner: <https://docs.hetzner.com/storage/object-storage/supported-actions/> · <https://docs.hetzner.com/storage/general/which-storage-is-right-for-me/>
- Scaleway: <https://www.scaleway.com/en/docs/object-storage/api-cli/using-conditional-writes/>
- Wasabi: <https://github.com/haileyok/engram-garden/pull/12>
- DigitalOcean: <https://docs.digitalocean.com/products/spaces/reference/s3-compatibility/> · <https://github.com/Goldberry-Playground/odoocker-goldberrygrove/issues/798> · <https://github.com/RichardMcQuiston01/storage-adapters/pull/35>
- Go: proxy.golang.org `@latest` for service/s3, minio-go/v7, aws-sdk-go-v2 and smithy-go; https://github.com/aws/aws-sdk-go-v2/blob/service/s3/v1.114.4/service/s3/CHANGELOG.md · https://github.com/minio/minio-go/blob/v7.4.1/api-put-object.go
