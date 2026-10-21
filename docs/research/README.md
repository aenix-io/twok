# Research notes

Source material the design is based on, as collected during the project, with citations into the code and documents that were read.
They are kept as written, so the design can be checked against them, and they are not maintained after the fact.

- [red-hat-two-node-fencing.md](red-hat-two-node-fencing.md): how two-node OpenShift with fencing handles etcd, from the enhancement, the Pacemaker resource agent and the cluster-etcd-operator code.
- [etcd-internals.md](etcd-internals.md): what `--force-new-cluster`, removed members, learners and the status API do, verified in etcd 3.5 and 3.6.
- [etcd-operator-and-s3.md](etcd-operator-and-s3.md): what cozystack/etcd-operator v0.5.4 does with membership, and which S3-compatible stores honour conditional writes.
