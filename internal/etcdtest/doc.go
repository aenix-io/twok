// Package etcdtest starts real etcd processes for the integration tests of
// twok. The etcd binary is taken from TEST_ETCD_BINARY; tests that need it are
// skipped when it is not set.
//
// Every member gets a temporary data directory, a client and a peer port on
// 127.0.0.1, client and peer mTLS with certificates issued by a per-test CA,
// and its own log file with etcd's stdout and stderr.
//
// # Partitions
//
// A member listens for peers on its real peer port, but advertises the
// address of a [Proxy] in front of that port. Membership stores the
// advertised URL, so every other member dials this member through its proxy,
// and a test cuts the peer traffic towards a member by cutting that member's
// proxy.
//
// The model is per dialer, not per message. In etcd's rafthttp, member A
// receives B's stream messages over a connection that A opened to B, and
// sends its pipeline and snapshot messages over connections that A opens to
// B as well. Cutting B's proxy therefore stops everything that travels over
// connections A opens to B: B's stream messages to A and A's pipeline posts
// to B. A's stream messages to B still travel over the connection B opened
// to A, through A's proxy. To partition two members completely, cut both
// proxies. A single cut is an asymmetric fault that does not map exactly to
// "A cannot send to B"; it is useful to test one-way faults, knowingly.
//
// Client traffic is not proxied: tests reach every member on its real client
// port at all times, which is what twok sees on a real node, where the
// supervisor runs next to its etcd.
package etcdtest
