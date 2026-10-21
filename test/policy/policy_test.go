//go:build policy

// Package policy tests deploy/twok-agent.yaml against a real API server.
// It needs a cluster with the policy applied, named by POLICY_KUBE_CONTEXT.
// Pods are created with --dry-run=server, so nothing is left behind.
package policy

import (
	"bytes"
	"encoding/json"
	"maps"
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"
)

const arbiterURL = "https://arbiter.example.com/twok/etcd"

type container struct {
	Name          string   `json:"name"`
	Image         string   `json:"image"`
	RestartPolicy string   `json:"restartPolicy"`
	Command       []string `json:"command"`
	Args          []string `json:"args"`
	VolumeMounts  []struct {
		Name      string `json:"name"`
		MountPath string `json:"mountPath"`
		ReadOnly  bool   `json:"readOnly"`
	} `json:"volumeMounts"`
	SecurityContext struct {
		RunAsNonRoot           *bool `json:"runAsNonRoot"`
		ReadOnlyRootFilesystem *bool `json:"readOnlyRootFilesystem"`
	} `json:"securityContext"`
}

type pod struct {
	Status struct {
		QOSClass string `json:"qosClass"`
	} `json:"status"`
	Spec struct {
		ShareProcessNamespace *bool       `json:"shareProcessNamespace"`
		InitContainers        []container `json:"initContainers"`
		Volumes               []struct {
			Name      string `json:"name"`
			ConfigMap *struct {
				Name     string `json:"name"`
				Optional bool   `json:"optional"`
			} `json:"configMap"`
		} `json:"volumes"`
	} `json:"spec"`
}

// memberPod is an etcd-operator member Pod as the operator builds it: the
// cluster label, the container named etcd with its flags on the command
// line, the data volume and, with TLS, the client and peer certificates.
func memberPod(labels map[string]string, tls, restore bool) map[string]any {
	volumes := []any{map[string]any{"name": "data", "emptyDir": map[string]any{}}}
	command := []any{"etcd", "--name=etcd-abcde", "--data-dir=/var/lib/etcd"}
	if tls {
		volumes = append(volumes,
			map[string]any{"name": "tls-client", "secret": map[string]any{"secretName": "etcd-client-tls"}},
			map[string]any{"name": "tls-peer", "secret": map[string]any{"secretName": "etcd-peer-tls"}},
		)
		command = append(command,
			"--listen-client-urls=https://0.0.0.0:2379",
			"--cert-file=/etc/etcd/tls/client/tls.crt",
			"--listen-peer-urls=https://0.0.0.0:2380",
			"--peer-client-cert-auth=true",
		)
	} else {
		command = append(command, "--listen-client-urls=http://0.0.0.0:2379", "--listen-peer-urls=http://0.0.0.0:2380")
	}
	spec := map[string]any{
		"subdomain":                    "etcd-headless",
		"automountServiceAccountToken": false,
		"securityContext":              map[string]any{"runAsUser": 65532, "runAsGroup": 65532, "fsGroup": 65532},
		"containers": []any{map[string]any{
			"name":         "etcd",
			"image":        "registry.k8s.io/etcd:3.6.4-0",
			"command":      command,
			"volumeMounts": []any{map[string]any{"name": "data", "mountPath": "/var/lib/etcd"}},
		}},
		"volumes": volumes,
	}
	if restore {
		spec["initContainers"] = []any{map[string]any{"name": "restore", "image": "registry.k8s.io/etcd:3.6.4-0"}}
	}
	all := map[string]string{
		"app.kubernetes.io/name":             "etcd",
		"app.kubernetes.io/instance":         "etcd",
		"app.kubernetes.io/managed-by":       "etcd-operator",
		"etcd-operator.cozystack.io/cluster": "etcd",
		"twok.aenix.io/inject":               "true",
	}
	for k, v := range labels {
		if v == "" {
			delete(all, k)
		} else {
			all[k] = v
		}
	}
	return map[string]any{
		"apiVersion": "v1",
		"kind":       "Pod",
		"metadata": map[string]any{
			"name":        "etcd-abcde",
			"namespace":   "default",
			"labels":      all,
			"annotations": map[string]any{"twok.aenix.io/arbiter": arbiterURL},
		},
		"spec": spec,
	}
}

func etcdContainer(manifest map[string]any) map[string]any {
	return manifest["spec"].(map[string]any)["containers"].([]any)[0].(map[string]any)
}

func admit(t *testing.T, manifest map[string]any) pod {
	t.Helper()
	kubeContext := os.Getenv("POLICY_KUBE_CONTEXT")
	if kubeContext == "" {
		t.Skip("POLICY_KUBE_CONTEXT not set")
	}
	in, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("kubectl", "--context", kubeContext, "create", "--dry-run=server", "--output=json", "--filename=-")
	cmd.Stdin = bytes.NewReader(in)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("kubectl create: %v: %s", err, stderr.String())
	}
	var p pod
	if err := json.Unmarshal(out, &p); err != nil {
		t.Fatal(err)
	}
	return p
}

func agentOf(p pod) (container, bool) {
	for _, c := range p.Spec.InitContainers {
		if c.Name == "twok" {
			return c, true
		}
	}
	return container{}, false
}

func mustAgent(t *testing.T, p pod) container {
	t.Helper()
	agent, ok := agentOf(p)
	if !ok {
		t.Fatalf("no twok sidecar in %v", names(p))
	}
	return agent
}

func names(p pod) []string {
	var n []string
	for _, c := range p.Spec.InitContainers {
		n = append(n, c.Name)
	}
	return n
}

func mounts(c container) map[string]string {
	m := map[string]string{}
	for _, vm := range c.VolumeMounts {
		m[vm.Name] = vm.MountPath
	}
	return m
}

func sharesProcesses(p pod) bool {
	return p.Spec.ShareProcessNamespace != nil && *p.Spec.ShareProcessNamespace
}

func TestInjectsIntoTLSMember(t *testing.T) {
	p := admit(t, memberPod(nil, true, true))
	if got := names(p); !slices.Equal(got, []string{"restore", "twok"}) {
		t.Fatalf("init containers = %v, want the sidecar after restore", got)
	}
	agent := mustAgent(t, p)
	if agent.RestartPolicy != "Always" {
		t.Errorf("restartPolicy = %q, want a native sidecar", agent.RestartPolicy)
	}
	if !slices.Equal(agent.Command, []string{"twok"}) {
		t.Errorf("command = %v, want [twok]", agent.Command)
	}
	want := []string{
		"agent",
		"--arbiter=" + arbiterURL,
		"--endpoint=https://127.0.0.1:2379",
		"--data-dir=/var/lib/etcd",
		"--config-dir=/etc/twok",
		"--metrics-listen=:2390",
		"--peer-endpoints=https://etcd-headless.default.svc:2379",
		"--cert-file=/etc/etcd/tls/client/tls.crt",
		"--key-file=/etc/etcd/tls/client/tls.key",
		"--trusted-ca-file=/etc/etcd/tls/client/ca.crt",
	}
	if !slices.Equal(agent.Args, want) {
		t.Errorf("args = %v, want %v", agent.Args, want)
	}
	wantMounts := map[string]string{
		"data":        "/var/lib/etcd",
		"twok-config": "/etc/twok",
		"tls-client":  "/etc/etcd/tls/client",
		"tls-peer":    "/etc/etcd/tls/peer",
	}
	if got := mounts(agent); !maps.Equal(got, wantMounts) {
		t.Errorf("mounts = %v, want %v", got, wantMounts)
	}
	for _, vm := range agent.VolumeMounts {
		if vm.ReadOnly == (vm.Name == "data") {
			t.Errorf("mount %s: readOnly = %t, want only the data volume writable", vm.Name, vm.ReadOnly)
		}
	}
	if !sharesProcesses(p) {
		t.Error("shareProcessNamespace is not set: the sidecar cannot find etcd")
	}
	sc := agent.SecurityContext
	if sc.RunAsNonRoot == nil || !*sc.RunAsNonRoot || sc.ReadOnlyRootFilesystem == nil || !*sc.ReadOnlyRootFilesystem {
		t.Errorf("securityContext = %+v, want non-root with a read-only root filesystem", sc)
	}
	last := p.Spec.Volumes[len(p.Spec.Volumes)-1]
	if last.Name != "twok-config" || last.ConfigMap == nil || last.ConfigMap.Name != "etcd-twok" || !last.ConfigMap.Optional {
		t.Errorf("config volume = %+v, want the optional ConfigMap etcd-twok", last)
	}
}

func TestInjectsIntoPlaintextMember(t *testing.T) {
	p := admit(t, memberPod(nil, false, false))
	agent := mustAgent(t, p)
	want := []string{
		"agent",
		"--arbiter=" + arbiterURL,
		"--endpoint=http://127.0.0.1:2379",
		"--data-dir=/var/lib/etcd",
		"--config-dir=/etc/twok",
		"--metrics-listen=:2390",
		"--peer-endpoints=http://etcd-headless.default.svc:2379",
	}
	if !slices.Equal(agent.Args, want) {
		t.Errorf("args = %v, want %v", agent.Args, want)
	}
	wantMounts := map[string]string{"data": "/var/lib/etcd", "twok-config": "/etc/twok"}
	if got := mounts(agent); !maps.Equal(got, wantMounts) {
		t.Errorf("mounts = %v, want %v", got, wantMounts)
	}
	if !sharesProcesses(p) {
		t.Error("shareProcessNamespace is not set")
	}
}

// The arbiter URL comes from the Pod annotation. Without the annotation the
// sidecar gets no --arbiter flag at all.
func TestArbiterFromAnnotation(t *testing.T) {
	other := "s3://bucket/clusters/etcd"
	manifest := memberPod(nil, false, false)
	manifest["metadata"].(map[string]any)["annotations"] = map[string]any{"twok.aenix.io/arbiter": other}
	agent := mustAgent(t, admit(t, manifest))
	if !slices.Contains(agent.Args, "--arbiter="+other) {
		t.Errorf("args = %v, want --arbiter=%s", agent.Args, other)
	}

	manifest = memberPod(nil, false, false)
	delete(manifest["metadata"].(map[string]any), "annotations")
	agent = mustAgent(t, admit(t, manifest))
	for _, arg := range agent.Args {
		if strings.HasPrefix(arg, "--arbiter=") {
			t.Errorf("args = %v, want no --arbiter without the annotation", agent.Args)
		}
	}
}

// The peer endpoints come from the Pod's subdomain, the headless Service
// that resolves to every member. Without a subdomain there is no flag.
func TestPeerEndpointsFromSubdomain(t *testing.T) {
	manifest := memberPod(nil, false, false)
	manifest["spec"].(map[string]any)["subdomain"] = "etcd-members"
	agent := mustAgent(t, admit(t, manifest))
	if !slices.Contains(agent.Args, "--peer-endpoints=http://etcd-members.default.svc:2379") {
		t.Errorf("args = %v, want --peer-endpoints from the subdomain", agent.Args)
	}

	manifest = memberPod(nil, false, false)
	delete(manifest["spec"].(map[string]any), "subdomain")
	agent = mustAgent(t, admit(t, manifest))
	for _, arg := range agent.Args {
		if strings.HasPrefix(arg, "--peer-endpoints=") {
			t.Errorf("args = %v, want no --peer-endpoints without a subdomain", agent.Args)
		}
	}
}

// The sidecar's --data-dir follows etcd's, which may be a directory below
// the mount point of the data volume.
func TestDataDirFollowsEtcd(t *testing.T) {
	manifest := memberPod(nil, false, false)
	etcd := etcdContainer(manifest)
	etcd["command"] = []any{"etcd", "--name=etcd-abcde"}
	etcd["args"] = []any{"--data-dir=/var/lib/etcd/member-data"}
	agent := mustAgent(t, admit(t, manifest))
	if !slices.Contains(agent.Args, "--data-dir=/var/lib/etcd/member-data") {
		t.Errorf("args = %v, want etcd's --data-dir", agent.Args)
	}
	if got := mounts(agent)["data"]; got != "/var/lib/etcd" {
		t.Errorf("data mounted at %q, want /var/lib/etcd as in etcd's container", got)
	}

	manifest = memberPod(nil, false, false)
	etcdContainer(manifest)["command"] = []any{"etcd", "--name=etcd-abcde"}
	agent = mustAgent(t, admit(t, manifest))
	if !slices.Contains(agent.Args, "--data-dir=/var/lib/etcd") {
		t.Errorf("args = %v, want the default --data-dir=/var/lib/etcd", agent.Args)
	}
}

// A sidecar with requests below its limits would demote a Guaranteed member,
// which is evicted last and may get exclusive CPUs, to Burstable.
func TestKeepsGuaranteedQoS(t *testing.T) {
	manifest := memberPod(nil, true, false)
	resources := map[string]any{"cpu": "1", "memory": "1Gi"}
	etcdContainer(manifest)["resources"] = map[string]any{"requests": resources, "limits": resources}
	p := admit(t, manifest)
	mustAgent(t, p)
	if p.Status.QOSClass != "Guaranteed" {
		t.Errorf("QoS class = %q, want Guaranteed", p.Status.QOSClass)
	}
}

func TestLeavesOtherPodsAlone(t *testing.T) {
	cases := map[string]map[string]any{
		"without the opt-in label":  memberPod(map[string]string{"twok.aenix.io/inject": ""}, true, true),
		"opt-in label is not true":  memberPod(map[string]string{"twok.aenix.io/inject": "false"}, true, true),
		"without the cluster label": memberPod(map[string]string{"etcd-operator.cozystack.io/cluster": ""}, true, true),
	}
	notEtcd := memberPod(nil, true, true)
	notEtcd["spec"].(map[string]any)["containers"] = []any{map[string]any{"name": "snapshot", "image": "registry.k8s.io/etcd:3.6.4-0"}}
	cases["without an etcd container"] = notEtcd
	for name, manifest := range cases {
		t.Run(name, func(t *testing.T) {
			p := admit(t, manifest)
			if _, ok := agentOf(p); ok {
				t.Error("sidecar injected")
			}
			if sharesProcesses(p) {
				t.Error("shareProcessNamespace set")
			}
		})
	}
}

// A Pod that already has the sidecar is not given a second one.
func TestDoesNotInjectTwice(t *testing.T) {
	manifest := memberPod(nil, false, false)
	manifest["spec"].(map[string]any)["initContainers"] = []any{map[string]any{
		"name": "twok", "image": "ghcr.io/aenix-io/twok:main", "restartPolicy": "Always",
	}}
	p := admit(t, manifest)
	if got := names(p); !slices.Equal(got, []string{"twok"}) {
		t.Errorf("init containers = %v, want the one sidecar the Pod came with", got)
	}
}
