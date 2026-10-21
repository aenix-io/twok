//go:build kubernetes

// Package kubernetes runs a two-member etcd as a StatefulSet on a real
// Kubernetes cluster, partitions the members with Cilium network policies and
// checks what each side accepts while they are apart.
//
// The cluster is reached with kubectl alone, with the kubeconfig in
// K8S_KUBECONFIG; everything is created in K8S_NAMESPACE (default
// twok-test) and deleted at the end unless K8S_KEEP is set. Without
// TWOK_IMAGE the members run plain upstream etcd, which is the baseline;
// with it every member has the twok sidecar from that image, with the
// arbiter of the overlay in TWOK_OVERLAY (default arbiter-http).
package kubernetes

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"testing"
	"text/template"
	"time"
)

const (
	// The etcdctl release used by the writers and the probe, with the
	// checksum from build/etcd.sha256.
	etcdVersion = "v3.6.15"
	etcdSHA256  = "b51d86e6bff2168de5ba8cd631c0fe6c785c7a1b32b083079f5677d6b7e2772c"

	// fetchImage downloads etcdctl; shellImage runs the loops, for GNU date
	// with milliseconds. Both pinned by the index digest read from Docker Hub.
	fetchImage = "docker.io/library/busybox:1.37.0@sha256:bdf57e528e45e4433820e045b29b4597825a1c9e38353532d90a01445013f82e"
	shellImage = "docker.io/library/debian:bookworm-20261005-slim@sha256:7c7b2c966bc9ee8cedfeef67e0e279108992c77681fa595db4a9d65c06ccc587"

	memberLabel = "twok.aenix.io/member"
)

var members = []string{"etcd-0", "etcd-1"}

type config struct {
	kubeconfig      string
	namespace       string
	twokImage       string
	twokMode        string
	overlay         string
	ciliumNamespace string
	partitionFor    time.Duration
	healFor         time.Duration
	healTimeout     time.Duration
	keep            bool
	results         string
}

var cfg config

func TestMain(m *testing.M) {
	cfg = config{
		kubeconfig:      os.Getenv("K8S_KUBECONFIG"),
		namespace:       envOr("K8S_NAMESPACE", "twok-test"),
		twokImage:       os.Getenv("TWOK_IMAGE"),
		twokMode:        envOr("TWOK_MODE", "enforce"),
		overlay:         envOr("TWOK_OVERLAY", "arbiter-http"),
		ciliumNamespace: envOr("K8S_CILIUM_NAMESPACE", "cozy-cilium"),
		partitionFor:    durationEnv("TWOK_PARTITION_FOR", 60*time.Second),
		healFor:         durationEnv("TWOK_HEAL_FOR", 10*time.Second),
		healTimeout:     durationEnv("TWOK_HEAL_TIMEOUT", 5*time.Minute),
		keep:            os.Getenv("K8S_KEEP") != "",
		results:         os.Getenv("TWOK_RESULTS"),
	}
	if cfg.kubeconfig == "" {
		fmt.Println("K8S_KUBECONFIG is not set, skipping Kubernetes tests")
		os.Exit(0)
	}
	if cfg.ciliumNamespace == "-" {
		cfg.ciliumNamespace = ""
	}
	os.Exit(m.Run())
}

func envOr(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

func durationEnv(name string, fallback time.Duration) time.Duration {
	v := os.Getenv(name)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		panic(fmt.Sprintf("%s: %v", name, err))
	}
	return d
}

func (c config) baseline() bool { return c.twokImage == "" }

func (c config) mode() string {
	if c.baseline() {
		return "baseline (upstream etcd, no arbiter)"
	}
	return "twok " + c.twokImage + " as a sidecar in mode " + c.twokMode + ", overlay " + c.overlay + ", arbiter " + c.arbiterURL()
}

// arbiterURL is the arbiter URL the overlay gives the members.
func (c config) arbiterURL() string {
	switch c.overlay {
	case "arbiter-etcd":
		return "etcd://arbiter-client:2379/twok/etcd"
	case "arbiter-kubernetes":
		return "kubernetes://" + c.namespace + "/twok-arbiter"
	default:
		return "http://arbiter:8080/twok/etcd"
	}
}

type env struct {
	t      *testing.T
	log    *bytes.Buffer // the raw record of the run, for TWOK_RESULTS
	events []event       // extra timeline entries
}

type event struct {
	at   int64 // Unix ms on the local clock
	what string
}

// mark adds an entry to the timeline.
func (e *env) mark(at int64, what string) {
	e.events = append(e.events, event{at, what})
}

func (e *env) logf(format string, args ...any) {
	e.t.Helper()
	line := fmt.Sprintf(format, args...)
	e.t.Log(line)
	e.log.WriteString(line + "\n")
}

// run runs kubectl with the configured kubeconfig. namespace "" means the
// test namespace.
func (e *env) run(ctx context.Context, namespace, stdin string, args ...string) (string, error) {
	if namespace == "" {
		namespace = cfg.namespace
	}
	full := append([]string{"--kubeconfig", cfg.kubeconfig, "--namespace", namespace}, args...)
	var stdout, stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, "kubectl", full...)
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if stdin != "" {
		cmd.Stdin = strings.NewReader(stdin)
	}
	if err := cmd.Run(); err != nil {
		return stdout.String(), fmt.Errorf("kubectl %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return stdout.String(), nil
}

func (e *env) kubectl(args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	return e.run(ctx, "", "", args...)
}

func (e *env) must(args ...string) string {
	e.t.Helper()
	out, err := e.kubectl(args...)
	if err != nil {
		e.t.Fatal(err)
	}
	return out
}

func (e *env) apply(manifest string) {
	e.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if _, err := e.run(ctx, "", manifest, "apply", "--filename", "-"); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) deleteManifest(manifest string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	_, err := e.run(ctx, "", manifest, "delete", "--ignore-not-found", "--wait=true", "--filename", "-")
	return err
}

func (e *env) applyFile(path string) {
	e.t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		e.t.Fatal(err)
	}
	e.apply(string(data))
}

func (e *env) deleteFile(path string) {
	e.t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		e.t.Fatal(err)
	}
	if err := e.deleteManifest(string(data)); err != nil {
		e.t.Fatal(err)
	}
}

// membersManifest renders the member manifests: the baseline without the
// sidecar, or the TWOK_OVERLAY overlay with TWOK_IMAGE for the sidecar and
// the HTTP arbiter, and TWOK_MODE in the ConfigMap.
func (e *env) membersManifest() string {
	e.t.Helper()
	dir := "manifests/baseline"
	if !cfg.baseline() {
		dir = "../../deploy/kubernetes/" + cfg.overlay
	}
	out, err := exec.Command("kubectl", "kustomize", dir).Output()
	if err != nil {
		e.t.Fatalf("kubectl kustomize %s: %v", dir, err)
	}
	manifest := string(out)
	if !cfg.baseline() {
		manifest = regexp.MustCompile(`ghcr\.io/aenix-io/twok:\S+`).ReplaceAllLiteralString(manifest, cfg.twokImage)
		manifest = strings.Replace(manifest, `mode: "off"`, "mode: "+cfg.twokMode, 1)
	}
	return manifest
}

func render(path string, data any) (string, error) {
	tmpl, err := template.ParseFiles(path)
	if err != nil {
		return "", err
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return "", err
	}
	return buf.String(), nil
}

func indent(text, prefix string) string {
	lines := strings.Split(strings.TrimRight(text, "\n"), "\n")
	for i, l := range lines {
		if l != "" {
			lines[i] = prefix + l
		}
	}
	return strings.Join(lines, "\n")
}

type helperData struct {
	Member, Node, ShellImage, ToolsInit string
}

func (e *env) helperManifest(path, member, node string) string {
	e.t.Helper()
	tools, err := render("manifests/tools-init.yaml", map[string]string{
		"FetchImage": fetchImage, "EtcdVersion": etcdVersion, "EtcdSHA256": etcdSHA256,
	})
	if err != nil {
		e.t.Fatal(err)
	}
	out, err := render(path, helperData{Member: member, Node: node, ShellImage: shellImage, ToolsInit: indent(tools, "    ")})
	if err != nil {
		e.t.Fatal(err)
	}
	return out
}

func (e *env) waitReady(timeout time.Duration, pods ...string) {
	e.t.Helper()
	args := append([]string{"wait", "--for=condition=Ready", "--timeout=" + timeout.String()}, prefixAll("pod/", pods)...)
	ctx, cancel := context.WithTimeout(context.Background(), timeout+30*time.Second)
	defer cancel()
	if _, err := e.run(ctx, "", "", args...); err != nil {
		e.t.Fatal(err)
	}
}

func prefixAll(prefix string, items []string) []string {
	out := make([]string, len(items))
	for i, s := range items {
		out[i] = prefix + s
	}
	return out
}

type podInfo struct {
	Spec struct {
		NodeName string `json:"nodeName"`
	} `json:"spec"`
	Status struct {
		PodIP string `json:"podIP"`
	} `json:"status"`
}

func (e *env) pod(name string) podInfo {
	e.t.Helper()
	var p podInfo
	if err := json.Unmarshal([]byte(e.must("get", "pod", name, "--output", "json")), &p); err != nil {
		e.t.Fatal(err)
	}
	return p
}

// labelMembers puts the member label on the member Pods and waits until
// Cilium has given each one an identity that carries it.
// statefulset.kubernetes.io/pod-name is not part of a Cilium identity, so
// the policies cannot select on it; see manifests/partition.yaml.
func (e *env) labelMembers() {
	e.t.Helper()
	for _, m := range members {
		e.must("label", "pod", m, "--overwrite", memberLabel+"="+m)
	}
	deadline := time.Now().Add(2 * time.Minute)
	for _, m := range members {
		for {
			var cep struct {
				Status struct {
					Identity struct {
						ID     int      `json:"id"`
						Labels []string `json:"labels"`
					} `json:"identity"`
				} `json:"status"`
			}
			out, err := e.kubectl("get", "ciliumendpoint", m, "--output", "json")
			if err == nil && json.Unmarshal([]byte(out), &cep) == nil {
				want := "k8s:" + memberLabel + "=" + m
				found := false
				for _, l := range cep.Status.Identity.Labels {
					found = found || l == want
				}
				if found {
					e.logf("%s: Cilium identity %d carries %s", m, cep.Status.Identity.ID, want)
					break
				}
			}
			if time.Now().After(deadline) {
				e.t.Fatalf("%s: the Cilium identity has no %s label after 2m", m, memberLabel)
			}
			time.Sleep(2 * time.Second)
		}
	}
}

// exec runs a command in a container of a Pod of the test namespace.
func (e *env) exec(pod string, cmd ...string) (string, error) {
	return e.kubectl(append([]string{"exec", pod, "--"}, cmd...)...)
}

// etcdctl runs etcdctl in the probe against one member.
func (e *env) etcdctl(member string, args ...string) (string, error) {
	return e.exec("probe", append([]string{
		"/tools/etcdctl", "--endpoints=http://" + member + ".etcd:2379", "--dial-timeout=2s", "--command-timeout=3s",
	}, args...)...)
}
