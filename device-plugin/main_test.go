// Copyright 2026 The Hyperlight Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
)

func TestMain(m *testing.M) {
	if len(os.Args) > 2 && strings.HasPrefix(os.Args[1], "--probe-hypervisor=") {
		if os.Getenv("TEST_PROBE_HANG") == "1" {
			time.Sleep(time.Minute)
		}
		hostPath := ""
		if len(os.Args) > 3 {
			hostPath = strings.TrimPrefix(os.Args[3], "--probe-host-path=")
		}
		err := checkDeviceWithHost(strings.TrimPrefix(os.Args[1], "--probe-hypervisor="), strings.TrimPrefix(os.Args[2], "--probe-path="), hostPath)
		if err != nil {
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func testPlugin(t *testing.T) *HyperlightDevicePlugin {
	t.Helper()
	dir := t.TempDir()
	return &HyperlightDevicePlugin{
		devices:    []*pluginapi.Device{{ID: "kvm-0", Health: pluginapi.Unhealthy}},
		hypervisor: "kvm", cdiPath: filepath.Join(dir, "hyperlight.json"),
		cdiSpec: desiredCDISpec("kvm", "/dev/kvm"),
		probe:   func(context.Context) error { return nil }, stopCh: make(chan struct{}),
		healthServer: health.NewServer(), interval: 10 * time.Millisecond,
		socket: filepath.Join(dir, "plugin.sock"), kubeletSocket: filepath.Join(dir, "kubelet.sock"),
		registrationTimeout: 100 * time.Millisecond,
	}
}

func TestCDIRepair(t *testing.T) {
	t.Setenv("DEVICE_UID", "1234")
	t.Setenv("DEVICE_GID", "5678")
	expected := desiredCDISpec("kvm", "/dev/kvm")
	cases := map[string][]byte{
		"missing":     nil,
		"malformed":   []byte("{"),
		"name":        bytes.ReplaceAll(expected, []byte(`"name": "kvm"`), []byte(`"name": "stale"`)),
		"kind":        bytes.ReplaceAll(expected, []byte(resourceName), []byte("example.dev/wrong")),
		"path":        bytes.ReplaceAll(expected, []byte("/dev/kvm"), []byte("/dev/null")),
		"uid":         bytes.ReplaceAll(expected, []byte("1234"), []byte("0")),
		"gid":         bytes.ReplaceAll(expected, []byte("5678"), []byte("0")),
		"permissions": bytes.ReplaceAll(expected, []byte(`"rw"`), []byte(`"rwm"`)),
		"extra edits": bytes.ReplaceAll(expected, []byte(`"containerEdits": {`), []byte(`"containerEdits": {"mounts": [{"hostPath":"/","containerPath":"/host"}],`)),
	}
	for name, initial := range cases {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "hyperlight.json")
			unrelated := filepath.Join(dir, "other.json")
			if err := os.WriteFile(unrelated, []byte("leave me"), 0600); err != nil {
				t.Fatal(err)
			}
			if initial != nil {
				if err := os.WriteFile(path, initial, 0600); err != nil {
					t.Fatal(err)
				}
			}
			if err := reconcileCDI(path, expected); err != nil {
				t.Fatal(err)
			}
			actual, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(actual, expected) {
				t.Fatalf("unexpected spec: %s", actual)
			}
			info, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			if info.Mode().Perm() != 0644 {
				t.Fatalf("mode: %v", info.Mode())
			}
			if err := reconcileCDI(path, expected); err != nil {
				t.Fatal(err)
			}
			after, _ := os.Stat(path)
			if !os.SameFile(info, after) {
				t.Fatal("valid file replaced")
			}
			data, _ := os.ReadFile(unrelated)
			if string(data) != "leave me" {
				t.Fatal("unrelated file changed")
			}
			entries, _ := os.ReadDir(dir)
			if len(entries) != 2 {
				t.Fatalf("temporary files leaked: %v", entries)
			}
		})
	}
}

func TestCDIRefusesNonRegularPaths(t *testing.T) {
	for _, kind := range []string{"symlink", "directory"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "hyperlight.json")
			target := filepath.Join(dir, "target")
			if err := os.WriteFile(target, []byte("untouched"), 0600); err != nil {
				t.Fatal(err)
			}
			var err error
			if kind == "symlink" {
				err = os.Symlink(target, path)
			} else {
				err = os.Mkdir(path, 0755)
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := reconcileCDI(path, desiredCDISpec("kvm", "/dev/kvm")); err == nil {
				t.Fatal("expected refusal")
			}
			data, _ := os.ReadFile(target)
			if string(data) != "untouched" {
				t.Fatal("followed symlink")
			}
		})
	}
}

func TestCDIAtomicReplacement(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hyperlight.json")
	first := desiredCDISpec("kvm", "/dev/kvm")
	second := desiredCDISpec("mshv", "/dev/mshv")
	if err := reconcileCDI(path, first); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		for {
			select {
			case <-done:
				result <- nil
				return
			default:
			}
			data, err := os.ReadFile(path)
			if err != nil {
				result <- err
				return
			}
			if !json.Valid(data) || (!bytes.Equal(data, first) && !bytes.Equal(data, second)) {
				result <- fmt.Errorf("partial spec: %s", data)
				return
			}
		}
	}()
	for i := 0; i < 30; i++ {
		desired := first
		if i%2 == 0 {
			desired = second
		}
		if err := reconcileCDI(path, desired); err != nil {
			close(done)
			t.Fatal(err)
		}
	}
	close(done)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

type watchStream struct {
	pluginapi.DevicePlugin_ListAndWatchServer
	ctx     context.Context
	updates chan *pluginapi.ListAndWatchResponse
}

func (s *watchStream) Context() context.Context { return s.ctx }
func (s *watchStream) Send(r *pluginapi.ListAndWatchResponse) error {
	select {
	case s.updates <- r:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}
func expectHealth(t *testing.T, s *watchStream, want string) {
	t.Helper()
	select {
	case response := <-s.updates:
		if response.Devices[0].Health != want {
			t.Fatalf("want %s, got %v", want, response)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("no %s update", want)
	}
}

func TestAllocationAndWatchRecover(t *testing.T) {
	p := testPlugin(t)
	var unusable atomic.Bool
	p.probe = func(context.Context) error {
		if unusable.Load() {
			return errors.New("device unusable")
		}
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := &watchStream{ctx: ctx, updates: make(chan *pluginapi.ListAndWatchResponse, 8)}
	done := make(chan error, 1)
	go func() { done <- p.ListAndWatch(&pluginapi.Empty{}, stream) }()
	defer func() { cancel(); <-done }()
	expectHealth(t, stream, pluginapi.Healthy)
	req := &pluginapi.AllocateRequest{ContainerRequests: []*pluginapi.ContainerAllocateRequest{{DevicesIds: []string{"kvm-0"}}}}
	if err := os.Remove(p.cdiPath); err != nil {
		t.Fatal(err)
	}
	response, err := p.Allocate(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if response.ContainerResponses[0].CdiDevices[0].Name != "hyperlight.dev/hypervisor=kvm" {
		t.Fatal(response)
	}
	if ok, err := matchesCDI(p.cdiPath, p.cdiSpec); err != nil || !ok {
		t.Fatalf("not repaired: %v", err)
	}
	unusable.Store(true)
	if _, err := p.Allocate(ctx, req); status.Code(err) != codes.Unavailable {
		t.Fatalf("allocation allowed: %v", err)
	}
	expectHealth(t, stream, pluginapi.Unhealthy)
	unusable.Store(false)
	expectHealth(t, stream, pluginapi.Healthy)
	// An unrecoverable owned path blocks both new allocation and health advertisement.
	p.checkGate <- struct{}{}
	if err := os.Remove(p.cdiPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(p.cdiPath, 0755); err != nil {
		t.Fatal(err)
	}
	<-p.checkGate
	if _, err := p.Allocate(ctx, req); status.Code(err) != codes.Unavailable {
		t.Fatalf("repair failure ignored: %v", err)
	}
	expectHealth(t, stream, pluginapi.Unhealthy)
	if err := os.Remove(p.cdiPath); err != nil {
		t.Fatal(err)
	}
	expectHealth(t, stream, pluginapi.Healthy)
	req.ContainerRequests[0].DevicesIds = []string{"unknown"}
	if _, err := p.Allocate(ctx, req); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid device accepted: %v", err)
	}
}

type registrationServer struct {
	pluginapi.UnimplementedRegistrationServer
	blocked          atomic.Bool
	expectedResource string
}

func (s *registrationServer) Register(ctx context.Context, r *pluginapi.RegisterRequest) (*pluginapi.Empty, error) {
	if s.blocked.Load() {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	expected := s.expectedResource
	if expected == "" {
		expected = resourceName
	}
	if r.ResourceName != expected {
		return nil, status.Error(codes.InvalidArgument, "wrong resource")
	}
	return &pluginapi.Empty{}, nil
}

func TestRegistrationFailureAndRecovery(t *testing.T) {
	p := testPlugin(t)
	listener, err := net.Listen("unix", p.kubeletSocket)
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	registration := &registrationServer{}
	registration.blocked.Store(true)
	pluginapi.RegisterRegistrationServer(server, registration)
	go server.Serve(listener)
	defer server.Stop()
	if err := p.Start(); err == nil {
		t.Fatal("stalled registration succeeded")
	}
	if _, err := os.Stat(p.socket); !os.IsNotExist(err) {
		t.Fatalf("failed start leaked socket: %v", err)
	}
	registration.blocked.Store(false)
	if err := p.Start(); err != nil {
		t.Fatal(err)
	}
	defer p.Stop()
	if err := checkHealth("liveness", p.socket); err != nil {
		t.Fatal(err)
	}
	if err := checkHealth("readiness", p.socket); err == nil {
		t.Fatal("registration alone marked ready")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn, err := dialPlugin(ctx, p.socket)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	watch, err := pluginapi.NewDevicePluginClient(conn).ListAndWatch(ctx, &pluginapi.Empty{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := watch.Recv(); err != nil {
		t.Fatal(err)
	}
	if err := checkHealth("readiness", p.socket); err != nil {
		t.Fatal(err)
	}
	// Recreate the server after kubelet removes the plugin socket.
	if err := os.Remove(p.socket); err != nil {
		t.Fatal(err)
	}
	returned := make(chan struct{})
	go func() { p.watchKubeletRestart(); close(returned) }()
	select {
	case <-returned:
	case <-time.After(3 * time.Second):
		t.Fatal("missed socket deletion")
	}
	cancel()
	conn.Close()
	p.Stop()
	if err := p.Start(); err != nil {
		t.Fatal(err)
	}
	if err := checkHealth("readiness", p.socket); err == nil {
		t.Fatal("old registration/stream marked restarted server ready")
	}
}

func TestReadinessRequiresRegistration(t *testing.T) {
	p := testPlugin(t)
	p.watchers = 1
	if err := p.checkReadiness(context.Background()); err != nil {
		t.Fatal(err)
	}
	response, err := p.healthServer.Check(context.Background(), &healthpb.HealthCheckRequest{})
	if err != nil || response.Status != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("unregistered readiness: %v %v", response, err)
	}
}

func TestDeviceProbeRejectsUnusableDevices(t *testing.T) {
	regular := filepath.Join(t.TempDir(), "kvm")
	if err := os.WriteFile(regular, nil, 0600); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{regular, regular + "-missing", "/dev/null"} {
		if err := checkDevice("kvm", path); err == nil {
			t.Fatalf("accepted %s", path)
		}
	}
	info, _ := os.Stat("/dev/null")
	if err := checkDevice("kvm", "/dev/null"); err == nil {
		t.Fatal("accepted null")
	}
	after, _ := os.Stat("/dev/null")
	if !os.SameFile(info, after) || info.Mode() != after.Mode() {
		t.Fatal("changed host device")
	}
}

func TestDeviceProbeTimeoutAndNoOverlap(t *testing.T) {
	t.Setenv("TEST_PROBE_HANG", "1")
	p := &deviceProbe{}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := p.check(ctx, "kvm", "/dev/null"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("missing deadline: %v", err)
	}
	if p.pending == nil {
		t.Fatal("missing child reap")
	}
	select {
	case <-p.pending:
	case <-time.After(3 * time.Second):
		t.Fatal("helper did not exit")
	}
	p.pending = make(chan error, 1)
	if err := p.check(context.Background(), "kvm", "/dev/null"); err == nil || !strings.Contains(err.Error(), "has not exited") {
		t.Fatalf("started overlapping probe: %v", err)
	}
}

func TestCDIUnwritableDirectoryRecovers(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("requires non-root permission enforcement")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "hyperlight.json")
	if err := os.Chmod(dir, 0555); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(dir, 0755)
	expected := desiredCDISpec("kvm", "/dev/kvm")
	if err := reconcileCDI(path, expected); err == nil {
		t.Fatal("repair succeeded in unwritable directory")
	}
	if err := os.Chmod(dir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := reconcileCDI(path, expected); err != nil {
		t.Fatal(err)
	}
}

func TestAllocationHealthChangesWakeEveryWatcher(t *testing.T) {
	p := testPlugin(t)
	p.interval = time.Hour
	var unusable atomic.Bool
	p.probe = func(context.Context) error {
		if unusable.Load() {
			return errors.New("device unusable")
		}
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	streams := []*watchStream{{ctx: ctx, updates: make(chan *pluginapi.ListAndWatchResponse, 8)}, {ctx: ctx, updates: make(chan *pluginapi.ListAndWatchResponse, 8)}}
	done := make(chan error, 2)
	for _, stream := range streams {
		go func() { done <- p.ListAndWatch(&pluginapi.Empty{}, stream) }()
		expectHealth(t, stream, pluginapi.Healthy)
	}
	defer func() { cancel(); <-done; <-done }()
	req := &pluginapi.AllocateRequest{ContainerRequests: []*pluginapi.ContainerAllocateRequest{{DevicesIds: []string{"kvm-0"}}}}
	unusable.Store(true)
	if _, err := p.Allocate(ctx, req); status.Code(err) != codes.Unavailable {
		t.Fatal(err)
	}
	for _, stream := range streams {
		expectHealth(t, stream, pluginapi.Unhealthy)
	}
	unusable.Store(false)
	if _, err := p.Allocate(ctx, req); err != nil {
		t.Fatal(err)
	}
	for _, stream := range streams {
		expectHealth(t, stream, pluginapi.Healthy)
	}
}

func TestCancelledChecksPreserveSharedReadiness(t *testing.T) {
	for _, initiallyHealthy := range []bool{false, true} {
		t.Run(fmt.Sprint(initiallyHealthy), func(t *testing.T) {
			p := testPlugin(t)
			p.registered = true
			p.watchers = 1
			initialErr := errors.New("device unusable")
			p.probe = func(context.Context) error {
				if initiallyHealthy {
					return nil
				}
				return initialErr
			}
			_ = p.checkReadiness(context.Background())
			before, _ := p.healthServer.Check(context.Background(), &healthpb.HealthCheckRequest{})
			p.probe = func(ctx context.Context) error { return ctx.Err() }
			for _, deadline := range []bool{false, true} {
				var ctx context.Context
				var cancel context.CancelFunc
				expected := codes.Canceled
				if deadline {
					ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
					expected = codes.DeadlineExceeded
				} else {
					ctx, cancel = context.WithCancel(context.Background())
					cancel()
				}
				defer cancel()
				if _, err := p.Allocate(ctx, &pluginapi.AllocateRequest{}); status.Code(err) != expected {
					t.Fatalf("status=%v, want %v", err, expected)
				}
				after, _ := p.healthServer.Check(context.Background(), &healthpb.HealthCheckRequest{})
				if after.Status != before.Status {
					t.Fatalf("caller cancellation changed shared readiness: %v -> %v", before, after)
				}
			}
		})
	}
}

func TestCancellationDuringProbePreservesReadiness(t *testing.T) {
	p := testPlugin(t)
	if err := p.checkReadiness(context.Background()); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	p.probe = func(ctx context.Context) error { close(entered); <-ctx.Done(); return ctx.Err() }
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- p.checkReadiness(ctx) }()
	<-entered
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if p.readinessErr != nil || !p.readinessKnown {
		t.Fatal("cancelled probe replaced healthy observation")
	}
}

func TestCancelledCheckDoesNotWaitForActiveProbe(t *testing.T) {
	p := testPlugin(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	p.probe = func(context.Context) error { close(entered); <-release; return nil }
	first := make(chan error, 1)
	go func() { first <- p.checkReadiness(context.Background()) }()
	<-entered
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	second := make(chan error, 1)
	go func() { second <- p.checkReadiness(ctx) }()
	select {
	case err := <-second:
		if !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
	case <-time.After(time.Second):
		t.Error("cancelled check waited for active probe")
	}
	close(release)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
}

func TestProbeDeadlineRemainsDeviceFailure(t *testing.T) {
	p := testPlugin(t)
	p.probe = func(context.Context) error { return context.DeadlineExceeded }
	_, err := p.Allocate(context.Background(), &pluginapi.AllocateRequest{})
	if status.Code(err) != codes.Unavailable || !errors.Is(p.readinessErr, context.DeadlineExceeded) {
		t.Fatalf("internal probe deadline was mistaken for caller cancellation: %v", err)
	}
}

func TestCDIOwnershipValuesFitRuntimeSchema(t *testing.T) {
	for _, value := range []string{"0", "65534", "4294967295", "4294967296", "18446744073709551616", "-1", "invalid"} {
		t.Run(value, func(t *testing.T) {
			t.Setenv("DEVICE_UID", value)
			t.Setenv("DEVICE_GID", value)
			p := testPlugin(t)
			if err := p.checkReadiness(context.Background()); err != nil {
				t.Fatal(err)
			}
			var spec struct {
				Devices []struct {
					ContainerEdits struct {
						DeviceNodes []struct {
							UID uint32
							GID uint32
						}
					}
				}
			}
			if err := json.Unmarshal(p.cdiSpec, &spec); err != nil {
				t.Fatal(err)
			}
			node := spec.Devices[0].ContainerEdits.DeviceNodes[0]
			expected := uint32(65534)
			switch value {
			case "0":
				expected = 0
			case "4294967295":
				expected = 4294967295
			}
			if node.UID != expected || node.GID != expected {
				t.Fatalf("ownership=%v, want %d", node, expected)
			}
		})
	}
}

func TestBootstrapGrantNeedsNoHypervisorOpenOrCDI(t *testing.T) {
	t.Setenv("DEVICE_COUNT", "2000")
	p, err := newDevicePluginWithDevice("kvm", "/dev/null", true)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.devices) != 1 || p.registrationResource() != deviceAccessResource || p.socket != deviceAccessSock {
		t.Fatal("bootstrap registration is not isolated")
	}
	p.cdiPath = filepath.Join(t.TempDir(), "must-not-create.json")
	req := &pluginapi.AllocateRequest{ContainerRequests: []*pluginapi.ContainerAllocateRequest{{DevicesIds: []string{"kvm-0"}}}}
	response, err := p.Allocate(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	allocated := response.ContainerResponses[0]
	if len(allocated.CdiDevices) != 0 || len(allocated.Devices) != 1 || len(allocated.Mounts) != 0 || len(allocated.Envs) != 0 {
		t.Fatalf("unexpected bootstrap grant: %v", allocated)
	}
	device := allocated.Devices[0]
	if device.HostPath != "/dev/null" || device.ContainerPath != "/dev/null" || device.Permissions != "rw" {
		t.Fatal(device)
	}
	if _, err := os.Stat(p.cdiPath); !os.IsNotExist(err) {
		t.Fatal("bootstrap wrote CDI")
	}
	if err := checkDevice("kvm", "/dev/null"); err == nil {
		t.Fatal("main probe did not distinguish access inventory from usability")
	}
	req.ContainerRequests[0].DevicesIds = []string{"kvm-1"}
	if _, err := p.Allocate(context.Background(), req); status.Code(err) != codes.InvalidArgument {
		t.Fatal("bootstrap accepted another grant")
	}
}

func TestBootstrapRefusesNonDevicePaths(t *testing.T) {
	path := filepath.Join(t.TempDir(), "device")
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	p, err := newDevicePluginWithDevice("kvm", path, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Allocate(context.Background(), &pluginapi.AllocateRequest{}); status.Code(err) != codes.Unavailable {
		t.Fatal("bootstrap accepted regular file")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("/dev/null", path); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Allocate(context.Background(), &pluginapi.AllocateRequest{}); status.Code(err) != codes.Unavailable {
		t.Fatal("bootstrap accepted symlink")
	}
}

func TestBootstrapRegistersAndAllocatesOverGRPC(t *testing.T) {
	p, err := newDevicePluginWithDevice("kvm", "/dev/null", true)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	p.socket = filepath.Join(dir, "access.sock")
	p.kubeletSocket = filepath.Join(dir, "kubelet.sock")
	listener, err := net.Listen("unix", p.kubeletSocket)
	if err != nil {
		t.Fatal(err)
	}
	registration := grpc.NewServer()
	pluginapi.RegisterRegistrationServer(registration, &registrationServer{expectedResource: deviceAccessResource})
	go registration.Serve(listener)
	defer registration.Stop()
	if err := p.Start(); err != nil {
		t.Fatal(err)
	}
	defer p.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, err := dialPlugin(ctx, p.socket)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := pluginapi.NewDevicePluginClient(conn)
	watch, err := client.ListAndWatch(ctx, &pluginapi.Empty{})
	if err != nil {
		t.Fatal(err)
	}
	update, err := watch.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if len(update.Devices) != 1 || update.Devices[0].Health != pluginapi.Healthy {
		t.Fatal(update)
	}
	if err := checkHealth("readiness", p.socket); err != nil {
		t.Fatal(err)
	}
	response, err := client.Allocate(ctx, &pluginapi.AllocateRequest{ContainerRequests: []*pluginapi.ContainerAllocateRequest{{DevicesIds: []string{update.Devices[0].ID}}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(response.ContainerResponses[0].Devices) != 1 || len(response.ContainerResponses[0].CdiDevices) != 0 {
		t.Fatal(response)
	}
}
