// Copyright 2026 The Hyperlight Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"
)

// Keep at most one helper alive if a kernel operation cannot be interrupted.
type deviceProbe struct {
	mu      sync.Mutex
	pending chan error
}

func (p *deviceProbe) check(ctx context.Context, hypervisor, path string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.pending != nil {
		select {
		case <-p.pending:
			p.pending = nil
		default:
			return fmt.Errorf("previous device probe has not exited")
		}
	}
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, executable, "--probe-hypervisor="+hypervisor, "--probe-path="+path)
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return err
	}
	pending := make(chan error, 1)
	p.pending = pending
	go func() { pending <- cmd.Wait() }()
	select {
	case err := <-pending:
		p.pending = nil
		if err != nil {
			return fmt.Errorf("device usability probe: %w", err)
		}
		return nil
	case <-ctx.Done():
		return fmt.Errorf("device usability probe: %w", ctx.Err())
	}
}

// checkDevice never changes host device ownership, permissions or existing VMs.
func checkDevice(hypervisor, path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeCharDevice == 0 {
		return fmt.Errorf("%s is not a character device", path)
	}
	fd, err := syscall.Open(path, syscall.O_RDWR|syscall.O_CLOEXEC|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return fmt.Errorf("open hypervisor: %w", err)
	}
	defer syscall.Close(fd)
	var stat syscall.Stat_t
	if err := syscall.Fstat(fd, &stat); err != nil {
		return err
	}
	original, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Mode&syscall.S_IFMT != syscall.S_IFCHR || stat.Rdev != original.Rdev {
		return fmt.Errorf("hypervisor device identity changed")
	}
	switch hypervisor {
	case "kvm":
		// Linux KVM API: GET_API_VERSION and CREATE_VM (no guest memory or vCPUs).
		version, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd), 0xAE00, 0)
		if errno != 0 {
			return fmt.Errorf("KVM_GET_API_VERSION: %w", errno)
		}
		if version != 12 {
			return fmt.Errorf("unsupported KVM API version %d", version)
		}
		vm, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd), 0xAE01, 0)
		if errno != 0 {
			return fmt.Errorf("KVM_CREATE_VM: %w", errno)
		}
		return syscall.Close(int(vm))
	case "mshv":
		// MSHV currently has only an open check, not a VM-creation readiness guarantee.
		return nil
	default:
		return fmt.Errorf("unsupported hypervisor %q", hypervisor)
	}
}
