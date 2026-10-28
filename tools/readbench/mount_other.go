//go:build !linux

package main

import "errors"

func cmdMount(args []string) error {
	return errors.New("mount: the go-fuse backend is Linux-only")
}
