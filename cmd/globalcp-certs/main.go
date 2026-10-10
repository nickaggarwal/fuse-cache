// Command globalcp-certs issues a throwaway CA and the per-cluster
// certificates the global control plane and its coordinators present to
// each other.
//
// It exists so that tests and scripts/ops share ONE implementation of the
// trust model. A shell script stringing together openssl invocations would
// be a second definition of what a cluster identity is -- and since the
// control plane reads the cluster ID out of the first DNS SAN, a drift
// between the two definitions is an authorization bug, not a formatting
// one.
//
// NOT for production. internal/mtls.TestCA generates keys in memory with no
// passphrase, 24h validity, no rotation and no revocation. Real deployments
// get certificates from cert-manager or whatever the cluster already runs.
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"fuse-client/internal/mtls"
)

func main() {
	var (
		dir   = flag.String("dir", "", "Directory to write certificates into (required)")
		names = flag.String("names", "globalcp", "Comma-separated identities to issue. Each becomes <name>.crt/<name>.key, and the name IS the cluster ID the control plane will read.")
	)
	flag.Parse()

	if strings.TrimSpace(*dir) == "" {
		fmt.Fprintln(os.Stderr, "-dir is required")
		os.Exit(2)
	}
	if err := os.MkdirAll(*dir, 0o700); err != nil {
		fmt.Fprintf(os.Stderr, "create %s: %v\n", *dir, err)
		os.Exit(1)
	}

	ca, err := mtls.NewTestCA()
	if err != nil {
		fmt.Fprintf(os.Stderr, "generate CA: %v\n", err)
		os.Exit(1)
	}
	for _, raw := range strings.Split(*names, ",") {
		name := strings.TrimSpace(raw)
		if name == "" {
			continue
		}
		cfg, err := ca.WriteTo(*dir, name)
		if err != nil {
			fmt.Fprintf(os.Stderr, "issue %s: %v\n", name, err)
			os.Exit(1)
		}
		fmt.Printf("%s\t%s\t%s\t%s\n", name, cfg.CertFile, cfg.KeyFile, cfg.CAFile)
	}
}
