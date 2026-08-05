package nodelabels

import (
	"context"
	"reflect"
	"testing"
)

func TestParseSpec(t *testing.T) {
	tests := []struct {
		name       string
		raw        string
		wantKeys   []string
		wantRename map[string]string
	}{
		{"empty", "", nil, nil},
		{"whitespace", "   ", nil, nil},
		{"plain keys", "agentpool,zone", []string{"agentpool", "zone"}, nil},
		{
			"rename", "pool=agentpool",
			[]string{"agentpool"}, map[string]string{"agentpool": "pool"},
		},
		{
			"mixed with slashes and spaces",
			" pool=agentpool , topology.kubernetes.io/zone ",
			[]string{"agentpool", "topology.kubernetes.io/zone"},
			map[string]string{"agentpool": "pool"},
		},
		{"empty halves are dropped", "pool=,=agentpool,,ok", []string{"ok"}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			keys, rename := ParseSpec(tt.raw)
			if !reflect.DeepEqual(keys, tt.wantKeys) {
				t.Errorf("keys = %v, want %v", keys, tt.wantKeys)
			}
			if !reflect.DeepEqual(rename, tt.wantRename) {
				t.Errorf("rename = %v, want %v", rename, tt.wantRename)
			}
		})
	}
}

func TestSelect(t *testing.T) {
	node := map[string]string{
		"agentpool":                   "ckpt2404",
		"topology.kubernetes.io/zone": "westus2-1",
		"blank":                       "",
		"kubernetes.io/os":            "linux",
	}
	keys, rename := ParseSpec("pool=agentpool,topology.kubernetes.io/zone,blank,absent")
	got := Select(node, keys, rename)
	want := map[string]string{"pool": "ckpt2404", "topology.kubernetes.io/zone": "westus2-1"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Select = %v, want %v", got, want)
	}
	// Never slurp the whole label set: unrequested keys stay out.
	if _, ok := got["kubernetes.io/os"]; ok {
		t.Fatal("Select imported an unrequested key")
	}
	if Select(node, nil, nil) != nil {
		t.Fatal("no keys must select nothing")
	}
	if Select(map[string]string{}, keys, rename) != nil {
		t.Fatal("no matches must return nil, not an empty map")
	}
}

func TestFetch_FailsSoftOutsideCluster(t *testing.T) {
	// No keys: nothing to do, and no API call attempted.
	got, err := Fetch(context.Background(), Config{NodeName: "n1"})
	if got != nil || err != nil {
		t.Fatalf("Fetch with no keys = %v, %v", got, err)
	}
	// Missing node name is an error the caller can log and ignore.
	if _, err := Fetch(context.Background(), Config{Keys: []string{"agentpool"}}); err == nil {
		t.Fatal("empty node name must error")
	}
	// Outside a cluster there is no service-account token: an error, never a
	// panic and never a hang.
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	if _, err := Fetch(context.Background(), Config{NodeName: "n1", Keys: []string{"agentpool"}}); err == nil {
		t.Fatal("expected an error without in-cluster credentials")
	}
}
