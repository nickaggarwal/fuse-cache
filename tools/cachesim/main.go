// Command cachesim simulates fuse-cache across a fleet serving many model
// deployments: cache misses, cold starts, eviction under NVMe pressure, and
// how holder leases and the replica reconciler repair the cluster afterwards.
//
// It is a discrete-event model of the policies, not of the bytes: each
// deployment start needs the layers of one base image plus one model's
// weights; each layer comes from the node's NVMe, a peer holding it, or the
// registry (registry fill), at throughputs measured on AKS cpu-w3 and Nebius.
// Eviction is LRU against each node's NVMe budget; a node advertises a holder
// row per layer with a lease it renews; the reconciler tops up hot layers
// below the replica target, within a per-node budget and a cluster-wide slot
// cap. Node failures drop a node's rows only when their leases lapse.
//
//	go run ./tools/cachesim -nodes 16 -hours 6 -seed 1
//	go run ./tools/cachesim -scenario all -out results/
//
// Output: a per-scenario summary (cold starts by source, p50/p95 start time,
// evictions, misses from evicted layers, reconcile copies, stale-holder
// reads) and an optional CSV timeline.
package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// Throughputs (MB/s) and fixed costs, from the cold-start runs.
const (
	registryMBps   = 237.0 // ACR -> node, 16 parallel streams (cpu-w3)
	peerMBps       = 400.0 // node -> node, 16 x 4 MiB (cpu-w3)
	diskMBps       = 424.0 // local NVMe read (cpu-w3)
	vllmFixedSec   = 50.0  // engine init + compile, independent of bytes
	peerReadSec    = 0.010 // per-read overhead on a peer path
	contentionStep = 0.07  // per concurrent reader on one holder (Azure fan-out: 8 -> 11 ms at 5)
)

type layer struct {
	id     string
	sizeMB float64
	kind   string // "base" or "model"
}

type image struct {
	name   string
	layers []*layer
}

type model struct {
	name    string
	weights *layer
	base    int // index into images
	weight  float64
}

type holderRow struct {
	expires float64
	gen     int
}

type node struct {
	id      int
	family  string
	capMB   float64
	usedMB  float64
	lru     map[string]float64 // layer -> last access (sim seconds)
	alive   bool
	readers int // concurrent peer readers right now (contention)
	nextGen int
	running map[int]bool // deployments running (pins their layers)
}

type cluster struct {
	cfg     config
	rng     *rand.Rand
	now     float64
	nodes   []*node
	images  []image
	models  []model
	layers  map[string]*layer
	holders map[string]map[int]holderRow // layer -> node -> row
	stats   map[string]float64
	starts  []startRec
	events  [][]string
	hot     map[string]float64 // layer -> last start that used it
}

type startRec struct {
	t, secs     float64
	node, model int
	src         string // local | peer | registry | mixed
	bytesReg    float64
	bytesPeer   float64
	missEvicted bool
}

type config struct {
	nodes, slots, perNodeBudget, replicaTarget int
	hours, startsPerHour, leaseSec, renewSec   float64
	reconcileSec, failAt, failFrac, partition  float64
	capMB, maxReplicateMB                      float64
	pinRunning, leases                         bool
	scenario                                   string
	seed                                       int64
}

func main() {
	var c config
	flag.IntVar(&c.nodes, "nodes", 16, "model nodes")
	flag.Float64Var(&c.capMB, "nvme-gb", 60, "NVMe cache budget per node (GB)")
	flag.Float64Var(&c.hours, "hours", 6, "simulated hours")
	flag.Float64Var(&c.startsPerHour, "starts-per-hour", 120, "deployment starts per hour across the fleet")
	flag.IntVar(&c.replicaTarget, "replicas", 3, "replica target for hot layers")
	flag.IntVar(&c.perNodeBudget, "per-node-budget", 8, "reconciler copies per node per pass")
	flag.IntVar(&c.slots, "slots", 16, "cluster-wide replication slots (0 = unlimited)")
	flag.Float64Var(&c.leaseSec, "lease-sec", 900, "holder lease")
	flag.Float64Var(&c.renewSec, "renew-sec", 300, "holder renewal interval")
	flag.Float64Var(&c.reconcileSec, "reconcile-sec", 20, "replica reconcile interval")
	flag.Float64Var(&c.maxReplicateMB, "max-replicate-mb", 64, "largest object the reconciler copies (the client's reconcileMaxObjectBytes); larger layers spread only by being read")
	flag.Float64Var(&c.failAt, "fail-at-hour", 3, "hour at which a group of nodes fails (negative: never)")
	flag.Float64Var(&c.failFrac, "fail-frac", 0.25, "fraction of nodes that fail")
	flag.BoolVar(&c.leases, "leases", true, "holder rows expire (false: rows of dead nodes live forever)")
	flag.BoolVar(&c.pinRunning, "pin-running", true, "layers of running deployments are not evicted")
	flag.StringVar(&c.scenario, "scenario", "", "run a named scenario set: all")
	flag.Int64Var(&c.seed, "seed", 1, "random seed")
	out := flag.String("out", "", "directory for timeline CSVs")
	flag.Parse()

	if c.scenario == "all" {
		runScenarios(c, *out)
		return
	}
	cl := newCluster(c)
	cl.run()
	cl.report("custom")
	if *out != "" {
		cl.writeCSV(filepath.Join(*out, "custom.csv"))
	}
}

// runScenarios runs the standard comparison set with one seed.
func runScenarios(base config, out string) {
	type sc struct {
		name string
		mod  func(*config)
	}
	scs := []sc{
		{"baseline", func(c *config) {}},
		{"tight-nvme-30gb", func(c *config) { c.capMB = 30 }},
		{"no-leases", func(c *config) { c.leases = false }},
		{"no-slot-cap", func(c *config) { c.slots = 0 }},
		{"no-pin-running", func(c *config) { c.pinRunning = false }},
		{"burst-4x", func(c *config) { c.startsPerHour *= 4 }},
		{"fail-50pct", func(c *config) { c.failFrac = 0.5 }},
		{"replicate-all", func(c *config) { c.maxReplicateMB = 1 << 30 }},
	}
	fmt.Printf("%-16s %6s %6s %6s %6s %7s %7s %6s %6s %7s %7s %6s\n", "scenario", "starts", "local", "peer", "reg", "p50 s", "p95 s", "evict", "evMiss", "copies", "peakCp", "stale")
	for _, s := range scs {
		c := base
		s.mod(&c)
		cl := newCluster(c)
		cl.run()
		cl.summaryLine(s.name)
		if out != "" {
			_ = os.MkdirAll(out, 0o755)
			cl.writeCSV(filepath.Join(out, s.name+".csv"))
		}
	}
}

func newCluster(c config) *cluster {
	cl := &cluster{cfg: c, rng: rand.New(rand.NewSource(c.seed)), layers: map[string]*layer{},
		holders: map[string]map[int]holderRow{}, stats: map[string]float64{}, hot: map[string]float64{}}
	cl.buildCatalog()
	fams := []struct {
		name string
		frac float64
	}{{"D8ds_v6", 0.5}, {"E8ds_v6", 0.3}, {"L8s_v4", 0.2}}
	for i := 0; i < c.nodes; i++ {
		f := fams[0].name
		x := float64(i) / float64(c.nodes)
		if x >= fams[0].frac {
			f = fams[1].name
		}
		if x >= fams[0].frac+fams[1].frac {
			f = fams[2].name
		}
		capMB := c.capMB * 1024
		if f == "L8s_v4" {
			capMB *= 4 // 1.75 TiB NVMe vs 440 GiB
		}
		cl.nodes = append(cl.nodes, &node{id: i, family: f, capMB: capMB, lru: map[string]float64{}, alive: true, running: map[int]bool{}})
	}
	return cl
}

// buildCatalog makes 10 base images and 20 models. Base images share a few
// OS / runtime layers (as real vLLM / SGLang / TGI images do), so some layers
// are hot across many deployments. Model popularity is Zipf-like.
func (cl *cluster) buildCatalog() {
	shared := []*layer{
		cl.addLayer("os-ubuntu-22.04", 80, "base"),
		cl.addLayer("cuda-12.4-runtime", 2800, "base"),
		cl.addLayer("python-3.11", 120, "base"),
	}
	runtimes := []struct {
		name string
		mb   float64
	}{
		{"vllm-0.11", 4200}, {"vllm-0.10", 4100}, {"sglang-0.5", 3900}, {"tgi-3.0", 3600}, {"llamacpp-b5000", 900},
		{"trtllm-0.18", 6200}, {"vllm-cpu-0.11", 1700}, {"lmdeploy-0.9", 3300}, {"triton-25.01", 5200}, {"ollama-0.6", 1100},
	}
	for i, r := range runtimes {
		ls := []*layer{shared[0], shared[2]}
		if !strings.Contains(r.name, "cpu") && !strings.Contains(r.name, "llamacpp") && !strings.Contains(r.name, "ollama") {
			ls = append(ls, shared[1])
		}
		ls = append(ls, cl.addLayer(fmt.Sprintf("%s-runtime", r.name), r.mb, "base"))
		ls = append(ls, cl.addLayer(fmt.Sprintf("%s-app-%d", r.name, i), 150, "base"))
		cl.images = append(cl.images, image{name: r.name, layers: ls})
	}
	models := []struct {
		name string
		gb   float64
	}{
		{"qwen2.5-0.5b", 1}, {"qwen2.5-1.5b", 3.1}, {"qwen2.5-7b", 15.2}, {"qwen2.5-14b", 29.5}, {"qwen2.5-32b", 65},
		{"llama-3.1-8b", 16}, {"llama-3.2-3b", 6.4}, {"llama-3.3-70b", 141}, {"mistral-7b", 14.5}, {"mixtral-8x7b", 93},
		{"gemma-2-9b", 18.5}, {"gemma-2-27b", 54.6}, {"phi-3.5-mini", 7.6}, {"phi-4", 29.3}, {"deepseek-r1-distill-7b", 15.2},
		{"deepseek-r1-distill-32b", 65.5}, {"granite-3.1-8b", 16.3}, {"smollm2-1.7b", 3.4}, {"olmo-2-7b", 14.6}, {"flint-1", 24},
	}
	for i, m := range models {
		w := cl.addLayer("weights-"+m.name, m.gb*1024, "model")
		// Large models run on GPU runtimes; small ones spread across all.
		base := i % len(cl.images)
		if m.gb > 50 {
			base = []int{0, 2, 5}[i%3]
		}
		cl.models = append(cl.models, model{name: m.name, weights: w, base: base, weight: 1 / math.Pow(float64(i+1), 0.9)})
	}
}

func (cl *cluster) addLayer(id string, mb float64, kind string) *layer {
	l := &layer{id: id, sizeMB: mb, kind: kind}
	cl.layers[id] = l
	return l
}

func (cl *cluster) pickModel() int {
	total := 0.0
	for _, m := range cl.models {
		total += m.weight
	}
	r := cl.rng.Float64() * total
	for i, m := range cl.models {
		if r -= m.weight; r <= 0 {
			return i
		}
	}
	return len(cl.models) - 1
}

// run advances time in 1-second ticks: starts arrive (Poisson), deployments
// end after a random lifetime, renewals and reconcile passes fire on their
// intervals, and a group of nodes fails at failAt.
func (cl *cluster) run() {
	end := cl.cfg.hours * 3600
	lambda := cl.cfg.startsPerHour / 3600
	type run struct {
		node, model int
		until       float64
	}
	var running []run
	failed := false
	for cl.now = 0; cl.now < end; cl.now++ {
		// deployments ending free their pins
		kept := running[:0]
		for _, r := range running {
			if cl.now >= r.until {
				delete(cl.nodes[r.node].running, r.model)
			} else {
				kept = append(kept, r)
			}
		}
		running = kept
		if !failed && cl.cfg.failAt >= 0 && cl.now >= cl.cfg.failAt*3600 {
			cl.failNodes()
			failed = true
		}
		for n := poisson(cl.rng, lambda); n > 0; n-- {
			m := cl.pickModel()
			nd := cl.pickNode(m)
			if nd == nil {
				continue
			}
			rec := cl.start(nd, m)
			cl.starts = append(cl.starts, rec)
			nd.running[m] = true
			running = append(running, run{node: nd.id, model: m, until: cl.now + 600 + cl.rng.Float64()*3000})
		}
		if math.Mod(cl.now, cl.cfg.renewSec) == 0 {
			cl.renew()
		}
		if math.Mod(cl.now, cl.cfg.reconcileSec) == 0 {
			cl.reconcile()
		}
		if math.Mod(cl.now, 300) == 0 {
			cl.snapshot()
		}
	}
}

// pickNode chooses a live node, preferring one that already holds the
// model's weights (cache-affine scheduling), else the emptiest.
func (cl *cluster) pickNode(m int) *node {
	w := cl.models[m].weights.id
	var best *node
	for _, n := range cl.nodes {
		if !n.alive {
			continue
		}
		if _, ok := n.lru[w]; ok && cl.rng.Float64() < 0.7 {
			return n
		}
		if best == nil || n.usedMB/n.capMB < best.usedMB/best.capMB {
			best = n
		}
	}
	return best
}

// start runs one deployment start on n: every layer is read locally, from a
// live holder (peer), or from the registry; misses are filled and admitted,
// evicting LRU layers (not pinned by running deployments) to make room.
func (cl *cluster) start(n *node, m int) startRec {
	md := cl.models[m]
	ls := append(append([]*layer{}, cl.images[md.base].layers...), md.weights)
	rec := startRec{t: cl.now, node: n.id, model: m}
	var secs float64
	srcs := map[string]bool{}
	for _, l := range ls {
		cl.hot[l.id] = cl.now
		if _, ok := n.lru[l.id]; ok {
			n.lru[l.id] = cl.now
			secs += l.sizeMB / diskMBps
			srcs["local"] = true
			continue
		}
		if cl.stats["evicted:"+l.id] > 0 {
			rec.missEvicted = true
		}
		if h := cl.liveHolder(l.id, n.id); h != nil {
			h.readers++
			rate := peerMBps / (1 + contentionStep*float64(h.readers-1))
			secs += l.sizeMB/rate + peerReadSec*l.sizeMB/8
			h.readers--
			rec.bytesPeer += l.sizeMB
			srcs["peer"] = true
		} else {
			// A row whose holder is gone (no leases) costs a failed probe
			// before falling back to the registry.
			if cl.staleHolder(l.id, n.id) {
				cl.stats["stale_reads"]++
				secs += 2
			}
			secs += l.sizeMB / registryMBps
			rec.bytesReg += l.sizeMB
			srcs["registry"] = true
		}
		cl.admit(n, l)
	}
	rec.secs = secs + vllmFixedSec
	switch {
	case len(srcs) == 1 && srcs["local"]:
		rec.src = "local"
	case srcs["registry"]:
		rec.src = "registry"
	default:
		rec.src = "peer"
	}
	return rec
}

func (cl *cluster) liveHolder(id string, except int) *node {
	var best *node
	for nid, row := range cl.holders[id] {
		if nid == except {
			continue
		}
		hn := cl.nodes[nid]
		if !hn.alive || !cl.rowLive(row) {
			continue
		}
		if _, ok := hn.lru[id]; !ok {
			continue
		}
		if best == nil || hn.readers < best.readers {
			best = hn
		}
	}
	return best
}

func (cl *cluster) staleHolder(id string, except int) bool {
	for nid, row := range cl.holders[id] {
		if nid != except && cl.rowLive(row) && !cl.nodes[nid].alive {
			return true
		}
	}
	return false
}

func (cl *cluster) rowLive(r holderRow) bool {
	return !cl.cfg.leases || cl.now <= r.expires
}

// admit lands l on n, evicting coldest unpinned layers first.
func (cl *cluster) admit(n *node, l *layer) {
	for n.usedMB+l.sizeMB > n.capMB {
		victim, oldest := "", math.Inf(1)
		pinned := cl.pinnedOn(n)
		for id, t := range n.lru {
			if cl.cfg.pinRunning && pinned[id] {
				continue
			}
			if t < oldest {
				victim, oldest = id, t
			}
		}
		if victim == "" {
			cl.stats["admit_refused"]++
			return // everything pinned: read through without keeping it
		}
		cl.evict(n, victim)
	}
	n.lru[l.id] = cl.now
	n.usedMB += l.sizeMB
	n.nextGen++
	if cl.holders[l.id] == nil {
		cl.holders[l.id] = map[int]holderRow{}
	}
	cl.holders[l.id][n.id] = holderRow{expires: cl.now + cl.cfg.leaseSec, gen: n.nextGen}
}

func (cl *cluster) evict(n *node, id string) {
	n.usedMB -= cl.layers[id].sizeMB
	delete(n.lru, id)
	delete(cl.holders[id], n.id) // eviction retracts its own row (generation-guarded)
	cl.stats["evictions"]++
	cl.stats["evicted_mb"] += cl.layers[id].sizeMB
	cl.stats["evicted:"+id]++
}

func (cl *cluster) pinnedOn(n *node) map[string]bool {
	p := map[string]bool{}
	for m := range n.running {
		md := cl.models[m]
		for _, l := range cl.images[md.base].layers {
			p[l.id] = true
		}
		p[md.weights.id] = true
	}
	return p
}

// renew: each live node refreshes the lease on every row it still holds.
func (cl *cluster) renew() {
	for _, n := range cl.nodes {
		if !n.alive {
			continue
		}
		for id := range n.lru {
			if row, ok := cl.holders[id][n.id]; ok {
				row.expires = cl.now + cl.cfg.leaseSec
				cl.holders[id][n.id] = row
			}
		}
	}
}

// reconcile: hot layers (used in the last 10 min) below the replica target
// are copied from a live holder to the emptiest live node lacking them,
// within each node's budget and the cluster-wide slot cap.
func (cl *cluster) reconcile() {
	budget := map[int]int{}
	slots := cl.cfg.slots
	copies := 0
	ids := make([]string, 0, len(cl.hot))
	for id, t := range cl.hot {
		if cl.now-t <= 600 {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	for _, id := range ids {
		live := 0
		var src *node
		for nid, row := range cl.holders[id] {
			if cl.rowLive(row) {
				live++ // what the reconciler believes, stale or not
				if hn := cl.nodes[nid]; hn.alive {
					src = hn
				}
			}
		}
		if src == nil || live >= cl.cfg.replicaTarget {
			continue
		}
		if cl.layers[id].sizeMB > cl.cfg.maxReplicateMB {
			cl.stats["reconcile_too_big"]++
			continue
		}
		if budget[src.id] >= cl.cfg.perNodeBudget || (slots > 0 && copies >= slots) {
			cl.stats["reconcile_deferred"]++
			continue
		}
		var dst *node
		for _, n := range cl.nodes {
			if !n.alive {
				continue
			}
			if _, has := n.lru[id]; has {
				continue
			}
			if dst == nil || n.usedMB/n.capMB < dst.usedMB/dst.capMB {
				dst = n
			}
		}
		if dst == nil {
			continue
		}
		cl.admit(dst, cl.layers[id])
		budget[src.id]++
		copies++
		cl.stats["reconcile_copies"]++
		cl.stats["reconcile_mb"] += cl.layers[id].sizeMB
	}
	if float64(copies) > cl.stats["peak_copies_per_pass"] {
		cl.stats["peak_copies_per_pass"] = float64(copies)
	}
}

func (cl *cluster) failNodes() {
	k := int(math.Round(float64(len(cl.nodes)) * cl.cfg.failFrac))
	for i := 0; i < k; i++ {
		cl.nodes[i].alive = false
		cl.nodes[i].running = map[int]bool{}
	}
	cl.stats["failed_nodes"] = float64(k)
}

func (cl *cluster) snapshot() {
	used, cap := 0.0, 0.0
	for _, n := range cl.nodes {
		if n.alive {
			used += n.usedMB
			cap += n.capMB
		}
	}
	staleRows := 0
	for _, hs := range cl.holders {
		for nid, row := range hs {
			if cl.rowLive(row) && !cl.nodes[nid].alive {
				staleRows++
			}
		}
	}
	cl.events = append(cl.events, []string{
		strconv.Itoa(int(cl.now)), fmt.Sprintf("%.3f", used/math.Max(cap, 1)),
		strconv.Itoa(int(cl.stats["evictions"])), strconv.Itoa(int(cl.stats["reconcile_copies"])),
		strconv.Itoa(staleRows), strconv.Itoa(len(cl.starts)),
	})
}

func (cl *cluster) summary() (map[string]int, float64, float64, int) {
	src := map[string]int{}
	var secs []float64
	evMiss := 0
	for _, s := range cl.starts {
		src[s.src]++
		secs = append(secs, s.secs)
		if s.missEvicted {
			evMiss++
		}
	}
	sort.Float64s(secs)
	pct := func(p float64) float64 {
		if len(secs) == 0 {
			return 0
		}
		return secs[int(p*float64(len(secs)-1))]
	}
	return src, pct(0.5), pct(0.95), evMiss
}

func (cl *cluster) summaryLine(name string) {
	src, p50, p95, evMiss := cl.summary()
	fmt.Printf("%-16s %6d %6d %6d %6d %7.0f %7.0f %6d %6d %7d %7d %6d\n", name, len(cl.starts), src["local"], src["peer"], src["registry"],
		p50, p95, int(cl.stats["evictions"]), evMiss, int(cl.stats["reconcile_copies"]), int(cl.stats["peak_copies_per_pass"]), int(cl.stats["stale_reads"]))
}

func (cl *cluster) report(name string) {
	src, p50, p95, evMiss := cl.summary()
	c := cl.cfg
	fmt.Printf("cachesim %s: %d nodes, %.0f GB NVMe/node, %.1f h, %.0f starts/h, 10 base images, 20 models\n", name, c.nodes, c.capMB, c.hours, c.startsPerHour)
	fmt.Printf("  starts            %d (local %d, peer %d, registry %d)\n", len(cl.starts), src["local"], src["peer"], src["registry"])
	fmt.Printf("  start time        p50 %.0f s, p95 %.0f s\n", p50, p95)
	fmt.Printf("  evictions         %d (%.0f GB); starts that missed a layer evicted earlier: %d\n", int(cl.stats["evictions"]), cl.stats["evicted_mb"]/1024, evMiss)
	fmt.Printf("  reconcile         %d copies (%.0f GB), peak %d per pass, %d deferred by budget/slots\n", int(cl.stats["reconcile_copies"]), cl.stats["reconcile_mb"]/1024, int(cl.stats["peak_copies_per_pass"]), int(cl.stats["reconcile_deferred"]))
	fmt.Printf("  failures          %d nodes at hour %.1f; reads sent to dead holders: %d\n", int(cl.stats["failed_nodes"]), c.failAt, int(cl.stats["stale_reads"]))
	fmt.Printf("  admits refused    %d (everything pinned)\n", int(cl.stats["admit_refused"]))
	fmt.Printf("  timeline (5 min)  t   fill  evictions  copies  stale-rows  starts\n")
	for i, e := range cl.events {
		if i%6 == 0 {
			fmt.Printf("                    %4.1fh %s %9s %7s %11s %7s\n", atof(e[0])/3600, e[1], e[2], e[3], e[4], e[5])
		}
	}
}

func (cl *cluster) writeCSV(p string) {
	f, err := os.Create(p)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return
	}
	defer f.Close()
	w := csv.NewWriter(f)
	_ = w.Write([]string{"t_sec", "nvme_fill", "evictions", "reconcile_copies", "stale_rows", "starts"})
	_ = w.WriteAll(cl.events)
}

func atof(s string) float64 { f, _ := strconv.ParseFloat(s, 64); return f }

func poisson(r *rand.Rand, lambda float64) int {
	l, k, p := math.Exp(-lambda), 0, 1.0
	for {
		p *= r.Float64()
		if p <= l {
			return k
		}
		k++
	}
}
