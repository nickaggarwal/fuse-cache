// Command scoreprobe drives one request through a running EPP's ext_proc
// endpoint and prints the per-endpoint scheduler scores.
//
// This exists because EPP is not an HTTP service you can curl. It is an Envoy
// external processor: a bidirectional gRPC stream that Envoy calls on the
// request path, and the routing decision comes back as Envoy dynamic metadata
// rather than a response body. Standing up a full Envoy/Gateway just to read
// that decision would add a large moving part between the plugin under test and
// the observation — this speaks the same protocol directly.
//
// It reads x-gateway-destination-endpoint-scores (EPP must run with
// --emit-endpoint-scores), which carries a score for EVERY candidate the
// scheduler considered, including the ones the picker rejected. That is the
// distinction that matters here: seeing only the winner would leave us
// inferring the scorer's contribution from which pod got traffic, whereas the
// score map shows the warm node and the cold node side by side.
package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"time"

	envoyCorev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extProcPb "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	// Mirrors llm-d's pkg/epp/metadata/consts.go. Duplicated rather than
	// imported so this tool stays runnable against an EPP whose internal
	// package layout has moved; the wire keys are the stable contract.
	destinationEndpointNamespace = "envoy.lb"
	destinationEndpointKey       = "x-gateway-destination-endpoint"
	destinationEndpointScoresKey = "x-gateway-destination-endpoint-scores"

	// Names the inference objective for the request. EPP also parses the model
	// out of the JSON body; sending both matches what its own integration
	// tests do.
	objectiveKey = "x-llm-d-inference-objective"
)

func main() {
	addr := flag.String("addr", "localhost:9002", "EPP ext_proc gRPC address")
	model := flag.String("model", "Qwen/Qwen2.5-1.5B-Instruct", "model name to request")
	timeout := flag.Duration("timeout", 15*time.Second, "overall deadline")
	asJSON := flag.Bool("json", false, "emit JSON instead of a table")
	// EPP defaults to --secure-serving with a self-signed cert it mints at
	// startup, so there is no CA to verify against. Envoy is configured the
	// same way in llm-d's own deployments.
	plaintext := flag.Bool("plaintext", false, "connect without TLS (EPP started with --secure-serving=false)")
	flag.Parse()

	scores, chosen, err := probe(*addr, *model, *timeout, *plaintext)
	if err != nil {
		fmt.Fprintf(os.Stderr, "probe failed: %v\n", err)
		os.Exit(1)
	}

	if *asJSON {
		out, _ := json.Marshal(map[string]any{"chosen": chosen, "scores": scores})
		fmt.Println(string(out))
		return
	}

	endpoints := make([]string, 0, len(scores))
	for ep := range scores {
		endpoints = append(endpoints, ep)
	}
	// Highest score first: the routing decision reads top-down.
	sort.Slice(endpoints, func(i, j int) bool { return scores[endpoints[i]] > scores[endpoints[j]] })

	fmt.Printf("chosen: %s\n", chosen)
	fmt.Printf("%-28s %8s\n", "ENDPOINT", "SCORE")
	for _, ep := range endpoints {
		marker := ""
		if ep == chosen {
			marker = "  <- picked"
		}
		fmt.Printf("%-28s %8.4f%s\n", ep, scores[ep], marker)
	}
}

func probe(addr, model string, timeout time.Duration, plaintext bool) (map[string]float64, string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	creds := credentials.NewTLS(&tls.Config{InsecureSkipVerify: true}) //nolint:gosec // self-signed by design; see -plaintext
	if plaintext {
		creds = insecure.NewCredentials()
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, "", fmt.Errorf("dial %s: %w", addr, err)
	}
	defer conn.Close()

	stream, err := extProcPb.NewExternalProcessorClient(conn).Process(ctx)
	if err != nil {
		return nil, "", fmt.Errorf("open ext_proc stream: %w", err)
	}

	// Envoy sends headers, then the body. EPP buffers for the body before it
	// schedules — the model name it routes on is parsed out of the OpenAI JSON
	// payload, not the headers — so both phases have to go over the wire or the
	// stream just sits there waiting.
	body := fmt.Sprintf(`{"model":%q,"prompt":"hello","max_tokens":1}`, model)
	msgs := []*extProcPb.ProcessingRequest{
		{Request: &extProcPb.ProcessingRequest_RequestHeaders{
			RequestHeaders: &extProcPb.HttpHeaders{
				Headers: &envoyCorev3.HeaderMap{Headers: []*envoyCorev3.HeaderValue{
					{Key: "x-request-id", Value: "scoreprobe"},
					{Key: ":path", Value: "/v1/completions"},
					{Key: ":method", Value: "POST"},
					{Key: "content-type", Value: "application/json"},
					{Key: objectiveKey, Value: model},
				}},
			},
		}},
		{Request: &extProcPb.ProcessingRequest_RequestBody{
			RequestBody: &extProcPb.HttpBody{Body: []byte(body), EndOfStream: true},
		}},
	}

	// Send BOTH phases before reading anything. EPP holds the headers phase
	// open until the body arrives, so a Send/Recv ping-pong deadlocks on the
	// first Recv — the client waits for a response that is waiting for the
	// client.
	for _, m := range msgs {
		if err := stream.Send(m); err != nil {
			return nil, "", fmt.Errorf("send: %w", err)
		}
	}

	// The scheduling decision rides on whichever response carries it; scan all
	// of them rather than assuming an index, since a header-phase response with
	// no metadata is legitimate.
	var md *structpb.Struct
	for range msgs {
		resp, err := stream.Recv()
		if err != nil {
			return nil, "", fmt.Errorf("recv: %w", err)
		}
		if got := resp.GetDynamicMetadata(); got != nil {
			md = got
		}
	}

	if md == nil {
		return nil, "", fmt.Errorf("no dynamic metadata in any response — is EPP running with --emit-endpoint-scores?")
	}
	lb, ok := md.Fields[destinationEndpointNamespace]
	if !ok {
		return nil, "", fmt.Errorf("no %q namespace in dynamic metadata", destinationEndpointNamespace)
	}
	fields := lb.GetStructValue().GetFields()

	chosen := fields[destinationEndpointKey].GetStringValue()

	scoresVal, ok := fields[destinationEndpointScoresKey]
	if !ok {
		return nil, chosen, fmt.Errorf("no %q — EPP must run with --emit-endpoint-scores", destinationEndpointScoresKey)
	}
	scores := map[string]float64{}
	for ep, v := range scoresVal.GetStructValue().GetFields() {
		scores[ep] = v.GetNumberValue()
	}
	return scores, chosen, nil
}
