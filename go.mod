module fuse-client

go 1.21

require (
	bazil.org/fuse v0.0.0-20230120002735-62a210ff1fd5
	github.com/Azure/azure-sdk-for-go/sdk/storage/azblob v1.0.0
	github.com/aws/aws-sdk-go v1.44.315
	github.com/gorilla/mux v1.8.0
	google.golang.org/grpc v1.59.0
	google.golang.org/protobuf v1.33.0
)

require (
	github.com/Azure/azure-sdk-for-go/sdk/azcore v1.10.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/internal v1.5.2 // indirect
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd/v22 v22.3.2 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/hanwen/go-fuse/v2 v2.9.0 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	go.etcd.io/etcd/api/v3 v3.5.13 // indirect
	go.etcd.io/etcd/client/pkg/v3 v3.5.13 // indirect
	go.etcd.io/etcd/client/v3 v3.5.13 // indirect
	go.uber.org/atomic v1.7.0 // indirect
	go.uber.org/multierr v1.6.0 // indirect
	go.uber.org/zap v1.17.0 // indirect
	golang.org/x/crypto v0.23.0 // indirect
	golang.org/x/net v0.21.0 // indirect
	golang.org/x/sys v0.28.0 // indirect
	golang.org/x/text v0.15.0 // indirect
	google.golang.org/genproto v0.0.0-20230920204549-e6e6cdab5c13 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20230913181813-007df8e322eb // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20231002182017-d307bd883b97 // indirect
)

replace github.com/hanwen/go-fuse/v2 => ./third_party/go-fuse-local
