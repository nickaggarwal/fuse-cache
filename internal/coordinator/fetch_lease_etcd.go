package coordinator

import (
	"context"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Compile-time check that EtcdStore supports fetch leases.
var _ FetchLeaseStore = (*EtcdStore)(nil)

func (s *EtcdStore) inflightKey(key string) string { return s.prefix + "/inflight/" + key }

// AcquireFetchLease takes /fuse/inflight/<key> with an etcd TTL lease using a
// create-if-absent transaction. Losing the race returns the current holder so
// the requester can wait briefly and read from that peer instead of origin.
// Expiry is handled by etcd lease expiry — a crashed holder's key vanishes on
// its own within the TTL.
func (s *EtcdStore) AcquireFetchLease(ctx context.Context, key, peerID string, ttl time.Duration) (string, bool, error) {
	ttlSec := int64(ttl / time.Second)
	if ttlSec < 1 {
		ttlSec = 1
	}
	etcdKey := s.inflightKey(key)

	grant, err := s.client.Grant(ctx, ttlSec)
	if err != nil {
		return "", false, err
	}

	txn, err := s.client.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(etcdKey), "=", 0)).
		Then(clientv3.OpPut(etcdKey, peerID, clientv3.WithLease(grant.ID))).
		Else(clientv3.OpGet(etcdKey)).
		Commit()
	if err != nil {
		_, _ = s.client.Revoke(context.Background(), grant.ID)
		return "", false, err
	}
	if txn.Succeeded {
		return peerID, true, nil
	}

	// Lost the race: revoke our unused lease grant and report the holder.
	_, _ = s.client.Revoke(context.Background(), grant.ID)
	holder := ""
	if len(txn.Responses) > 0 {
		if rr := txn.Responses[0].GetResponseRange(); rr != nil && len(rr.Kvs) > 0 {
			holder = string(rr.Kvs[0].Value)
		}
	}
	if holder == peerID {
		// We already hold it (e.g. retry after a network blip). Treat as granted;
		// the original TTL keeps ticking, which is fine for an advisory lease.
		return peerID, true, nil
	}
	return holder, false, nil
}

// ReleaseFetchLease deletes the in-flight key if this peer still holds it,
// using a value-compare transaction so a later holder is never clobbered.
// The key's etcd lease is revoked too — deleting only the key would leave
// the lease object alive until TTL, churning etcd's lease heap on busy
// clusters (acquire already revokes on the losing branch).
func (s *EtcdStore) ReleaseFetchLease(ctx context.Context, key, peerID string) error {
	etcdKey := s.inflightKey(key)
	txn, err := s.client.Txn(ctx).
		If(clientv3.Compare(clientv3.Value(etcdKey), "=", peerID)).
		Then(clientv3.OpGet(etcdKey), clientv3.OpDelete(etcdKey)).
		Commit()
	if err != nil || !txn.Succeeded {
		return err
	}
	if rr := txn.Responses[0].GetResponseRange(); rr != nil && len(rr.Kvs) > 0 {
		if leaseID := rr.Kvs[0].Lease; leaseID != 0 {
			revokeCtx, cancel := context.WithTimeout(context.Background(), defaultEtcdRequestTimeout)
			defer cancel()
			_, _ = s.client.Revoke(revokeCtx, clientv3.LeaseID(leaseID))
		}
	}
	return nil
}
