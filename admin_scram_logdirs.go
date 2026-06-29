package gokafka

import (
	"context"
	"crypto/rand"

	"github.com/sinamohsenifar/gokafka/internal/auth"
	"github.com/sinamohsenifar/gokafka/internal/protocol"
)

// ScramMechanism identifies a SCRAM credential mechanism (KIP-554).
type ScramMechanism int8

const (
	ScramSHA256 ScramMechanism = ScramMechanism(protocol.ScramMechanismSHA256)
	ScramSHA512 ScramMechanism = ScramMechanism(protocol.ScramMechanismSHA512)
)

func (m ScramMechanism) saslName() string {
	if m == ScramSHA512 {
		return "SCRAM-SHA-512"
	}
	return "SCRAM-SHA-256"
}

// UpsertUserScramCredential creates or updates a user's SCRAM credential. The
// salt is generated locally and the salted password derived with PBKDF2; the
// plaintext password never leaves the client. iterations <= 0 defaults to 4096.
func (a *Admin) UpsertUserScramCredential(ctx context.Context, user string, mechanism ScramMechanism, password string, iterations int) error {
	if iterations <= 0 {
		iterations = 4096
	}
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return err
	}
	salted, err := auth.SaltedPassword(mechanism.saslName(), password, salt, iterations)
	if err != nil {
		return err
	}
	up := protocol.ScramUpsertion{
		Name: user, Mechanism: int8(mechanism), Iterations: int32(iterations),
		Salt: salt, SaltedPassword: salted,
	}
	return a.alterUserScram(ctx, nil, []protocol.ScramUpsertion{up})
}

// DeleteUserScramCredential removes a user's SCRAM credential for one mechanism.
func (a *Admin) DeleteUserScramCredential(ctx context.Context, user string, mechanism ScramMechanism) error {
	del := protocol.ScramDeletion{Name: user, Mechanism: int8(mechanism)}
	return a.alterUserScram(ctx, []protocol.ScramDeletion{del}, nil)
}

// UserScramCredential is one stored SCRAM credential (mechanism + iteration count).
type UserScramCredential struct {
	Mechanism  ScramMechanism
	Iterations int
}

// UserScramCredentials describes the SCRAM credentials registered for a user.
type UserScramCredentials struct {
	User        string
	Credentials []UserScramCredential
}

// DescribeUserScramCredentials returns the SCRAM credentials registered for the
// given users (KIP-554, API 50). With no users it describes all users. The
// salted passwords are never returned by the broker — only the mechanism and
// iteration count per credential.
func (a *Admin) DescribeUserScramCredentials(ctx context.Context, users ...string) ([]UserScramCredentials, error) {
	body := protocol.EncodeDescribeUserScramCredentialsRequest(users)
	resp, err := a.requestAny(ctx, protocol.APIDescribeUserScramCreds, protocol.VerDescribeUserScramCreds, body)
	if err != nil {
		return nil, err
	}
	topErr, topMsg, results, err := protocol.DecodeDescribeUserScramCredentialsResponse(resp)
	if err != nil {
		return nil, err
	}
	if topErr != 0 {
		return nil, newKafkaError(topErr, "", 0, topMsg)
	}
	out := make([]UserScramCredentials, 0, len(results))
	for _, r := range results {
		if r.ErrorCode != 0 {
			// RESOURCE_NOT_FOUND for a specific user — skip rather than fail the batch.
			continue
		}
		creds := make([]UserScramCredential, 0, len(r.Credentials))
		for _, c := range r.Credentials {
			creds = append(creds, UserScramCredential{Mechanism: ScramMechanism(c.Mechanism), Iterations: int(c.Iterations)})
		}
		out = append(out, UserScramCredentials{User: r.User, Credentials: creds})
	}
	return out, nil
}

func (a *Admin) alterUserScram(ctx context.Context, dels []protocol.ScramDeletion, ups []protocol.ScramUpsertion) error {
	body := protocol.EncodeAlterUserScramCredentialsRequest(dels, ups)
	resp, err := a.requestAny(ctx, protocol.APIAlterUserScramCreds, protocol.VerAlterUserScramCreds, body)
	if err != nil {
		return err
	}
	results, err := protocol.DecodeAlterUserScramCredentialsResponse(resp)
	if err != nil {
		return err
	}
	for _, r := range results {
		if r.ErrorCode != 0 {
			return newKafkaError(r.ErrorCode, "", 0, r.ErrorMessage)
		}
	}
	return nil
}

// LogDirPartition is per-partition storage info from DescribeLogDirs.
type LogDirPartition struct {
	Topic     string
	Partition int32
	Size      int64
	OffsetLag int64
	IsFuture  bool
}

// LogDir reports one broker log directory's storage usage.
type LogDir struct {
	BrokerID    int32
	Path        string
	TotalBytes  int64 // -1 if the broker does not report it
	UsableBytes int64 // -1 if the broker does not report it
	Err         error // non-nil if this log dir is offline/errored
	Partitions  []LogDirPartition
}

// DescribeLogDirs reports storage usage per broker log directory. Pass nil
// topics to describe all log dirs, or a topic->partitions map to scope it.
// brokerIDs limits which brokers are queried (empty = all brokers in metadata).
func (a *Admin) DescribeLogDirs(ctx context.Context, topics map[string][]int32, brokerIDs ...int32) ([]LogDir, error) {
	meta := a.client.cluster.Metadata()
	targets := brokerIDs
	if len(targets) == 0 {
		for _, b := range meta.Brokers {
			targets = append(targets, b.NodeID)
		}
	}
	ver := a.client.cluster.NegotiatedVersion(protocol.APIDescribeLogDirs, protocol.VerDescribeLogDirs)
	if ver <= 0 {
		ver = protocol.VerDescribeLogDirs
	}
	body := protocol.EncodeDescribeLogDirsRequest(ver, topics)

	var out []LogDir
	for _, node := range targets {
		resp, err := a.client.cluster.Request(ctx, node, protocol.APIDescribeLogDirs, ver, body)
		if err != nil {
			return nil, err
		}
		topErr, results, err := protocol.DecodeDescribeLogDirsResponse(ver, resp)
		if err != nil {
			return nil, err
		}
		if topErr != 0 {
			return nil, newKafkaError(topErr, "", 0, "describe log dirs failed")
		}
		for _, r := range results {
			ld := LogDir{
				BrokerID: node, Path: r.LogDir,
				TotalBytes: r.TotalBytes, UsableBytes: r.UsableBytes,
			}
			if r.ErrorCode != 0 {
				ld.Err = newKafkaError(r.ErrorCode, "", 0, "log dir error")
			}
			for _, p := range r.Partitions {
				ld.Partitions = append(ld.Partitions, LogDirPartition{
					Topic: p.Topic, Partition: p.Partition,
					Size: p.Size, OffsetLag: p.OffsetLag, IsFuture: p.IsFuture,
				})
			}
			out = append(out, ld)
		}
	}
	return out, nil
}
