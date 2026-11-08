// Package storetest holds the storage test fixtures (PLAN §15.1): an
// in-memory linearizable store with scheduled faults (Mem), a fault proxy
// that preserves signed requests to real S3 endpoints (Proxy), and helpers
// that locate a pinned local S3 server or a remote test endpoint. It is imported
// only by tests and qualification code, never by the library.
package storetest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Endpoint is one S3 endpoint and a disposable prefix in one of its buckets.
type Endpoint struct {
	URL, Region, Bucket, Prefix string
	AccessKey, SecretKey        string
}

// Client returns an SDK client for e using httpClient (nil: a fresh
// transport, so two calls never share connections).
func (e Endpoint) Client(httpClient *http.Client) *s3.Client {
	if httpClient == nil {
		httpClient = &http.Client{Transport: http.DefaultTransport.(*http.Transport).Clone()}
	}
	creds := aws.Credentials{AccessKeyID: e.AccessKey, SecretAccessKey: e.SecretKey}
	return s3.New(s3.Options{
		Region:       e.Region,
		BaseEndpoint: aws.String(e.URL),
		UsePathStyle: true,
		HTTPClient:   httpClient,
		Credentials:  aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) { return creds, nil }),
	})
}

// Sub returns e with a fresh random sub-prefix appended.
func (e Endpoint) Sub(name string) Endpoint {
	e.Prefix += "/" + name + "-" + RandomHex(8)
	return e
}

// RandomHex returns n random bytes in hex.
func RandomHex(n int) string {
	b := make([]byte, n)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// ErrBlockedRemote is returned when remote qualification is required and its
// configuration is absent. Commands print it and exit nonzero.
var ErrBlockedRemote = errors.New("blocked_missing_remote_configuration")

// RemotePrefixRoot is the only prefix remote qualification may write under.
const RemotePrefixRoot = "qual-state/"

// RemoteFromEnv reads QUAL_S3_ENDPOINT, QUAL_S3_REGION, QUAL_S3_BUCKET,
// QUAL_S3_PREFIX, AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY. Any missing
// value is ErrBlockedRemote; a prefix outside qual-state/ is refused.
func RemoteFromEnv() (Endpoint, error) {
	e := Endpoint{URL: os.Getenv("QUAL_S3_ENDPOINT"), Region: os.Getenv("QUAL_S3_REGION"),
		Bucket: os.Getenv("QUAL_S3_BUCKET"), Prefix: strings.TrimSuffix(os.Getenv("QUAL_S3_PREFIX"), "/"),
		AccessKey: os.Getenv("AWS_ACCESS_KEY_ID"), SecretKey: os.Getenv("AWS_SECRET_ACCESS_KEY")}
	for _, v := range []string{e.URL, e.Region, e.Bucket, e.Prefix, e.AccessKey, e.SecretKey} {
		if v == "" {
			return e, ErrBlockedRemote
		}
	}
	if !strings.HasPrefix(e.Prefix+"/", RemotePrefixRoot) || strings.Contains(e.Prefix, "..") {
		return e, fmt.Errorf("QUAL_S3_PREFIX %q is not under %s", e.Prefix, RemotePrefixRoot)
	}
	return e, nil
}

// Local S3 fixtures: pinned servers under rootless podman (PLAN §15.1),
// each with its image digest in research.lock.json. RustFS is the default;
// MinIO stays selectable because it alone reproduces a GET that blocks ~30 s
// behind a stalled conditional PUT (docs/PERFORMANCE.md).
const (
	LocalEnv       = "GARDEN_TEST_S3"        // set (to anything) to run the local S3 tests
	legacyLocalEnv = "GARDEN_TEST_MINIO"     // older name of LocalEnv, still honored
	ServerEnv      = "GARDEN_TEST_S3_SERVER" // "rustfs" (default) or "minio"
	localBucket    = "state-test"
	localTmpfs     = "/data:rw,size=1g,mode=1777"
)

type fixture struct {
	name, addr, lockKey, user, password string
	run                                 func(image string) []string // podman run arguments after the image's ports
}

var fixtures = map[string]fixture{
	"rustfs": {"state-rustfs", "127.0.0.1:9100", "rustfs_fixture", "staterustfs", "staterustfs-secret",
		func(image string) []string {
			return []string{"-p", "127.0.0.1:9100:9000", "-e", "RUSTFS_ACCESS_KEY=staterustfs", "-e", "RUSTFS_SECRET_KEY=staterustfs-secret",
				"-e", "RUSTFS_CONSOLE_ENABLE=false", "--tmpfs", localTmpfs, image, "/data"}
		}},
	"minio": {"state-minio", "127.0.0.1:9000", "minio_fixture", "stateminio", "stateminio-secret",
		func(image string) []string {
			return []string{"-p", "127.0.0.1:9000:9000", "-p", "127.0.0.1:9001:9001",
				"-e", "MINIO_ROOT_USER=stateminio", "-e", "MINIO_ROOT_PASSWORD=stateminio-secret",
				"--tmpfs", localTmpfs, image, "server", "/data", "--console-address", ":9001"}
		}},
}

// RemoteEnv, set (to anything) together with GARDEN_TEST_S3, runs the local
// S3 tests against the remote endpoint of RemoteFromEnv instead.
const RemoteEnv = "GARDEN_TEST_REMOTE"

// LocalOn reports whether GARDEN_TEST_S3 (or GARDEN_TEST_MINIO) is set.
func LocalOn() bool { return os.Getenv(LocalEnv) != "" || os.Getenv(legacyLocalEnv) != "" }

// Local returns a fresh prefix on the selected local S3 fixture, starting
// the container if needed, or with GARDEN_TEST_REMOTE on the remote endpoint,
// where missing configuration fails the test with
// blocked_missing_remote_configuration. It skips t only when GARDEN_TEST_S3
// is unset; a fixture that cannot start fails the test.
func Local(t testing.TB) Endpoint {
	t.Helper()
	if !LocalOn() {
		t.Skip(LocalEnv + " is not set")
	}
	start := StartLocal
	if os.Getenv(RemoteEnv) != "" {
		start = func(context.Context) (Endpoint, error) { return RemoteFromEnv() }
	}
	e, err := start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return e.Sub(strings.ReplaceAll(t.Name(), "/", "_"))
}

// StartLocal makes sure the fixture named by GARDEN_TEST_S3_SERVER (image
// digest from research.lock.json) serves on its 127.0.0.1 port and has the
// test bucket. It reuses a running container of that name and refuses one
// with another image.
func StartLocal(ctx context.Context) (Endpoint, error) {
	server := os.Getenv(ServerEnv)
	if server == "" {
		server = "rustfs"
	}
	f, ok := fixtures[server]
	if !ok {
		return Endpoint{}, fmt.Errorf("%s=%q: want rustfs or minio", ServerEnv, server)
	}
	e := Endpoint{URL: "http://" + f.addr, Region: "us-east-1", Bucket: localBucket, Prefix: "t",
		AccessKey: f.user, SecretKey: f.password}
	image, err := lockedImage(f.lockKey)
	if err != nil {
		return e, err
	}
	out, err := exec.CommandContext(ctx, "podman", "inspect", "--format", "{{.State.Running}} {{.ImageName}}", f.name).Output()
	switch {
	case err != nil: // absent: start it, detached from this process group
		cmd := exec.CommandContext(ctx, "setsid", append([]string{"podman", "run", "-d", "--name", f.name}, f.run(image)...)...)
		if b, err := cmd.CombinedOutput(); err != nil {
			return e, fmt.Errorf("podman run %s: %v: %s", f.name, err, b)
		}
	case strings.TrimSpace(string(out)) != "true "+image:
		return e, fmt.Errorf("container %s is %q, want running %s", f.name, strings.TrimSpace(string(out)), image)
	}
	c := e.Client(nil)
	for i := 0; ; i++ {
		_, err = c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: &e.Bucket})
		if err == nil || strings.Contains(err.Error(), "BucketAlreadyOwnedByYou") {
			return e, nil
		}
		if i == 50 {
			return e, fmt.Errorf("%s not ready: %w", server, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// lockedImage reads image@digest of research.lock.json's key at the module root.
func lockedImage(key string) (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for ; ; dir = filepath.Dir(dir) {
		b, err := os.ReadFile(filepath.Join(dir, "research.lock.json"))
		if err == nil {
			var lock map[string]struct{ Image, Digest string }
			_ = json.Unmarshal(b, &lock) // other entries have other shapes
			if f := lock[key]; f.Image != "" && strings.HasPrefix(f.Digest, "sha256:") {
				return f.Image + "@" + f.Digest, nil
			}
			return "", fmt.Errorf("research.lock.json: no %s image digest", key)
		}
		if filepath.Dir(dir) == dir {
			return "", errors.New("research.lock.json not found above the working directory")
		}
	}
}
