// Package storetest holds the storage test fixtures (PLAN §15.1): an
// in-memory linearizable store with scheduled faults (Mem), a fault proxy
// that preserves signed requests to real S3 endpoints (Proxy), and helpers
// that locate a pinned local MinIO or a remote test endpoint. It is imported
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

// MinIO fixture: container name, address and test-only credentials.
const (
	MinIOEnv       = "GARDEN_TEST_MINIO" // set (to anything) to run MinIO tests
	minioName      = "state-minio"
	minioAddr      = "127.0.0.1:9000"
	minioUser      = "stateminio"
	minioPassword  = "stateminio-secret"
	minioBucket    = "state-test"
	minioTmpfsSize = "1g"
)

// RemoteEnv, set (to anything) together with GARDEN_TEST_MINIO, runs the
// MinIO tests against the remote endpoint of RemoteFromEnv instead.
const RemoteEnv = "GARDEN_TEST_REMOTE"

// MinIO returns a fresh prefix on the local MinIO fixture, starting the
// container if needed, or with GARDEN_TEST_REMOTE on the remote endpoint,
// where missing configuration fails the test with
// blocked_missing_remote_configuration. It skips t only when
// GARDEN_TEST_MINIO is unset; a fixture that cannot start fails the test.
func MinIO(t testing.TB) Endpoint {
	t.Helper()
	if os.Getenv(MinIOEnv) == "" {
		t.Skip(MinIOEnv + " is not set")
	}
	start := StartMinIO
	if os.Getenv(RemoteEnv) != "" {
		start = func(context.Context) (Endpoint, error) { return RemoteFromEnv() }
	}
	e, err := start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return e.Sub(strings.ReplaceAll(t.Name(), "/", "_"))
}

// StartMinIO makes sure the pinned MinIO container (image digest from
// research.lock.json) serves on 127.0.0.1:9000 and has the test bucket. It
// reuses a running state-minio container and refuses one with another image.
func StartMinIO(ctx context.Context) (Endpoint, error) {
	e := Endpoint{URL: "http://" + minioAddr, Region: "us-east-1", Bucket: minioBucket, Prefix: "t",
		AccessKey: minioUser, SecretKey: minioPassword}
	image, err := lockedMinIOImage()
	if err != nil {
		return e, err
	}
	out, err := exec.CommandContext(ctx, "podman", "inspect", "--format", "{{.State.Running}} {{.ImageName}}", minioName).Output()
	switch {
	case err != nil: // absent: start it, detached from this process group
		cmd := exec.CommandContext(ctx, "setsid", "podman", "run", "-d", "--name", minioName,
			"-p", minioAddr+":9000", "-p", "127.0.0.1:9001:9001",
			"-e", "MINIO_ROOT_USER="+minioUser, "-e", "MINIO_ROOT_PASSWORD="+minioPassword,
			"--tmpfs", "/data:rw,size="+minioTmpfsSize+",mode=1777", image, "server", "/data", "--console-address", ":9001")
		if b, err := cmd.CombinedOutput(); err != nil {
			return e, fmt.Errorf("podman run %s: %v: %s", minioName, err, b)
		}
	case strings.TrimSpace(string(out)) != "true "+image:
		return e, fmt.Errorf("container %s is %q, want running %s", minioName, strings.TrimSpace(string(out)), image)
	}
	c := e.Client(nil)
	for i := 0; ; i++ {
		_, err = c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: &e.Bucket})
		if err == nil || strings.Contains(err.Error(), "BucketAlreadyOwnedByYou") {
			return e, nil
		}
		if i == 50 {
			return e, fmt.Errorf("minio not ready: %w", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// lockedMinIOImage reads image@digest from research.lock.json at the module root.
func lockedMinIOImage() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for ; ; dir = filepath.Dir(dir) {
		b, err := os.ReadFile(filepath.Join(dir, "research.lock.json"))
		if err == nil {
			var lock struct {
				Minio struct{ Image, Digest string } `json:"minio_fixture"`
			}
			if err := json.Unmarshal(b, &lock); err != nil || lock.Minio.Image == "" || !strings.HasPrefix(lock.Minio.Digest, "sha256:") {
				return "", fmt.Errorf("research.lock.json: no minio_fixture image digest (%v)", err)
			}
			return lock.Minio.Image + "@" + lock.Minio.Digest, nil
		}
		if filepath.Dir(dir) == dir {
			return "", errors.New("research.lock.json not found above the working directory")
		}
	}
}
