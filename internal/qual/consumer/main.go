// Command consumer is the minimal consumer of the library, used to measure
// the runtime import closure, purity and binary footprint (PLAN §12.3). It
// imports only the public package and the caller-supplied SDK client.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/OrdalieTech/state"
)

func main() {
	ctx := context.Background()
	n, err := state.Open(ctx, state.Config{
		S3:       s3.New(s3.Options{Region: os.Getenv("AWS_REGION")}),
		Bucket:   os.Getenv("STATE_BUCKET"),
		Prefix:   os.Getenv("STATE_PREFIX"),
		NodeID:   "consumer",
		CacheDir: os.TempDir(),
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer n.Close(ctx)
	ref, res, err := n.Create(ctx, "counter", state.Request{Schema: 1}, func(tx *state.Tx) ([]byte, error) {
		_, err := tx.Exec(ctx, "CREATE TABLE counter(n INTEGER NOT NULL)")
		return nil, err
	})
	fmt.Println(ref, res, err)
}
