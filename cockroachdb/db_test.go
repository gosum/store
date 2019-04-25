// Copyright 2019 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cockroachdb

import (
	"context"
	"flag"
	"os"
	"testing"

	"golang.org/x/exp/sumdb/internal/tkv/tkvtest"
)

var testInstance = flag.String("cockroachdb", "", "test cockroachdb instance (postgres://pqgotest:password@localhost/pqgotest?sslmode=verify-full)")

func TestSpanner(t *testing.T) {
	// Test basic operations
	// (exercising interface wrapper, not spanner itself).

	if *testInstance == "" {
		t.Skip("no test instance given in -cockroachdb flag")
	}

	os.Setenv("CONNSTR", *testInstance)
	ctx := context.Background()
	DeleteTestStorage(ctx, "test_cockroachdb")
	s, err := CreateStorage(ctx, "test_cockroachdb")
	if err != nil {
		t.Fatal(err)
	}
	defer DeleteTestStorage(ctx, "test_cockroachdb")

	tkvtest.TestStorage(t, ctx, s)
}
