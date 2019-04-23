// Copyright 2019 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
// Package cockroachdb implements a database.DB using cockroachdb.
package cockroachdb

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/lib/pq"
	"golang.org/x/exp/notary/internal/database"
)

// A DB is a connection to a cockroach database.
type DB struct {
	name   string
	client *sql.DB
}

// OpenDB opens the cockroachdb with the given name and connect string.
// (for example, "postgres://pqgotest:password@localhost/pqgotest?sslmode=verify-full").
// The database must already exist.
func OpenDB(ctx context.Context, name string) (*DB, error) {
	connStr := os.Getenv("CONNSTR")
	client, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}
	db := &DB{name: name, client: client}
	return db, nil
}

// CreateDB creates a cockroachdb with the given name
// (for example, "projects/my-project/instances/my-instance/databases/my_db").
// The database must not already exist.
func CreateDB(ctx context.Context, name string) (*DB, error) {
	db, err := OpenDB(ctx, name)
	if err != nil {
		return nil, err
	}
	_, err = db.Exec("CREATE DATABASE $1", name)
	if err != nil {
		return nil, err
	}
	return db, nil
}

// DeleteTestDB deletes the cockroachdb with the given name.
// To avoid unfortunate accidents, DeleteTestDB returns an error
// if the database name does not begin with "test_".
func DeleteTestDB(ctx context.Context, name string) error {
	if !strings.HasPrefix(name, "test_") {
		return fmt.Errorf("can only delete test dbs")
	}

	db, err := OpenDB(ctx, name)
	if err != nil {
		return err
	}
	defer db.Close()

	_, err = db.Exec("DROP DATABASE $1", name)
	return err
}

// CreateTables creates the described tables.
func (db *DB) CreateTables(ctx context.Context, tables []*database.Table) error {
	if db.client == nil {
		client, err := OpenDB("postgres", db.name)
		if err != nil {
			return err
		}
		db.client = client
	}
	var stmts []string
	for _, table := range tables {
		var buf bytes.Buffer
		fmt.Fprintf(&buf, "CREATE TABLE `%s` (\n", table.Name)
		for _, col := range table.Columns {
			fmt.Fprintf(&buf, "\t`%s` %s", col.Name, strings.TrimSuffix(strings.ToUpper(col.Type), "64"))
			switch col.Type {
			case "string", "bytes":
				if col.Size > 0 {
					fmt.Fprintf(&buf, "(%d)", col.Size)
				}
			}
			for _, key := range table.PrimaryKey {
				if key == col.Name {
					fmt.Fprintf(&buf, "`%s`", " PRIMARY KEY")
				}
			}
			fmt.Fprintf(&buf, ",\n")
		}
		fmt.Fprintf(&buf, ");\n")
		stmts = append(stmts, buf.String())
	}

	_, err = db.Exec(stmts)
	return err
}

// ReadOnly executes f in a read-only transaction.
func (db *DB) ReadOnly(ctx context.Context, f func(context.Context, database.Transaction) error) error {
	tx, err := db.client.BeginTx()
	if err != nil {
		return err
	}
	return f(ctx, &cockroachdbTx{tx})
}

// ReadWrite executes f in a read-write transaction.
func (db *DB) ReadWrite(ctx context.Context, f func(context.Context, database.Transaction) error) error {
	tx, err := db.client.BeginTx()
	if err != nil {
		return err
	}
	return f(ctx, &cockroachdbTx{tx})
}

// A cockroachdbTx is the underlying cockroachdb transaction.
type cockroachdbTx struct {
	tx *sql.Tx
}

// Read reads rows matching keys from the database.
func (tx *cockroachdbTx) Read(ctx context.Context, table string, keys database.Keys, columns []string) database.Rows {

}

// ReadRow reads a single row matching key from the database.
func (tx *cockroachdbTx) ReadRow(ctx context.Context, table string, key database.Key, columns []string) (database.Row, error) {

}

// BufferWrite buffers the given writes.
func (tx *cockroachdbTx) BufferWrite(writes []database.Mutation) error {

}
