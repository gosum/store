// Copyright 2019 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
// Package cockroachdb implements a database.DB using cockroachdb.
package cockroachdb

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"

	"github.com/gosum/common/tkv"
	_ "github.com/lib/pq"
)

const tableName = "tkv"

// A DB is a connection to a cockroach database.
type DB struct {
	name   string
	client *sql.DB
}

// OpenStorage opens the cockroachdb with the given name and connect string.
// (for example, "postgres://pqgotest:password@localhost/pqgotest?sslmode=verify-full").
// The database must already exist.
func OpenStorage(ctx context.Context, name string) (*DB, error) {
	connStr := os.Getenv("CONNSTR")
	client, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}
	db := &DB{name: name, client: client}
	return db, nil
}

// CreateStorage creates a cockroachdb with the given name
// (for example, "postgres://pqgotest:password@localhost/pqgotest?sslmode=verify-full").
// The database must not already exist.
func CreateStorage(ctx context.Context, name string) (*DB, error) {
	db, err := OpenStorage(ctx, name)
	if err != nil {
		return nil, err
	}
	_, err = db.client.Exec("CREATE DATABASE " + name)
	if err != nil {
		return nil, err
	}

	sm := "CREATE TABLE " + tableName + " (key STRING PRIMARY KEY,value STRING);"
	_, err = db.client.Exec(sm)
	if err != nil {
		return nil, err
	}
	return db, nil
}

// DeleteTestStorage deletes the cockroachdb with the given name.
// To avoid unfortunate accidents, DeleteTestStorage returns an error
// if the database name does not begin with "test_".
func DeleteTestStorage(ctx context.Context, name string) error {
	if !strings.HasPrefix(name, "test_") {
		return fmt.Errorf("can only delete test dbs")
	}

	db, err := OpenStorage(ctx, name)
	if err != nil {
		return err
	}
	defer db.client.Close()

	_, err = db.client.Exec("DROP DATABASE " + name)
	return err
}

// Close closes store
func (db *DB) Close() error {
	if db == nil || db.client == nil {
		return nil
	}
	return db.client.Close()
}

// ReadOnly executes f in a read-only transaction.
func (db *DB) ReadOnly(ctx context.Context, f func(context.Context, tkv.Transaction) error) error {
	return f(ctx, &sqlTx{client: db.client})
}

// ReadWrite executes f in a read-write transaction.
func (db *DB) ReadWrite(ctx context.Context, f func(context.Context, tkv.Transaction) error) error {
	return f(ctx, &sqlTx{client: db.client})
}

// A cockroachdbTx is the underlying cockroachdb transaction.
type sqlTx struct {
	client *sql.DB
}

func (s *sqlTx) ReadValues(ctx context.Context, keys []string) ([]string, error) {
	var res []string
	tx, err := s.client.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return res, err
	}

	for _, key := range keys {
		var k, v string
		err := tx.QueryRowContext(ctx, "SELECT key,value  FROM "+tableName+" WHERE key='"+key+"'").Scan(&k, &v)
		if err != nil {
			if err == sql.ErrNoRows {
				res = append(res, v)
				continue
			}
			tx.Rollback()
			return nil, err
		}
		res = append(res, v)
	}
	tx.Commit()
	return res, nil
}

func (s *sqlTx) ReadValue(ctx context.Context, key string) (string, error) {
	var k, v string
	tx, err := s.client.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return "", err
	}
	err = tx.QueryRowContext(ctx, "SELECT key,value  FROM "+tableName+" WHERE key='"+key+"'").Scan(&k, &v)
	if err != nil {
		if err == sql.ErrNoRows {
			tx.Commit()
			return "", nil
		}
		tx.Rollback()
		return v, err
	}
	tx.Commit()
	return v, nil
}

// BufferWrite buffers the given writes.
func (s *sqlTx) BufferWrites(writes []tkv.Write) error {
	tx, err := s.client.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}

	for _, w := range writes {
		var m string
		if w.Value == "" {
			m = fmt.Sprintf("DELETE FROM %s WHERE key = %s", tableName, w.Key)
		} else {
			m = fmt.Sprintf("UPSERT INTO %s (key, value) VALUES ('%s', '%s')", tableName, w.Key, w.Value)
		}
		_, err := tx.Exec(m)
		if err != nil {
			tx.Rollback()
			return err
		}
	}
	tx.Commit()
	return nil
}
