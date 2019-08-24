/*
   Copyright 2018-2019 Banco Bilbao Vizcaya Argentaria, S.A.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package postgres

import (
	"database/sql"

	"github.com/bbva/qed/log"
	"github.com/bbva/qed/storage"
	"github.com/lib/pq"
	_ "github.com/lib/pq"
)

type PostgresStore struct {
	db *sql.DB
}

func NewPostgresStore(host, user, pass string) (*PostgresStore, error) {
	connStr := "postgres://qed:qed@localhost/qed?sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}
	return &PostgresStore{
		db: db,
	}, nil
}

func (pg *PostgresStore) Mutate(mutations []*storage.Mutation, metadata []byte) error {

	txn, err := pg.db.Begin()
	if err != nil {
		return err
	}

	tables := make(map[storage.Table][]*storage.Mutation)
	for _, m := range mutations {
		tables[m.Table] = append(tables[m.Table], m)
	}

	for k, t := range tables {
		stmt, err := txn.Prepare(pq.CopyIn(k.String(), "k", "v", "meta"))
		if err != nil {
			return err
		}
		for _, m := range t {
			_, err = stmt.Exec(m.Key, m.Value, metadata)
			if err != nil {
				txn.Rollback()
				return err
			}
		}

		_, err = stmt.Exec()
		if err != nil {
			txn.Rollback()
			return err
		}
		err = stmt.Close()
		if err != nil {
			txn.Rollback()
			return err
		}
	}

	err = txn.Commit()
	if err != nil {
		txn.Rollback()
		return err
	}
	return nil
}

func (pg *PostgresStore) GetRange(table storage.Table, start, end []byte) (storage.KVRange, error) {
	rows, err := pg.db.Query("SELECT k,v FROM "+table.String()+" WHERE k > $1 AND k <= $2", start, end)
	if err != nil {
		return nil, err
	}
	result := make(storage.KVRange, 0)
	for rows.Next() {
		var kv storage.KVPair
		err := rows.Scan(&kv.Key, &kv.Value)
		if err != nil {
			return nil, err
		}
		result = append(result, kv)
	}
	rerr := rows.Close()
	if rerr != nil {
		return nil, err
	}

	// Rows.Err will report the last error encountered by Rows.Scan.
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func (pg *PostgresStore) Get(table storage.Table, key []byte) (*storage.KVPair, error) {
	rows, err := pg.db.Query("SELECT k,v from "+table.String()+" where k=$1", key)
	if err != nil {
		return nil, err
	}
	kv := new(storage.KVPair)
	rows.Next()
	err = rows.Scan(&kv.Key, &kv.Value)
	if err != nil {
		return nil, err
	}
	rerr := rows.Close()
	if rerr != nil {
		return nil, err
	}

	// Rows.Err will report the last error encountered by Rows.Scan.
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return kv, nil
}

type PostgresKVPairReader struct {
	rows *sql.Rows
}

func NewPostgreKVPairReader(db *sql.DB, table storage.Table) *PostgresKVPairReader {
	rows, err := db.Query("SELECT k,v from " + table.String())
	if err != nil {
		log.Error(err)
	}
	return &PostgresKVPairReader{
		rows: rows,
	}
}

func (r *PostgresKVPairReader) Read(buffer []*storage.KVPair) (int, error) {
	var i int
	for i = 0; i < len(buffer); i++ {
		kv := new(storage.KVPair)
		r.rows.Next()
		err := r.rows.Scan(&kv.Key, &kv.Value)
		if err != nil {
			return i, err
		}
		buffer[i] = kv
	}
	return i, nil
}

func (r *PostgresKVPairReader) Close() {
	err := r.rows.Close()
	if err != nil {
		log.Error(err)
	}

	// Rows.Err will report the last error encountered by Rows.Scan.
	if err = r.rows.Err(); err != nil {
		log.Error(err)
	}
}

func (pg *PostgresStore) GetAll(table storage.Table) storage.KVPairReader {
	return NewPostgreKVPairReader(pg.db, table)
}

func (pg *PostgresStore) GetLast(table storage.Table) (*storage.KVPair, error) {

	return nil, nil
}

func (pg *PostgresStore) Close() error {

	return nil
}
