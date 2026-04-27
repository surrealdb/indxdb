// Copyright © SurrealDB Ltd
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Headless-browser integration tests for the buffered transaction layer.
//!
//! Each test opens a fresh IndexedDB database under a unique name so the
//! suite is safe to run in parallel and does not pollute prior runs.

#![cfg(target_arch = "wasm32")]

use indxdb::Database;
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

wasm_bindgen_test_configure!(run_in_browser);

/// Build a database name unique to this test run. Combines the millisecond
/// timestamp with a random component so concurrent suites and
/// re-runs of the same test in a single browser context do not collide.
fn unique_db_name(label: &str) -> String {
	let now = js_sys::Date::now() as u64;
	let rnd = (js_sys::Math::random() * 1.0e9) as u64;
	format!("indxdb_test_{label}_{now}_{rnd}")
}

async fn open(label: &str) -> Database {
	Database::new(&unique_db_name(label)).await.expect("open database")
}

#[wasm_bindgen_test]
async fn put_and_get_roundtrip() {
	let db = open("put_get").await;

	let mut tx = db.begin(true).await.expect("begin write");
	tx.set(b"alpha".to_vec(), b"one".to_vec()).await.expect("set alpha");
	tx.set(b"beta".to_vec(), b"two".to_vec()).await.expect("set beta");
	tx.commit().await.expect("commit puts");

	let tx = db.begin(false).await.expect("begin read");
	assert_eq!(tx.get(b"alpha".to_vec()).await.expect("get alpha"), Some(b"one".to_vec()));
	assert_eq!(tx.get(b"beta".to_vec()).await.expect("get beta"), Some(b"two".to_vec()));
	assert_eq!(tx.get(b"gamma".to_vec()).await.expect("get gamma"), None);
}

#[wasm_bindgen_test]
async fn delete_after_put_commits_atomically() {
	let db = open("delete_after_put").await;

	let mut tx = db.begin(true).await.expect("begin seed");
	tx.set(b"keep".to_vec(), b"v1".to_vec()).await.expect("seed keep");
	tx.set(b"drop".to_vec(), b"v2".to_vec()).await.expect("seed drop");
	tx.commit().await.expect("seed commit");

	let mut tx = db.begin(true).await.expect("begin mutate");
	tx.del(b"drop".to_vec()).await.expect("buffer delete");
	tx.set(b"new".to_vec(), b"v3".to_vec()).await.expect("buffer set");
	tx.commit().await.expect("mutate commit");

	let tx = db.begin(false).await.expect("begin read");
	assert_eq!(tx.get(b"keep".to_vec()).await.expect("get keep"), Some(b"v1".to_vec()));
	assert_eq!(tx.get(b"drop".to_vec()).await.expect("get drop"), None);
	assert_eq!(tx.get(b"new".to_vec()).await.expect("get new"), Some(b"v3".to_vec()));
}

#[wasm_bindgen_test]
async fn mixed_put_delete_in_single_commit() {
	// Regression: this is the shape of work that triggered
	// `TransactionInactiveError` in indxdb 0.11.0 -- multiple writes
	// fired on a single IDB transaction across awaits. The buffered
	// commit must flush all of these in one atomic batch.
	let db = open("mixed").await;

	let mut tx = db.begin(true).await.expect("begin seed");
	for i in 0u8..16 {
		tx.set(vec![b'k', i], vec![b'v', i]).await.expect("seed");
	}
	tx.commit().await.expect("seed commit");

	let mut tx = db.begin(true).await.expect("begin mixed");
	for i in 0u8..16 {
		if i % 2 == 0 {
			tx.set(vec![b'k', i], vec![b'V', i]).await.expect("update");
		} else {
			tx.del(vec![b'k', i]).await.expect("delete");
		}
	}
	tx.commit().await.expect("mixed commit");

	let tx = db.begin(false).await.expect("begin read");
	for i in 0u8..16 {
		let got = tx.get(vec![b'k', i]).await.expect("get");
		if i % 2 == 0 {
			assert_eq!(got, Some(vec![b'V', i]));
		} else {
			assert_eq!(got, None);
		}
	}
}

#[wasm_bindgen_test]
async fn multi_transaction_use_simulation() {
	// Regression for surrealdb.js#571 and surrealist#1155: a sequence
	// of small write transactions, mirroring the way `db.use(ns, db)`
	// inserts a namespace record and then a database record in two
	// back-to-back transactions.
	let db = open("use_sim").await;

	let mut tx = db.begin(true).await.expect("begin ns");
	tx.set(b"ns:default".to_vec(), b"{namespace_id:1}".to_vec()).await.expect("ns put");
	tx.commit().await.expect("ns commit");

	let mut tx = db.begin(true).await.expect("begin db");
	tx.set(b"db:default:default".to_vec(), b"{database_id:1}".to_vec()).await.expect("db put");
	tx.commit().await.expect("db commit");

	let tx = db.begin(false).await.expect("begin read");
	assert_eq!(
		tx.get(b"ns:default".to_vec()).await.expect("get ns"),
		Some(b"{namespace_id:1}".to_vec())
	);
	assert_eq!(
		tx.get(b"db:default:default".to_vec()).await.expect("get db"),
		Some(b"{database_id:1}".to_vec())
	);
}

#[wasm_bindgen_test]
async fn savepoint_rollback_restores_buffer() {
	let db = open("savepoint").await;

	let mut tx = db.begin(true).await.expect("begin seed");
	tx.set(b"x".to_vec(), b"original".to_vec()).await.expect("seed x");
	tx.commit().await.expect("seed commit");

	let mut tx = db.begin(true).await.expect("begin sp");
	tx.set(b"y".to_vec(), b"new".to_vec()).await.expect("set y");
	tx.set_savepoint().await.expect("savepoint");
	tx.set(b"x".to_vec(), b"changed".to_vec()).await.expect("modify x");
	tx.del(b"y".to_vec()).await.expect("delete y");
	tx.set(b"z".to_vec(), b"added".to_vec()).await.expect("add z");
	tx.rollback_to_savepoint().await.expect("rollback");
	tx.commit().await.expect("commit after rollback");

	let tx = db.begin(false).await.expect("begin read");
	assert_eq!(tx.get(b"x".to_vec()).await.expect("get x"), Some(b"original".to_vec()));
	assert_eq!(tx.get(b"y".to_vec()).await.expect("get y"), Some(b"new".to_vec()));
	assert_eq!(tx.get(b"z".to_vec()).await.expect("get z"), None);
}

#[wasm_bindgen_test]
async fn read_your_own_writes() {
	let db = open("ryow").await;

	let mut tx = db.begin(true).await.expect("begin");
	tx.set(b"a".to_vec(), b"1".to_vec()).await.expect("set");
	assert_eq!(tx.get(b"a".to_vec()).await.expect("get"), Some(b"1".to_vec()));
	tx.del(b"a".to_vec()).await.expect("del");
	assert_eq!(tx.get(b"a".to_vec()).await.expect("get after del"), None);
	assert!(!tx.exists(b"a".to_vec()).await.expect("exists after del"));
	tx.commit().await.expect("commit");
}

#[wasm_bindgen_test]
async fn scan_merges_buffer_with_store() {
	let db = open("scan").await;

	let mut tx = db.begin(true).await.expect("begin seed");
	for i in 0u8..5 {
		tx.set(vec![b'k', i], vec![i]).await.expect("seed");
	}
	tx.commit().await.expect("seed commit");

	let mut tx = db.begin(true).await.expect("begin scan tx");
	tx.set(vec![b'k', 5], vec![5]).await.expect("set new");
	tx.del(vec![b'k', 2]).await.expect("delete middle");
	tx.set(vec![b'k', 3], vec![33]).await.expect("override");

	let scanned = tx.scan(vec![b'k', 0]..vec![b'k', 6], 100).await.expect("scan");
	let keys: Vec<Vec<u8>> = scanned.iter().map(|(k, _)| k.clone()).collect();
	assert_eq!(
		keys,
		vec![vec![b'k', 0], vec![b'k', 1], vec![b'k', 3], vec![b'k', 4], vec![b'k', 5],]
	);
	let vals: Vec<Vec<u8>> = scanned.iter().map(|(_, v)| v.clone()).collect();
	assert_eq!(vals, vec![vec![0], vec![1], vec![33], vec![4], vec![5]]);

	tx.commit().await.expect("commit scan tx");
}

#[wasm_bindgen_test]
async fn putc_and_delc_conditional_writes() {
	let db = open("conditional").await;

	let mut tx = db.begin(true).await.expect("begin seed");
	tx.set(b"k".to_vec(), b"v0".to_vec()).await.expect("seed");
	tx.commit().await.expect("seed commit");

	let mut tx = db.begin(true).await.expect("begin putc ok");
	tx.putc(b"k".to_vec(), b"v1".to_vec(), Some(b"v0".to_vec())).await.expect("putc match");
	tx.commit().await.expect("commit");

	let mut tx = db.begin(true).await.expect("begin putc fail");
	let err = tx
		.putc(b"k".to_vec(), b"v2".to_vec(), Some(b"v0".to_vec()))
		.await
		.expect_err("putc should fail with stale check");
	assert!(matches!(err, indxdb::Error::ValNotExpectedValue), "unexpected error: {err}");
	tx.cancel().await.expect("cancel");

	let mut tx = db.begin(true).await.expect("begin delc");
	tx.delc(b"k".to_vec(), Some(b"v1".to_vec())).await.expect("delc match");
	tx.commit().await.expect("commit delc");

	let tx = db.begin(false).await.expect("begin read");
	assert_eq!(tx.get(b"k".to_vec()).await.expect("get"), None);
}
