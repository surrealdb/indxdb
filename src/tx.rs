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

//! Buffered transaction layer for IndexedDB.
//!
//! IndexedDB transactions auto-commit whenever the event loop is reached
//! without any pending requests. Rust async/await yields to the JS event
//! loop on every `.await`, so multiple IDB operations against a single
//! transaction will trigger `TransactionInactiveError` as soon as the
//! second request fires after the first has settled.
//!
//! To avoid that hazard, this layer splits a logical transaction into two
//! phases:
//!
//! 1. **Read phase** -- each read opens a fresh, short-lived read-only IDB
//!    transaction. Reads are idempotent so a potential auto-commit between
//!    reads is harmless. Reads also consult an in-memory `BTreeMap` write
//!    buffer so that read-your-own-writes works correctly.
//!
//! 2. **Flush phase** (`commit`) -- a single fresh read-write IDB
//!    transaction is opened and *every* buffered mutation (put + delete) is
//!    dispatched synchronously, mirroring the spec-recommended `put_all`
//!    pattern. Only the *last* request handle is awaited; the transaction
//!    stays active because no `.await` is interleaved with the issuing
//!    loop. Finally we await the transaction itself, which resolves once
//!    IDB has durably committed the entire batch.
//!
//! This goes through the [`idb`] crate directly (rather than `rexie`)
//! because we need to construct, hold and only-conditionally-await
//! heterogeneous request handles to keep the transaction active across a
//! mixed put+delete batch -- something that rexie's `put_all` helper
//! cannot express.

use std::collections::BTreeMap;
use std::ops::Range;
use std::rc::Rc;

use idb::{
	request::{DeleteStoreRequest, PutStoreRequest},
	CursorDirection, Database as IdbDatabase, KeyRange, ObjectStore, Query, TransactionMode,
};
use wasm_bindgen::JsValue;

use crate::err::Error;
use crate::kv::Convert;
use crate::kv::Key;
use crate::kv::Val;
use crate::sp::Operation;
use crate::sp::Savepoint;

/// Object store name -- single store layout, kept identical to the
/// previous rexie-backed implementation so we don't break existing
/// IndexedDB databases on disk.
const STORE: &str = "kv";

#[derive(Clone, Debug)]
pub(crate) enum Buffered {
	Set(Val),
	Del,
}

/// The handle to the *last* IDB request we fire during a commit batch.
///
/// `idb`'s typed request wrappers (`PutStoreRequest`, `DeleteStoreRequest`)
/// each implement `IntoFuture` with different output types, so we can't
/// collect them into a uniform `Vec`. Instead we keep only the most recent
/// one -- which is all we need to keep the surrounding IDB transaction
/// active until every queued request has settled.
enum LastRequest {
	Put(PutStoreRequest),
	Delete(DeleteStoreRequest),
}

impl LastRequest {
	async fn finish(self) -> Result<(), Error> {
		match self {
			LastRequest::Put(req) => req.await.map(|_| ()).map_err(Into::into),
			LastRequest::Delete(req) => req.await.map_err(Into::into),
		}
	}
}

/// A serializable snapshot-isolated database transaction.
///
/// All mutations are buffered in-memory. On `commit()` they are flushed to
/// IndexedDB in a single synchronous batch so that the IDB transaction
/// never goes idle between requests.
pub struct Transaction {
	pub(crate) done: bool,
	pub(crate) write: bool,
	/// Shared reference to the IDB database for opening fresh transactions.
	pub(crate) db: Rc<IdbDatabase>,
	/// Buffered mutations: key -> Set(val) | Del.
	pub(crate) buffer: BTreeMap<Key, Buffered>,
	pub(crate) savepoints: Vec<Savepoint>,
	pub(crate) operations: Vec<Operation>,
}

impl Transaction {
	pub(crate) fn new(db: Rc<IdbDatabase>, write: bool) -> Transaction {
		Transaction {
			done: false,
			write,
			db,
			buffer: BTreeMap::new(),
			savepoints: Vec::new(),
			operations: Vec::new(),
		}
	}

	pub fn closed(&self) -> bool {
		self.done
	}

	/// Open a fresh read-only IDB store for a single read request.
	///
	/// The returned `idb::Transaction` is intentionally dropped together
	/// with the `ObjectStore` once the caller's await completes -- IDB
	/// auto-commits read-only transactions which have no further pending
	/// requests, so this is safe and matches the rexie behaviour.
	fn fresh_read_store(&self) -> Result<(idb::Transaction, ObjectStore), Error> {
		let tx = self.db.transaction(&[STORE], TransactionMode::ReadOnly)?;
		let store = tx.object_store(STORE)?;
		Ok((tx, store))
	}

	/// Open a fresh read-write IDB store for the commit flush.
	fn fresh_write_store(&self) -> Result<(idb::Transaction, ObjectStore), Error> {
		let tx = self.db.transaction(&[STORE], TransactionMode::ReadWrite)?;
		let store = tx.object_store(STORE)?;
		Ok((tx, store))
	}

	/// Read a key, checking the write buffer first.
	async fn buffered_get(&self, key: &Key) -> Result<Option<Val>, Error> {
		match self.buffer.get(key) {
			Some(Buffered::Set(v)) => Ok(Some(v.clone())),
			Some(Buffered::Del) => Ok(None),
			None => {
				let (_tx, store) = self.fresh_read_store()?;
				let res = store.get(key.clone().convert())?.await?;
				match res {
					Some(v) => Ok(Some(v.convert())),
					None => Ok(None),
				}
			}
		}
	}

	// ------------------------------------------------------------------
	// Transaction lifecycle
	// ------------------------------------------------------------------

	pub async fn cancel(&mut self) -> Result<(), Error> {
		if self.done {
			return Err(Error::TxClosed);
		}
		self.done = true;
		self.buffer.clear();
		Ok(())
	}

	/// Commit: flush all buffered writes to IndexedDB in one atomic batch.
	///
	/// Opens a single fresh read-write IDB transaction. Every buffered put
	/// and delete is dispatched synchronously -- no `.await` is interleaved
	/// with the issuing loop, so the IDB transaction stays continuously
	/// active until every request has been queued. Only the last request
	/// is awaited (this is the same pattern `idb::ObjectStore::put_all`
	/// uses internally). Finally we await the transaction itself, which
	/// resolves when the browser has durably committed the batch.
	pub async fn commit(&mut self) -> Result<(), Error> {
		if self.done {
			return Err(Error::TxClosed);
		}
		if !self.write {
			return Err(Error::TxNotWritable);
		}
		self.done = true;

		if self.buffer.is_empty() {
			return Ok(());
		}

		let (tx, store) = self.fresh_write_store()?;
		let buffer = std::mem::take(&mut self.buffer);

		// Fire every put/delete synchronously, keeping only the most
		// recently issued request handle. Because there is no `.await`
		// between iterations, the IDB transaction never sees an empty
		// pending-request queue and so cannot auto-commit early.
		let mut last: Option<LastRequest> = None;
		for (key, op) in buffer {
			let js_key: JsValue = key.convert();
			match op {
				Buffered::Set(val) => {
					let js_val: JsValue = val.convert();
					let req = store.put(&js_val, Some(&js_key))?;
					last = Some(LastRequest::Put(req));
				}
				Buffered::Del => {
					let req = store.delete(Query::Key(js_key))?;
					last = Some(LastRequest::Delete(req));
				}
			}
		}

		// Await the last request to surface any per-request error before
		// the transaction tries to commit.
		if let Some(req) = last {
			req.finish().await?;
		}

		// Wait for the IDB transaction to durably commit the whole batch.
		// `tx.await` resolves to `TransactionResult::{Committed, Aborted}`
		// or fails with a DOM error.
		match tx.await? {
			idb::TransactionResult::Committed => Ok(()),
			idb::TransactionResult::Aborted => {
				Err(Error::IndexedDbError("transaction aborted".to_string()))
			}
		}
	}

	// ------------------------------------------------------------------
	// Reads
	// ------------------------------------------------------------------

	pub async fn exists(&self, key: Key) -> Result<bool, Error> {
		if self.done {
			return Err(Error::TxClosed);
		}
		match self.buffer.get(&key) {
			Some(Buffered::Set(_)) => Ok(true),
			Some(Buffered::Del) => Ok(false),
			None => {
				let (_tx, store) = self.fresh_read_store()?;
				let res = store.get_key(key.convert())?.await?;
				Ok(res.is_some())
			}
		}
	}

	pub async fn get(&self, key: Key) -> Result<Option<Val>, Error> {
		if self.done {
			return Err(Error::TxClosed);
		}
		self.buffered_get(&key).await
	}

	// ------------------------------------------------------------------
	// Writes (buffered)
	// ------------------------------------------------------------------

	pub async fn set(&mut self, key: Key, val: Val) -> Result<(), Error> {
		if self.done {
			return Err(Error::TxClosed);
		}
		if !self.write {
			return Err(Error::TxNotWritable);
		}
		if !self.savepoints.is_empty() || !self.operations.is_empty() {
			match self.buffered_get(&key).await? {
				Some(existing_val) => {
					self.operations.push(Operation::RestoreValue(key.clone(), existing_val));
				}
				None => {
					self.operations.push(Operation::DeleteKey(key.clone()));
				}
			}
		}
		self.buffer.insert(key, Buffered::Set(val));
		Ok(())
	}

	pub async fn put(&mut self, key: Key, val: Val) -> Result<(), Error> {
		if self.done {
			return Err(Error::TxClosed);
		}
		if !self.write {
			return Err(Error::TxNotWritable);
		}
		match self.buffered_get(&key).await? {
			None => self.set(key, val).await,
			_ => Err(Error::KeyAlreadyExists),
		}
	}

	pub async fn putc(&mut self, key: Key, val: Val, chk: Option<Val>) -> Result<(), Error> {
		if self.done {
			return Err(Error::TxClosed);
		}
		if !self.write {
			return Err(Error::TxNotWritable);
		}
		match (self.buffered_get(&key).await?, chk) {
			(Some(v), Some(w)) if v == w => self.set(key, val).await,
			(None, None) => self.set(key, val).await,
			_ => Err(Error::ValNotExpectedValue),
		}
	}

	pub async fn del(&mut self, key: Key) -> Result<(), Error> {
		if self.done {
			return Err(Error::TxClosed);
		}
		if !self.write {
			return Err(Error::TxNotWritable);
		}
		if !self.savepoints.is_empty() || !self.operations.is_empty() {
			if let Some(existing_val) = self.buffered_get(&key).await? {
				self.operations.push(Operation::RestoreDeleted(key.clone(), existing_val));
			}
		}
		self.buffer.insert(key, Buffered::Del);
		Ok(())
	}

	pub async fn delc(&mut self, key: Key, chk: Option<Val>) -> Result<(), Error> {
		if self.done {
			return Err(Error::TxClosed);
		}
		if !self.write {
			return Err(Error::TxNotWritable);
		}
		match (self.buffered_get(&key).await?, chk) {
			(Some(v), Some(w)) if v == w => self.del(key).await,
			(None, None) => self.del(key).await,
			_ => Err(Error::ValNotExpectedValue),
		}
	}

	// ------------------------------------------------------------------
	// Range operations -- merge IDB results with the write buffer
	// ------------------------------------------------------------------

	pub async fn keys(&self, rng: Range<Key>, limit: u32) -> Result<Vec<Key>, Error> {
		if self.done {
			return Err(Error::TxClosed);
		}
		let Range {
			start,
			end,
		} = rng;
		let kr = bound(&start, &end)?;
		let idb_results =
			scan_cursor(&self.fresh_read_store()?.1, kr, Some(limit), CursorDirection::Next, false)
				.await?;

		let mut merged: BTreeMap<Key, ()> = BTreeMap::new();
		for (k, _) in idb_results {
			match self.buffer.get(&k) {
				Some(Buffered::Del) => {}
				_ => {
					merged.insert(k, ());
				}
			}
		}
		for (key, op) in self.buffer.range(start..end) {
			match op {
				Buffered::Set(_) => {
					merged.insert(key.clone(), ());
				}
				Buffered::Del => {
					merged.remove(key);
				}
			}
		}

		Ok(merged.into_keys().take(limit as usize).collect())
	}

	pub async fn keysr(&self, rng: Range<Key>, limit: u32) -> Result<Vec<Key>, Error> {
		if self.done {
			return Err(Error::TxClosed);
		}
		let Range {
			start,
			end,
		} = rng;
		let kr = bound(&start, &end)?;
		let idb_results =
			scan_cursor(&self.fresh_read_store()?.1, kr, Some(limit), CursorDirection::Prev, false)
				.await?;

		let mut merged: BTreeMap<Key, ()> = BTreeMap::new();
		for (k, _) in idb_results {
			match self.buffer.get(&k) {
				Some(Buffered::Del) => {}
				_ => {
					merged.insert(k, ());
				}
			}
		}
		for (key, op) in self.buffer.range(start..end) {
			match op {
				Buffered::Set(_) => {
					merged.insert(key.clone(), ());
				}
				Buffered::Del => {
					merged.remove(key);
				}
			}
		}

		Ok(merged.into_keys().rev().take(limit as usize).collect())
	}

	pub async fn scan(&self, rng: Range<Key>, limit: u32) -> Result<Vec<(Key, Val)>, Error> {
		if self.done {
			return Err(Error::TxClosed);
		}
		let Range {
			start,
			end,
		} = rng;
		let kr = bound(&start, &end)?;
		let idb_results =
			scan_cursor(&self.fresh_read_store()?.1, kr, Some(limit), CursorDirection::Next, true)
				.await?;

		let mut merged: BTreeMap<Key, Val> = BTreeMap::new();
		for (k, v) in idb_results {
			match self.buffer.get(&k) {
				Some(Buffered::Del) => {}
				Some(Buffered::Set(bv)) => {
					merged.insert(k, bv.clone());
				}
				None => {
					merged.insert(k, v);
				}
			}
		}
		for (key, op) in self.buffer.range(start..end) {
			match op {
				Buffered::Set(v) => {
					merged.insert(key.clone(), v.clone());
				}
				Buffered::Del => {
					merged.remove(key);
				}
			}
		}

		Ok(merged.into_iter().take(limit as usize).collect())
	}

	pub async fn scanr(&self, rng: Range<Key>, limit: u32) -> Result<Vec<(Key, Val)>, Error> {
		if self.done {
			return Err(Error::TxClosed);
		}
		let Range {
			start,
			end,
		} = rng;
		let kr = bound(&start, &end)?;
		let idb_results =
			scan_cursor(&self.fresh_read_store()?.1, kr, Some(limit), CursorDirection::Prev, true)
				.await?;

		let mut merged: BTreeMap<Key, Val> = BTreeMap::new();
		for (k, v) in idb_results {
			match self.buffer.get(&k) {
				Some(Buffered::Del) => {}
				Some(Buffered::Set(bv)) => {
					merged.insert(k, bv.clone());
				}
				None => {
					merged.insert(k, v);
				}
			}
		}
		for (key, op) in self.buffer.range(start..end) {
			match op {
				Buffered::Set(v) => {
					merged.insert(key.clone(), v.clone());
				}
				Buffered::Del => {
					merged.remove(key);
				}
			}
		}

		Ok(merged.into_iter().rev().take(limit as usize).collect())
	}

	// ------------------------------------------------------------------
	// Savepoints
	// ------------------------------------------------------------------

	pub async fn set_savepoint(&mut self) -> Result<(), Error> {
		if self.done {
			return Err(Error::TxClosed);
		}
		if !self.write {
			return Err(Error::TxNotWritable);
		}
		self.savepoints.push(Savepoint {
			operations: std::mem::take(&mut self.operations),
		});
		Ok(())
	}

	/// Rollback to the most recent savepoint by replaying undo operations
	/// against the in-memory buffer. No IDB calls are needed.
	pub async fn rollback_to_savepoint(&mut self) -> Result<(), Error> {
		if self.done {
			return Err(Error::TxClosed);
		}
		if !self.write {
			return Err(Error::TxNotWritable);
		}
		if self.savepoints.is_empty() {
			return Err(Error::NoSavepoint);
		}
		let savepoint = self.savepoints.pop().unwrap();
		for op in self.operations.iter().rev() {
			match op {
				Operation::DeleteKey(key) => {
					self.buffer.remove(key);
				}
				Operation::RestoreValue(key, val) => {
					self.buffer.insert(key.clone(), Buffered::Set(val.clone()));
				}
				Operation::RestoreDeleted(key, val) => {
					self.buffer.insert(key.clone(), Buffered::Set(val.clone()));
				}
			}
		}
		self.operations = savepoint.operations;
		Ok(())
	}
}

/// Build a `KeyRange` covering `start <= key < end`.
fn bound(start: &Key, end: &Key) -> Result<KeyRange, Error> {
	let lower: JsValue = start.clone().convert();
	let upper: JsValue = end.clone().convert();
	KeyRange::bound(&lower, &upper, None, Some(true)).map_err(Into::into)
}

/// Iterate a cursor over `range`, returning up to `limit` `(key, value)`
/// pairs. When `with_value` is `false`, the returned values are empty
/// `Vec`s -- callers that only need keys should use `false` to avoid a
/// pointless clone of the stored value bytes.
async fn scan_cursor(
	store: &ObjectStore,
	range: KeyRange,
	limit: Option<u32>,
	direction: CursorDirection,
	with_value: bool,
) -> Result<Vec<(Key, Val)>, Error> {
	let cursor = store.open_cursor(Some(Query::KeyRange(range)), Some(direction))?.await?;
	let Some(cursor) = cursor else {
		return Ok(Vec::new());
	};
	let mut cursor = cursor.into_managed();
	let mut out = Vec::new();
	let cap = limit.unwrap_or(u32::MAX);
	for _ in 0..cap {
		let key = cursor.key()?;
		let value = if with_value {
			cursor.value()?
		} else {
			Some(JsValue::NULL)
		};
		match (key, value) {
			(Some(k), Some(v)) => {
				let key: Key = k.convert();
				let val: Val = if with_value {
					v.convert()
				} else {
					Vec::new()
				};
				out.push((key, val));
				cursor.next(None).await?;
			}
			_ => break,
		}
	}
	Ok(out)
}
