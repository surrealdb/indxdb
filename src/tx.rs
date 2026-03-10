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
//! IndexedDB auto-commits a transaction whenever the event loop is idle and
//! there are no pending requests. Rust async/await yields to the JS event loop
//! on every `.await`, so multiple IDB operations within a single Rexie
//! transaction will cause `TransactionInactiveError` as soon as the second
//! request fires after an idle microtask checkpoint.
//!
//! To work around this, we split the transaction into two phases:
//!
//! 1. **Read phase** – each read opens a fresh, short-lived read-only IDB
//!    transaction. This is safe because reads are idempotent and a potential
//!    auto-commit between reads is harmless. Reads also consult a `BTreeMap`
//!    write-buffer so that read-your-own-writes works correctly.
//!
//! 2. **Flush phase** (`commit`) – a *new* read-write IDB transaction is
//!    opened and every buffered mutation is dispatched. Puts use `put_all`
//!    which fires all IDB requests synchronously without any `.await` between
//!    them. Deletes are issued sequentially (each awaited), which is safe
//!    because the Rust executor polls the next delete in the same microtask
//!    as the previous completion callback. Finally, `transaction.done()` is
//!    awaited, which resolves when IDB has durably committed everything.

use crate::err::Error;
use crate::kv::Convert;
use crate::kv::Key;
use crate::kv::Val;
use crate::sp::Operation;
use crate::sp::Savepoint;
use rexie::Direction;
use rexie::KeyRange;
use rexie::Rexie;
use rexie::Store;
use rexie::TransactionMode;
use std::collections::BTreeMap;
use std::ops::Range;
use std::rc::Rc;
use wasm_bindgen::JsValue;

#[derive(Clone, Debug)]
pub(crate) enum Buffered {
	Set(Val),
	Del,
}

/// A serializable snapshot isolated database transaction.
///
/// All mutations are buffered in-memory. On `commit()` they are flushed to
/// IndexedDB in a single synchronous batch so that the IDB transaction never
/// goes idle between requests.
pub struct Transaction {
	pub(crate) done: bool,
	pub(crate) write: bool,
	/// Shared reference to the Rexie database for opening new IDB transactions.
	pub(crate) db: Rc<Rexie>,
	/// Buffered mutations: key -> Set(val) | Del
	pub(crate) buffer: BTreeMap<Key, Buffered>,
	pub(crate) savepoints: Vec<Savepoint>,
	pub(crate) operations: Vec<Operation>,
}

impl Transaction {
	pub(crate) fn new(db: Rc<Rexie>, write: bool) -> Transaction {
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
	fn fresh_read_store(&self) -> Result<Store, Error> {
		let tx = self.db.transaction(&["kv"], TransactionMode::ReadOnly)
			.map_err(|_| Error::TxError)?;
		tx.store("kv").map_err(|_| Error::TxError)
	}

	/// Read a key, checking the write buffer first.
	async fn buffered_get(&self, key: &Key) -> Result<Option<Val>, Error> {
		match self.buffer.get(key) {
			Some(Buffered::Set(v)) => Ok(Some(v.clone())),
			Some(Buffered::Del) => Ok(None),
			None => {
				let store = self.fresh_read_store()?;
				let res = store.get(key.clone().convert()).await?;
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
	/// Opens a fresh read-write IDB transaction. Puts are batched via
	/// `put_all` (all IDB requests fired synchronously, only the last
	/// awaited). Deletes are issued sequentially -- each `await` is safe
	/// because the next `delete()` call is queued in the same microtask
	/// as the previous request's completion, keeping the transaction alive.
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

		let flush_tx = self.db.transaction(&["kv"], TransactionMode::ReadWrite)
			.map_err(|_| Error::TxError)?;
		let flush_store = flush_tx.store("kv").map_err(|_| Error::TxError)?;

		// Build an iterator of (JsValue, Option<JsValue>) for put_all, and
		// collect deletes separately.
		let buffer = std::mem::take(&mut self.buffer);

		let mut puts: Vec<(JsValue, Option<JsValue>)> = Vec::new();
		let mut deletes: Vec<JsValue> = Vec::new();

		for (key, op) in buffer {
			let js_key: JsValue = key.convert();
			match op {
				Buffered::Set(val) => {
					let js_val: JsValue = val.convert();
					puts.push((js_val, Some(js_key)));
				}
				Buffered::Del => {
					deletes.push(js_key);
				}
			}
		}

		// Use put_all which fires all IDB requests synchronously (no .await
		// between them) and only awaits the last request's result.
		if !puts.is_empty() {
			flush_store.put_all(puts.into_iter()).await?;
		}

		// Delete all keys. Each `Store::delete` awaits one IDB request,
		// but this is safe: completing request N immediately queues
		// request N+1 within the same microtask (wasm_bindgen_futures
		// polls continuations synchronously in the IDB callback), so
		// the transaction always has a pending request and never
		// auto-commits. This is the same pattern rexie's `scan` uses
		// internally when iterating a cursor.
		for js_key in deletes {
			flush_store.delete(js_key).await?;
		}

		// Wait for the IDB transaction to durably commit everything.
		flush_tx.done().await?;

		Ok(())
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
				let store = self.fresh_read_store()?;
				let res = store.key_exists(key.convert()).await?;
				Ok(res)
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
	// Range operations – merge IDB results with the write buffer
	// ------------------------------------------------------------------

	pub async fn keys(&self, rng: Range<Key>, limit: u32) -> Result<Vec<Key>, Error> {
		if self.done {
			return Err(Error::TxClosed);
		}
		let Range { start, end } = rng;
		let dir = Some(Direction::Next);
		let kr = KeyRange::bound(&start.clone().convert(), &end.clone().convert(), None, Some(true));
		let kr = kr.map_err(|e| Error::IndexedDbError(e.to_string()))?;

		let store = self.fresh_read_store()?;
		let idb_results = store.scan(Some(kr), Some(limit), None, dir).await?;

		let mut merged: BTreeMap<Key, ()> = BTreeMap::new();
		for (k, _) in idb_results {
			let key: Key = k.convert();
			match self.buffer.get(&key) {
				Some(Buffered::Del) => {}
				_ => { merged.insert(key, ()); }
			}
		}
		for (key, op) in self.buffer.range(start..end) {
			match op {
				Buffered::Set(_) => { merged.insert(key.clone(), ()); }
				Buffered::Del => { merged.remove(key); }
			}
		}

		let res: Vec<Key> = merged.into_keys().take(limit as usize).collect();
		Ok(res)
	}

	pub async fn keysr(&self, rng: Range<Key>, limit: u32) -> Result<Vec<Key>, Error> {
		if self.done {
			return Err(Error::TxClosed);
		}
		let Range { start, end } = rng;
		let dir = Some(Direction::Prev);
		let kr = KeyRange::bound(&end.clone().convert(), &start.clone().convert(), None, Some(true));
		let kr = kr.map_err(|e| Error::IndexedDbError(e.to_string()))?;

		let store = self.fresh_read_store()?;
		let idb_results = store.scan(Some(kr), Some(limit), None, dir).await?;

		let mut merged: BTreeMap<Key, ()> = BTreeMap::new();
		for (k, _) in idb_results {
			let key: Key = k.convert();
			match self.buffer.get(&key) {
				Some(Buffered::Del) => {}
				_ => { merged.insert(key, ()); }
			}
		}
		for (key, op) in self.buffer.range(start..end) {
			match op {
				Buffered::Set(_) => { merged.insert(key.clone(), ()); }
				Buffered::Del => { merged.remove(key); }
			}
		}

		let res: Vec<Key> = merged.into_keys().rev().take(limit as usize).collect();
		Ok(res)
	}

	pub async fn scan(&self, rng: Range<Key>, limit: u32) -> Result<Vec<(Key, Val)>, Error> {
		if self.done {
			return Err(Error::TxClosed);
		}
		let Range { start, end } = rng;
		let dir = Some(Direction::Next);
		let kr = KeyRange::bound(&start.clone().convert(), &end.clone().convert(), None, Some(true));
		let kr = kr.map_err(|e| Error::IndexedDbError(e.to_string()))?;

		let store = self.fresh_read_store()?;
		let idb_results = store.scan(Some(kr), Some(limit), None, dir).await?;

		let mut merged: BTreeMap<Key, Val> = BTreeMap::new();
		for (k, v) in idb_results {
			let key: Key = k.convert();
			let val: Val = v.convert();
			match self.buffer.get(&key) {
				Some(Buffered::Del) => {}
				Some(Buffered::Set(bv)) => { merged.insert(key, bv.clone()); }
				None => { merged.insert(key, val); }
			}
		}
		for (key, op) in self.buffer.range(start..end) {
			match op {
				Buffered::Set(v) => { merged.insert(key.clone(), v.clone()); }
				Buffered::Del => { merged.remove(key); }
			}
		}

		let res: Vec<(Key, Val)> = merged.into_iter().take(limit as usize).collect();
		Ok(res)
	}

	pub async fn scanr(&self, rng: Range<Key>, limit: u32) -> Result<Vec<(Key, Val)>, Error> {
		if self.done {
			return Err(Error::TxClosed);
		}
		let Range { start, end } = rng;
		let dir = Some(Direction::Prev);
		let kr = KeyRange::bound(&end.clone().convert(), &start.clone().convert(), None, Some(true));
		let kr = kr.map_err(|e| Error::IndexedDbError(e.to_string()))?;

		let store = self.fresh_read_store()?;
		let idb_results = store.scan(Some(kr), Some(limit), None, dir).await?;

		let mut merged: BTreeMap<Key, Val> = BTreeMap::new();
		for (k, v) in idb_results {
			let key: Key = k.convert();
			let val: Val = v.convert();
			match self.buffer.get(&key) {
				Some(Buffered::Del) => {}
				Some(Buffered::Set(bv)) => { merged.insert(key, bv.clone()); }
				None => { merged.insert(key, val); }
			}
		}
		for (key, op) in self.buffer.range(start..end) {
			match op {
				Buffered::Set(v) => { merged.insert(key.clone(), v.clone()); }
				Buffered::Del => { merged.remove(key); }
			}
		}

		let res: Vec<(Key, Val)> = merged.into_iter().rev().take(limit as usize).collect();
		Ok(res)
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
	/// against the in-memory buffer. No IDB calls needed.
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
