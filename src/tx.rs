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

//! This module stores the database transaction logic.

use crate::err::Error;
use crate::kv::Convert;
use crate::kv::Key;
use crate::kv::Val;
use crate::sp::Operation;
use crate::sp::Savepoint;
use rexie::Direction;
use rexie::KeyRange;
use rexie::Store;
use rexie::Transaction as RexieTransaction;
use std::ops::Range;

/// A serializable snapshot isolated database transaction
pub struct Transaction {
	/// Is the transaction complete?
	pub(crate) done: bool,
	/// Is the transaction read+write?
	pub(crate) write: bool,
	/// The underlying database store
	pub(crate) datastore: Option<Store>,
	/// The underlying database transaction
	pub(crate) transaction: Option<RexieTransaction>,
	/// Stack of savepoints for nested rollback support
	pub(crate) savepoints: Vec<Savepoint>,
	/// Current undo operations since the last savepoint
	pub(crate) operations: Vec<Operation>,
}

impl Transaction {
	/// Create a new transaction
	pub(crate) fn new(tx: RexieTransaction, st: Store, write: bool) -> Transaction {
		Transaction {
			done: false,
			write,
			datastore: Some(st),
			transaction: Some(tx),
			savepoints: Vec::new(),
			operations: Vec::new(),
		}
	}

	/// Check if the transaction is closed
	pub fn closed(&self) -> bool {
		self.done
	}

	/// Cancel the transaction and rollback any changes
	pub async fn cancel(&mut self) -> Result<(), Error> {
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxClosed);
		}
		// Mark this transaction as done
		self.done = true;
		// Abort the indexdb transaction
		self.transaction.take().unwrap().abort().await?;
		// Continue
		Ok(())
	}

	/// Commit the transaction and store all changes
	pub async fn commit(&mut self) -> Result<(), Error> {
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxClosed);
		}
		// Check to see if transaction is writable
		if !self.write {
			return Err(Error::TxNotWritable);
		}
		// Mark this transaction as done
		self.done = true;
		// Commit the indexdb transaction
		self.transaction.take().unwrap().done().await?;
		// Continue
		Ok(())
	}

	/// Check if a key exists in the database
	pub async fn exists(&mut self, key: Key) -> Result<bool, Error> {
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxClosed);
		}
		// Check the key
		let res = self.datastore.as_ref().unwrap().key_exists(key.convert()).await?;
		// Return result
		Ok(res)
	}

	/// Fetch a key from the database
	pub async fn get(&mut self, key: Key) -> Result<Option<Val>, Error> {
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxClosed);
		}
		// Get the key
		let res = self.datastore.as_ref().unwrap().get(key.convert()).await?;
		// Return result
		match res {
			Some(v) => Ok(Some(v.convert())),
			None => Ok(None),
		}
	}

	/// Insert or update a key in the database
	pub async fn set(&mut self, key: Key, val: Val) -> Result<(), Error> {
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxClosed);
		}
		// Check to see if transaction is writable
		if !self.write {
			return Err(Error::TxNotWritable);
		}
		// Record operation if we have savepoints
		if !self.savepoints.is_empty() || !self.operations.is_empty() {
			// Check if key already exists to determine undo operation
			match self.get(key.clone()).await? {
				Some(existing_val) => {
					// Key exists, record operation to restore old value
					self.operations.push(Operation::RestoreValue(key.clone(), existing_val));
				}
				None => {
					// Key doesn't exist, record operation to delete it
					self.operations.push(Operation::DeleteKey(key.clone()));
				}
			}
		}
		// Set the key
		self.datastore.as_ref().unwrap().put(&val.convert(), Some(&key.convert())).await?;
		// Return result
		Ok(())
	}

	/// Insert a key if it doesn't exist in the database
	pub async fn put(&mut self, key: Key, val: Val) -> Result<(), Error> {
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxClosed);
		}
		// Check to see if transaction is writable
		if !self.write {
			return Err(Error::TxNotWritable);
		}
		// Set the key
		match self.get(key.clone()).await? {
			None => self.set(key, val).await?,
			_ => return Err(Error::KeyAlreadyExists),
		};
		// Return result
		Ok(())
	}

	/// Insert a key if it matches a value
	pub async fn putc(&mut self, key: Key, val: Val, chk: Option<Val>) -> Result<(), Error> {
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxClosed);
		}
		// Check to see if transaction is writable
		if !self.write {
			return Err(Error::TxNotWritable);
		}
		// Set the key
		match (self.get(key.clone()).await?, chk) {
			(Some(v), Some(w)) if v == w => self.set(key, val).await?,
			(None, None) => self.set(key, val).await?,
			_ => return Err(Error::ValNotExpectedValue),
		};
		// Return result
		Ok(())
	}

	/// Delete a key from the database
	pub async fn del(&mut self, key: Key) -> Result<(), Error> {
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxClosed);
		}
		// Check to see if transaction is writable
		if !self.write {
			return Err(Error::TxNotWritable);
		}
		// Record operation if we have savepoints
		if !self.savepoints.is_empty() || !self.operations.is_empty() {
			// Check if key exists to record restoration operation
			if let Some(existing_val) = self.get(key.clone()).await? {
				// Key exists, record operation to restore it
				self.operations.push(Operation::RestoreDeleted(key.clone(), existing_val));
			}
		}
		// Remove the key
		self.datastore.as_ref().unwrap().delete(key.convert()).await?;
		// Return result
		Ok(())
	}

	/// Delete a key if it matches a value
	pub async fn delc(&mut self, key: Key, chk: Option<Val>) -> Result<(), Error> {
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxClosed);
		}
		// Check to see if transaction is writable
		if !self.write {
			return Err(Error::TxNotWritable);
		}
		// Remove the key
		match (self.get(key.clone()).await?, chk) {
			(Some(v), Some(w)) if v == w => self.del(key).await?,
			(None, None) => self.del(key).await?,
			_ => return Err(Error::ValNotExpectedValue),
		};
		// Return result
		Ok(())
	}

	/// Retrieve a range of keys from the databases
	pub async fn keys(&mut self, rng: Range<Key>, limit: u32) -> Result<Vec<Key>, Error> {
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxClosed);
		}
		// Get the iteration direction
		let dir = Some(Direction::Next);
		// Convert the range to JavaScript
		let rng = KeyRange::bound(&rng.start.convert(), &rng.end.convert(), None, Some(true));
		let rng = rng.map_err(|e| Error::IndexedDbError(e.to_string()))?;
		// Scan the keys
		let res = self.datastore.as_ref().unwrap().scan(Some(rng), Some(limit), None, dir).await?;
		let res = res.into_iter().map(|(k, _)| k.convert()).collect();
		// Return result
		Ok(res)
	}

	/// Retrieve a range of keys from the databases in reverse order
	pub async fn keysr(&mut self, rng: Range<Key>, limit: u32) -> Result<Vec<Key>, Error> {
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxClosed);
		}
		// Get the iteration direction
		let dir = Some(Direction::Prev);
		// Convert the range to JavaScript for reverse scanning
		// For reverse order, we need to swap the start and end bounds
		let rng = KeyRange::bound(&rng.end.convert(), &rng.start.convert(), None, Some(true));
		let rng = rng.map_err(|e| Error::IndexedDbError(e.to_string()))?;
		// Scan the keys in reverse order
		let res = self.datastore.as_ref().unwrap().scan(Some(rng), Some(limit), None, dir).await?;
		let res = res.into_iter().map(|(k, _)| k.convert()).collect();
		// Return result
		Ok(res)
	}

	/// Retrieve a range of key-value pairs from the databases
	pub async fn scan(&mut self, rng: Range<Key>, limit: u32) -> Result<Vec<(Key, Val)>, Error> {
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxClosed);
		}
		// Get the iteration direction
		let dir = Some(Direction::Next);
		// Convert the range to JavaScript
		let rng = KeyRange::bound(&rng.start.convert(), &rng.end.convert(), None, Some(true));
		let rng = rng.map_err(|e| Error::IndexedDbError(e.to_string()))?;
		// Scan the keys
		let res = self.datastore.as_ref().unwrap().scan(Some(rng), Some(limit), None, dir).await?;
		let res = res.into_iter().map(|(k, v)| (k.convert(), v.convert())).collect();
		// Return result
		Ok(res)
	}

	/// Retrieve a range of key-value pairs from the databases in reverse order
	pub async fn scanr(&mut self, rng: Range<Key>, limit: u32) -> Result<Vec<(Key, Val)>, Error> {
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxClosed);
		}
		// Get the iteration direction
		let dir = Some(Direction::Prev);
		// Convert the range to JavaScript for reverse scanning
		// For reverse order, we need to swap the start and end bounds
		let rng = KeyRange::bound(&rng.end.convert(), &rng.start.convert(), None, Some(true));
		let rng = rng.map_err(|e| Error::IndexedDbError(e.to_string()))?;
		// Scan the keys in reverse order
		let res = self.datastore.as_ref().unwrap().scan(Some(rng), Some(limit), None, dir).await?;
		let res = res.into_iter().map(|(k, v)| (k.convert(), v.convert())).collect();
		// Return result
		Ok(res)
	}

	/// Set a savepoint in the transaction for partial rollback
	/// This method is stackable and can be called multiple times with
	/// corresponding calls to `rollback_to_savepoint`
	pub async fn set_savepoint(&mut self) -> Result<(), Error> {
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxClosed);
		}
		// Check to see if transaction is writable
		if !self.write {
			return Err(Error::TxNotWritable);
		}
		// Create a new savepoint with current operations
		self.savepoints.push(Savepoint {
			operations: std::mem::take(&mut self.operations),
		});
		// Continue
		Ok(())
	}

	/// Rollback the transaction to the most recently set savepoint
	/// After calling this method, subsequent modifications within this
	/// transaction can be rolled back by calling `rollback_to_savepoint`
	/// again if there are more savepoints in the stack
	pub async fn rollback_to_savepoint(&mut self) -> Result<(), Error> {
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxClosed);
		}
		// Check to see if transaction is writable
		if !self.write {
			return Err(Error::TxNotWritable);
		}
		// Check if there are any savepoints
		if self.savepoints.is_empty() {
			return Err(Error::NoSavepoint);
		}
		// Get the most recent savepoint
		let savepoint = self.savepoints.pop().unwrap();
		// Execute undo operations in reverse order
		for op in self.operations.iter().rev() {
			match op {
				// Delete the key that was inserted
				Operation::DeleteKey(key) => {
					self.datastore.as_ref().unwrap().delete(key.clone().convert()).await?;
				}
				// Restore the previous value
				Operation::RestoreValue(key, val) => {
					self.datastore
						.as_ref()
						.unwrap()
						.put(&val.clone().convert(), Some(&key.clone().convert()))
						.await?;
				}
				// Restore the deleted key
				Operation::RestoreDeleted(key, val) => {
					self.datastore
						.as_ref()
						.unwrap()
						.put(&val.clone().convert(), Some(&key.clone().convert()))
						.await?;
				}
			}
		}
		// Restore the savepoint's operations as the current ones
		self.operations = savepoint.operations;
		// Continue
		Ok(())
	}
}
