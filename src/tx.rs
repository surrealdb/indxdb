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

use crate::err::Error;
use crate::kv::Convert;
use crate::kv::Key;
use crate::kv::Val;
use rexie::KeyRange;
use rexie::Store;
use rexie::Transaction;
use std::ops::Range;
use tokio::sync::OwnedMutexGuard;

pub struct Tx {
	// Is the transaction complete?
	pub(crate) ok: bool,
	// Is the transaction read+write?
	pub(crate) rw: bool,
	// The underlying database store
	pub(crate) st: Option<Store>,
	// The underlying database transaction
	pub(crate) tx: Option<Transaction>,
	// The underlying database write mutex
	pub(crate) lk: Option<OwnedMutexGuard<()>>,
}

impl Tx {
	// Create a transaction
	pub(crate) fn new(
		tx: Transaction,
		st: Store,
		write: bool,
		guard: Option<OwnedMutexGuard<()>>,
	) -> Tx {
		Tx {
			ok: false,
			rw: write,
			lk: guard,
			st: Some(st),
			tx: Some(tx),
		}
	}
	// Check if closed
	pub fn closed(&self) -> bool {
		self.ok
	}
	// Cancel a transaction
	pub async fn cancel(&mut self) -> Result<(), Error> {
		// Check to see if transaction is closed
		if self.ok == true {
			return Err(Error::TxClosed);
		}
		// Mark this transaction as done
		self.ok = true;
		// Abort the indexdb transaction
		self.tx.take().unwrap().abort().await?;
		// Unlock the database mutex
		if let Some(lk) = self.lk.take() {
			drop(lk);
		}
		// Continue
		Ok(())
	}
	// Commit a transaction
	pub async fn commit(&mut self) -> Result<(), Error> {
		// Check to see if transaction is closed
		if self.ok == true {
			return Err(Error::TxClosed);
		}
		// Check to see if transaction is writable
		if self.rw == false {
			return Err(Error::TxNotWritable);
		}
		// Mark this transaction as done
		self.ok = true;
		// Commit the indexdb transaction
		self.tx.take().unwrap().done().await?;
		// Unlock the database mutex
		if let Some(lk) = self.lk.take() {
			drop(lk);
		}
		// Continue
		Ok(())
	}
	// Check if a key exists
	pub async fn exi(&mut self, key: Key) -> Result<bool, Error> {
		// Check to see if transaction is closed
		if self.ok == true {
			return Err(Error::TxClosed);
		}
		// Check the key
		let res = self.st.as_ref().unwrap().get(&key.convert()).await?;
		// Return result
		Ok(res.is_undefined() == false)
	}
	// Fetch a key from the database
	pub async fn get(&mut self, key: Key) -> Result<Option<Val>, Error> {
		// Check to see if transaction is closed
		if self.ok == true {
			return Err(Error::TxClosed);
		}
		// Get the key
		let res = self.st.as_ref().unwrap().get(&key.convert()).await?;
		// Return result
		match res {
			v if v.is_undefined() => Ok(None),
			v => Ok(Some(v.convert())),
		}
	}
	// Insert or update a key in the database
	pub async fn set(&mut self, key: Key, val: Val) -> Result<(), Error> {
		// Check to see if transaction is closed
		if self.ok == true {
			return Err(Error::TxClosed);
		}
		// Check to see if transaction is writable
		if self.rw == false {
			return Err(Error::TxNotWritable);
		}
		// Set the key
		self.st.as_ref().unwrap().put(&val.convert(), Some(&key.convert())).await?;
		// Return result
		Ok(())
	}
	// Insert a key if it doesn't exist in the database
	pub async fn put(&mut self, key: Key, val: Val) -> Result<(), Error> {
		// Check to see if transaction is closed
		if self.ok == true {
			return Err(Error::TxClosed);
		}
		// Check to see if transaction is writable
		if self.rw == false {
			return Err(Error::TxNotWritable);
		}
		// Set the key
		self.st.as_ref().unwrap().add(&val.convert(), Some(&key.convert())).await?;
		// Return result
		Ok(())
	}
	// Insert a key if it doesn't exist in the database
	pub async fn putc(&mut self, key: Key, val: Val, chk: Option<Val>) -> Result<(), Error> {
		// Check to see if transaction is closed
		if self.ok == true {
			return Err(Error::TxClosed);
		}
		// Check to see if transaction is writable
		if self.rw == false {
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
	// Delete a key
	pub async fn del(&mut self, key: Key) -> Result<(), Error> {
		// Check to see if transaction is closed
		if self.ok == true {
			return Err(Error::TxClosed);
		}
		// Check to see if transaction is writable
		if self.rw == false {
			return Err(Error::TxNotWritable);
		}
		// Remove the key
		self.st.as_ref().unwrap().delete(&key.convert()).await?;
		// Return result
		Ok(())
	}
	// Delete a key
	pub async fn delc(&mut self, key: Key, chk: Option<Val>) -> Result<(), Error> {
		// Check to see if transaction is closed
		if self.ok == true {
			return Err(Error::TxClosed);
		}
		// Check to see if transaction is writable
		if self.rw == false {
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
	// Retrieve a range of keys from the databases
	pub async fn scan(&mut self, rng: Range<Key>, limit: u32) -> Result<Vec<(Key, Val)>, Error> {
		// Check to see if transaction is closed
		if self.ok == true {
			return Err(Error::TxClosed);
		}
		// Convert the range to JavaScript
		let rng = KeyRange::bound(&rng.start.convert(), &rng.end.convert(), false, true)?;
		// Scan the keys
		let res = self.st.as_ref().unwrap().get_all(Some(&rng), Some(limit), None, None).await?;
		let res = res.into_iter().map(|(k, v)| (k.convert(), v.convert())).collect();
		// Return result
		Ok(res)
	}
}
