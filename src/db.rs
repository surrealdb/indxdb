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

//! This module stores the core IndexedDB database type.

use crate::err::Error;
use crate::tx::Transaction;
use rexie::ObjectStore;
use rexie::Rexie;
use rexie::TransactionMode;

/// A transactional browser-based database
pub struct Database {
	/// The underlying IndexedDB datastore
	pub(crate) datastore: Rexie,
}

impl Database {
	/// Create a new transactional IndexedDB database
	pub async fn new(path: &str) -> Result<Self, Error> {
		// Create the new object store
		let store = ObjectStore::new("kv");
		// Build and initialise the database
		match Rexie::builder(path).version(1).add_object_store(store).build().await {
			Ok(db) => Ok(Database {
				datastore: db,
			}),
			Err(_) => Err(Error::DbError),
		}
	}

	/// Start a new read-only or writeable transaction
	pub async fn begin(&self, write: bool) -> Result<Transaction, Error> {
		// Set the transaction mode
		let mode = match write {
			true => TransactionMode::ReadWrite,
			false => TransactionMode::ReadOnly,
		};
		// Create the new transaction
		match self.datastore.transaction(&["kv"], mode) {
			Ok(tx) => match tx.store("kv") {
				Ok(st) => Ok(Transaction::new(tx, st, write)),
				Err(_) => Err(Error::TxError),
			},
			Err(_) => Err(Error::TxError),
		}
	}
}
