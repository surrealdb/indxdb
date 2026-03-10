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

use std::rc::Rc;

use crate::err::Error;
use crate::tx::Transaction;
use rexie::ObjectStore;
use rexie::Rexie;

/// A transactional browser-based database
pub struct Database {
	/// The underlying IndexedDB datastore, wrapped in Rc so transactions can
	/// hold a reference for opening fresh IDB transactions.
	pub(crate) datastore: Rc<Rexie>,
}

impl Database {
	/// Create a new transactional IndexedDB database
	pub async fn new(path: &str) -> Result<Self, Error> {
		let store = ObjectStore::new("kv");
		match Rexie::builder(path).version(1).add_object_store(store).build().await {
			Ok(db) => Ok(Database {
				datastore: Rc::new(db),
			}),
			Err(_) => Err(Error::DbError),
		}
	}

	/// Start a new transaction.
	///
	/// The returned transaction buffers all writes in memory. Reads open
	/// short-lived read-only IDB transactions on demand. On `commit()`, a
	/// fresh read-write IDB transaction is opened and all buffered mutations
	/// are flushed in one synchronous batch.
	pub async fn begin(&self, write: bool) -> Result<Transaction, Error> {
		Ok(Transaction::new(Rc::clone(&self.datastore), write))
	}
}
