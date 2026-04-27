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

use idb::builder::{DatabaseBuilder, ObjectStoreBuilder};
use idb::Database as IdbDatabase;

use crate::err::Error;
use crate::tx::Transaction;

/// A transactional browser-based database
pub struct Database {
	/// The underlying IndexedDB datastore.
	///
	/// Wrapped in `Rc` so transactions can hold a clone and open fresh
	/// short-lived IDB transactions on demand without taking exclusive
	/// ownership of the database handle.
	pub(crate) datastore: Rc<IdbDatabase>,
}

impl Database {
	/// Create a new transactional IndexedDB database.
	///
	/// Opens (or creates) an IndexedDB database with a single object store
	/// named `kv` at version `1`. If a database already exists at the path
	/// with a higher version, the open will fail; this matches the previous
	/// rexie-backed behaviour.
	pub async fn new(path: &str) -> Result<Self, Error> {
		let store = ObjectStoreBuilder::new("kv");
		let db = DatabaseBuilder::new(path).version(1).add_object_store(store).build().await?;
		Ok(Database {
			datastore: Rc::new(db),
		})
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
