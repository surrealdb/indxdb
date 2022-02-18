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
use crate::tx::Tx;
use rexie::ObjectStore;
use rexie::Rexie;
use rexie::TransactionMode;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct Db {
	pub(crate) lk: Arc<Mutex<()>>,
	pub(crate) ds: Rexie,
}

// Open a new database
pub async fn new(path: &str) -> Result<Db, Error> {
	match Rexie::builder(path).version(1).add_object_store(ObjectStore::new("kv")).build().await {
		Ok(db) => Ok(Db {
			lk: Arc::new(Mutex::new(())),
			ds: db,
		}),
		Err(_) => Err(Error::DbError),
	}
}

impl Db {
	// Start a new transaction
	pub async fn begin(&self, write: bool) -> Result<Tx, Error> {
		match write {
			true => match self.ds.transaction(&["kv"], TransactionMode::ReadWrite) {
				Ok(tx) => match tx.store("kv") {
					Ok(st) => Ok(Tx::new(tx, st, write, Some(self.lk.clone().lock_owned().await))),
					Err(_) => Err(Error::TxError),
				},
				Err(_) => Err(Error::TxError),
			},
			false => match self.ds.transaction(&["kv"], TransactionMode::ReadOnly) {
				Ok(tx) => match tx.store("kv") {
					Ok(st) => Ok(Tx::new(tx, st, write, None)),
					Err(_) => Err(Error::TxError),
				},
				Err(_) => Err(Error::TxError),
			},
		}
	}
}
