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

//! This module stores the database error types.

use thiserror::Error;

/// The errors which can be emitted from a database.
#[derive(Error, Debug)]
pub enum Error {
	#[error("Can not open database")]
	DbError,

	#[error("Can not open transaction")]
	TxError,

	#[error("Transaction is closed")]
	TxClosed,

	#[error("Transaction is not writable")]
	TxNotWritable,

	#[error("Key being inserted already exists")]
	KeyAlreadyExists,

	#[error("Value being checked was not correct")]
	ValNotExpectedValue,

	/// No savepoint has been set.
	#[error("No savepoint has been set")]
	NoSavepoint,

	#[error("An IndexedDB error occured: {0}")]
	IndexedDbError(String),
}

impl From<rexie::Error> for Error {
	fn from(v: rexie::Error) -> Self {
		Error::IndexedDbError(v.to_string())
	}
}
