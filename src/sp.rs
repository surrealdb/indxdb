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

//! This module stores the core IndexedDB savepoint type.

use crate::kv::Key;
use crate::kv::Val;

/// A savepoint state capturing operations that can be undone
#[derive(Debug, Clone)]
pub(crate) struct Savepoint {
	/// Operations that can be undone to rollback to this savepoint
	pub(crate) operations: Vec<Operation>,
}

/// An operation that can be undone during savepoint rollback
#[derive(Debug, Clone)]
pub(crate) enum Operation {
	/// Delete a key that was inserted
	DeleteKey(Key),
	/// Restore a key to its previous value
	RestoreValue(Key, Val),
	/// Restore a key that was deleted (insert it back)
	RestoreDeleted(Key, Val),
}
