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

use js_sys::ArrayBuffer;
use js_sys::Uint8Array;
use wasm_bindgen::{JsCast, JsValue};

pub type Key = Vec<u8>;

pub type Val = Vec<u8>;

pub struct Kv {
	pub key: Key,
	pub val: Val,
}

pub trait Convert<T> {
	fn convert(self) -> T;
}

impl Convert<Vec<u8>> for JsValue {
	fn convert(self) -> Vec<u8> {
		if self.has_type::<ArrayBuffer>() {
			let v = Uint8Array::new(&self);
			return v.to_vec();
		}
		if self.has_type::<Uint8Array>() {
			let v: Uint8Array = self.unchecked_into();
			return v.to_vec();
		}
		vec![]
	}
}

impl Convert<JsValue> for Vec<u8> {
	fn convert(self) -> JsValue {
		JsValue::from(Uint8Array::from(&self[..]))
	}
}
