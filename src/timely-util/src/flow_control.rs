// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Operators and abstractions for flow control.

use timely::dataflow::scopes::Child;
use timely::dataflow::scopes::ScopeParent;

use crate::order::Partitioned;

pub mod quota_feedback;
pub mod streaming_chunks;

/// A `T: Timestamp` broken into granular chunks, with each chunk
/// partitioned by worker id.
pub type Subtime<T> = (T, Partitioned<usize, usize>);

/// A child of `T: Timestamp` that is
pub type GranularChild<'c, G> = Child<'c, G, Subtime<<G as ScopeParent>::Timestamp>>;
