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

use super::*;

#[test]
fn metrics_registry() {
    let reg = MetricsRegistry::new();
    let counter: IntCounter = reg.register(metric!(
        name: "test_counter",
        help: "a counter for testing"
    ));
    counter.inc();

    let registry = reg.registry();
    let readings: Vec<_> = registry.iter().collect();
    assert_eq!(readings.len(), 1);
}

#[test]
fn thirdparty_metric_vecs() {
    let reg = MetricsRegistry::new();
    let cv: raw::IntCounterVec = reg.register(metric!(
        name: "test_counter_third_party",
        help: "an third_party counter for testing",
        var_labels: ["label"],
    ));
    let counter = cv.with_label_values(&["testing".to_string()]);
    counter.inc();
    let registry = reg.registry();
    let readings: Vec<_> = registry.iter().collect();
    assert_eq!(readings.len(), 1);
    assert_eq!(readings[0].0.name(), "test_counter_third_party");

    let mut buffer = String::new();
    prometheus_client::encoding::text::encode(&mut buffer, &reg.registry()).unwrap();

    let expected = "# HELP test_counter_third_party an third_party counter for testing.\n"
        .to_owned()
        + "# TYPE test_counter_third_party counter\n"
        + "test_counter_third_party_total{label=\"testing\"} 1\n"
        + "# EOF\n";
    assert_eq!(expected, buffer);
}
