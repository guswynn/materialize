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

//! Support for metrics that get removed from their corresponding metrics vector when dropped.
//!
//! # Ownership & life times
//!
//! This kind of data type is realized by a struct that retains ownership of the _labels_ used to
//! create the spin-off metric. The created metric follows these rules:
//! * When passing references, the metric must not outlive the references to the labels used to create
//!   it: A `'static` slice of static strings means the metric is allowed to live for the `'static`
//!   lifetime as well.
//! * Metrics created from references to dynamically constructed labels can only live as long as those
//!   labels do.
//! * When using owned data (an extension over what Prometheus allows, which only lets you use
//!   references to refer to labels), the created metric is also allowed to live for `'static`.

use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::ops::Deref;

pub trait LabelBuilder: Clone {
    type Metric;
    type LabelSet;

    fn get_or_create(&self, label_set: &Self::LabelSet) -> Self::Metric;
    fn remove(&self, label_set: &Self::LabelSet) -> bool;

    fn get_typed_delete_on_drop_metric(
        &self,
        labels: Self::LabelSet,
    ) -> DeleteOnDropMetric<'static, Self> {
        DeleteOnDropMetric::from_labels(self.clone(), labels)
    }
}

/// An extension trait for types that are valid (or convertible into) prometheus labels:
/// slices/vectors of strings, and [`BTreeMap`]s.
/// lalala
pub trait PromLabelsExt<'a> {
    fn into_label_set(&self) -> Vec<String>;
}

impl<'a> PromLabelsExt<'a> for &'a [&'a str] {
    fn into_label_set(&self) -> Vec<String> {
        self.iter().map(|s| s.to_string()).collect()
    }
}

impl PromLabelsExt<'static> for Vec<String> {
    fn into_label_set(&self) -> Vec<String> {
        self.iter().cloned().collect()
    }
}

impl<'a> PromLabelsExt<'a> for Vec<&'a str> {
    fn into_label_set(&self) -> Vec<String> {
        self.iter().map(|s| s.to_string()).collect()
    }
}

/// A [`GenericCounter`] wrapper that deletes its labels from the vec when it is dropped
///
/// It adds a method to create a concrete metric from the vector that gets removed from the vector
/// when the concrete metric is dropped.
#[derive(Debug)]
pub struct DeleteOnDropMetric<'a, LB>
where
    LB: LabelBuilder,
{
    inner: LB::Metric,
    labels: LB::LabelSet,
    label_builder: LB,
    _phantom: &'a PhantomData<()>,
}

impl<'a, LB> Deref for DeleteOnDropMetric<'a, LB>
where
    LB: LabelBuilder,
{
    type Target = LB::Metric;
    fn deref(&self) -> &LB::Metric {
        &self.inner
    }
}

impl<'a, LB> Drop for DeleteOnDropMetric<'a, LB>
where
    LB: LabelBuilder,
{
    fn drop(&mut self) {
        eprintln!("GUS");
        if !dbg!(self.label_builder.remove(&self.labels)) {
            // ignore.
        }
    }
}

impl<LB> DeleteOnDropMetric<'static, LB>
where
    LB: LabelBuilder,
{
    fn from_labels(label_builder: LB, labels: LB::LabelSet) -> Self {
        Self {
            inner: label_builder.get_or_create(&labels),
            labels,
            label_builder,
            _phantom: &PhantomData,
        }
    }
}

impl<'a, LB> DeleteOnDropMetric<'static, LB>
where
    LB: LabelBuilder<LabelSet = Vec<String>>,
{
    fn from_label_ext<L: PromLabelsExt<'a>>(label_builder: LB, labels: L) -> Self {
        let labels = labels.into_label_set();
        Self {
            inner: label_builder.get_or_create(&labels),
            labels,
            label_builder,
            _phantom: &PhantomData,
        }
    }
}

/// Extension trait for all gauge metrics vectors.
///
/// It adds a method to create a concrete metric from the vector that gets removed from the vector
/// when the concrete metric is dropped.
pub trait MetricVecExt: LabelBuilder {
    /// Returns a counter that deletes its labels from this metrics vector when dropped.
    /// See [`DeleteOnDropCounter`] for a detailed description.
    fn get_delete_on_drop_metric<'a, L: PromLabelsExt<'a>>(
        &self,
        labels: L,
    ) -> DeleteOnDropMetric<'a, Self>;
}

impl<LB> MetricVecExt for LB
where
    LB: LabelBuilder<LabelSet = Vec<String>> + Clone,
{
    fn get_delete_on_drop_metric<'a, L: PromLabelsExt<'a>>(
        &self,
        labels: L,
    ) -> DeleteOnDropMetric<'a, Self> {
        DeleteOnDropMetric::from_label_ext(self.clone(), labels)
    }
}

#[cfg(test)]
mod test {
    use super::super::{
        DeleteOnDropCounter, DeleteOnDropIGauge, IntCounterVec, IntGaugeVec, MetricsRegistry,
    };
    use super::MetricVecExt;
    use crate::metric;

    #[test]
    fn dropping_counters() {
        let reg = MetricsRegistry::new();
        let vec: IntCounterVec = reg.register(metric!(
            name: "test_metric",
            help: "a test metric",
            var_labels: ["dimension"]));

        let dims: &[&str] = &["one"];
        let metric_1 = vec.get_delete_on_drop_metric(dims);
        metric_1.inc();

        let registry = reg.registry();
        let metrics: Vec<_> = registry.iter().collect();
        assert_eq!(metrics.len(), 1);
        let reported_vec = &metrics[0];
        assert_eq!(reported_vec.0.name(), "test_metric");
        drop(registry);

        drop(metric_1);
        let registry = reg.registry();
        let metrics: Vec<_> = dbg!(registry.iter().collect());
        let mut buffer = String::new();
        prometheus_client::encoding::text::encode(&mut buffer, &reg.registry()).unwrap();
        eprintln!("{}", buffer);
        assert_eq!(metrics.len(), 0);

        let string_labels: Vec<String> = ["owned"].iter().map(ToString::to_string).collect();
        struct Ownership {
            counter: DeleteOnDropCounter<'static>,
        }
        let metric_owned = Ownership {
            counter: vec.get_delete_on_drop_metric(string_labels),
        };
        metric_owned.counter.inc();

        let registry = reg.registry();
        let metrics: Vec<_> = registry.iter().collect();
        assert_eq!(metrics.len(), 1);
        let reported_vec = &metrics[0];
        assert_eq!(reported_vec.0.name(), "test_metric");

        drop(metric_owned);
        let registry = reg.registry();
        let metrics: Vec<_> = registry.iter().collect();
        assert_eq!(metrics.len(), 0);
    }

    #[test]
    fn dropping_gauges() {
        let reg = MetricsRegistry::new();
        let vec: IntGaugeVec = reg.register(metric!(
            name: "test_metric",
            help: "a test metric",
            var_labels: ["dimension"]));

        let dims: &[&str] = &["one"];
        let metric_1 = vec.get_delete_on_drop_metric(dims);
        metric_1.set(666);

        let registry = reg.registry();
        let metrics: Vec<_> = registry.iter().collect();
        let reported_vec = &metrics[0];
        assert_eq!(reported_vec.0.name(), "test_metric");

        drop(metric_1);
        let registry = reg.registry();
        let metrics: Vec<_> = registry.iter().collect();
        assert_eq!(metrics.len(), 0);

        let string_labels: Vec<String> = ["owned"].iter().map(ToString::to_string).collect();
        struct Ownership {
            gauge: DeleteOnDropIGauge<'static>,
        }
        let metric_owned = Ownership {
            gauge: vec.get_delete_on_drop_metric(string_labels),
        };
        metric_owned.gauge.set(666);

        let registry = reg.registry();
        let metrics: Vec<_> = registry.iter().collect();
        assert_eq!(metrics.len(), 1);
        let reported_vec = &metrics[0];
        assert_eq!(reported_vec.0.name(), "test_metric");

        drop(metric_owned);
        let registry = reg.registry();
        let metrics: Vec<_> = registry.iter().collect();
        assert_eq!(metrics.len(), 0);
    }
}
