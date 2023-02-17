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

//! Metrics for materialize systems.
//!
//! The idea here is that each subsystem keeps its metrics in a scoped-to-it struct, which gets
//! registered (once) to the server's (or a test's) prometheus registry.
//!
//! Instead of using prometheus's (very verbose) metrics definitions, we rely on type inference to
//! reduce the verbosity a little bit. A typical subsystem will look like the following:
//!
//! ```rust
//! # use mz_ore::metrics::{MetricsRegistry, IntCounter};
//! # use mz_ore::metric;
//! #[derive(Debug, Clone)] // Note that prometheus metrics can safely be cloned
//! struct Metrics {
//!     pub bytes_sent: IntCounter,
//! }
//!
//! impl Metrics {
//!     pub fn register_into(registry: &MetricsRegistry) -> Metrics {
//!         Metrics {
//!             bytes_sent: registry.register(metric!(
//!                 name: "mz_pg_sent_bytes",
//!                 help: "total number of bytes sent here",
//!             )),
//!         }
//!     }
//! }
//! ```

use std::fmt;
use std::hash::Hash;
use std::sync::atomic::{AtomicI64, AtomicU64};
use std::sync::Arc;

use prometheus_client::encoding::MetricEncoder;
use prometheus_client::encoding::{EncodeCounterValue, EncodeGaugeValue, EncodeMetric};
use prometheus_client::encoding::{EncodeLabelKey, EncodeLabelSet, EncodeLabelValue};
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::family::MetricConstructor;
use prometheus_client::metrics::MetricType;
use prometheus_client::metrics::TypedMetric;
use prometheus_client::registry::Registry;

mod delete_on_drop;

pub use delete_on_drop::*;
use std::fmt::{Debug, Formatter};

/// Define a metric for use in materialize.
#[macro_export]
macro_rules! metric {
    (
        name: $name:expr,
        help: $help:expr
        $(,)?
    ) => {{
        #[allow(unused_mut)]
        let mut mk_opts = $crate::metrics::MakeCollectorOpts {
            name: $name,
            help: $help,
            more: (),
            buckets: None,
        };
        mk_opts
    }};
    (
        name: $name:expr,
        help: $help:expr
        $(, var_labels: [ $($vl_name:expr),* ])?
        $(, buckets: $bk_name:expr)?
        $(,)?
    ) => {{
        let var_labels = vec![
            $(
                $($vl_name.into(),)*
            )?];
        #[allow(unused_mut)]
        let mut mk_opts = $crate::metrics::MakeCollectorOpts {
            name: $name,
            help: $help,
            more: var_labels,
            buckets: None,
        };
        // Set buckets if passed
        $(mk_opts.buckets = Some($bk_name);)*
        mk_opts
    }}
}

/// Options for MakeCollector. This struct should be instantiated using the metric macro.
#[derive(Debug, Clone)]
pub struct MakeCollectorOpts<More> {
    pub name: &'static str,
    pub help: &'static str,
    /// Common Prometheus options
    pub more: More,
    /// Buckets to be used with Histogram and HistogramVec. Must be set to create Histogram types
    /// and must not be set for other types.
    pub buckets: Option<Vec<f64>>,
}

/// The materialize metrics registry.
#[derive(Debug, Clone)]
pub struct MetricsRegistry {
    inner: Arc<parking_lot::RwLock<Registry>>,
}

/// A wrapper for metrics to require delete on drop semantics
///
/// The wrapper behaves like regular metrics but only provides functions to create delete-on-drop
/// variants. This way, no metrics of this type can be leaked.
///
/// In situations where the delete-on-drop behavior is not desired or in legacy code, use the raw
/// variants of the metrics, as defined in [self::raw].
#[derive(Clone)]
pub struct DeleteOnDropWrapper<M> {
    inner: M,
}

impl<M: MakeMetric + Debug> Debug for DeleteOnDropWrapper<M> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

impl<M: TypedMetric> TypedMetric for DeleteOnDropWrapper<M> {
    const TYPE: MetricType = <M as TypedMetric>::TYPE;
}

impl<M: TypedMetric + EncodeMetric> EncodeMetric for DeleteOnDropWrapper<M> {
    fn encode(&self, encoder: MetricEncoder) -> Result<(), std::fmt::Error> {
        self.inner.encode(encoder)
    }

    fn metric_type(&self) -> MetricType {
        <Self as TypedMetric>::TYPE
    }
}

impl<M: TypedMetric + MakeMetric> MakeMetric for DeleteOnDropWrapper<M> {
    type Options = M::Options;

    fn make_metric(opts: Self::Options) -> Self {
        DeleteOnDropWrapper {
            inner: M::make_metric(opts),
        }
    }
}

impl<M: LabelBuilder> LabelBuilder for DeleteOnDropWrapper<M> {
    type Metric = M::Metric;
    type LabelSet = M::LabelSet;

    fn get_or_create(&self, label_set: &Self::LabelSet) -> Self::Metric {
        self.inner.get_or_create(label_set)
    }

    fn remove(&self, label_set: &Self::LabelSet) -> bool {
        self.inner.remove(label_set)
    }
}

#[derive(Clone, Debug)]
pub struct VecFamilyWithLabelKeys<LK, LV, M, C = fn() -> M> {
    family: Family<Vec<(LK, LV)>, M, C>,
    label_keys: Vec<LK>,
}

impl<LV: Clone + Hash + Eq, LK: Clone + Hash + Eq, M, C: MetricConstructor<M>>
    VecFamilyWithLabelKeys<LK, LV, M, C>
{
    pub fn get_or_create(&self, label_values: &[LV]) -> parking_lot::MappedRwLockReadGuard<'_, M> {
        // lalala
        let labels: Vec<(LK, LV)> = self
            .label_keys
            .iter()
            .zip(label_values.iter())
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        self.family.get_or_create(&labels)
    }

    pub fn with_label_values(&self, label_values: &[LV]) -> M
    where
        M: Clone,
    {
        self.get_or_create(label_values).clone()
    }

    pub fn remove(&self, label_values: &[LV]) -> bool {
        // lalala
        let labels: Vec<(LK, LV)> = self
            .label_keys
            .iter()
            .zip(label_values.iter())
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        self.family.remove(&labels)
    }
}

impl<LK, LV, M: TypedMetric, C> TypedMetric for VecFamilyWithLabelKeys<LK, LV, M, C> {
    const TYPE: MetricType = <M as TypedMetric>::TYPE;
}

impl<LK, LV, M, C> EncodeMetric for VecFamilyWithLabelKeys<LK, LV, M, C>
where
    LK: Clone + std::hash::Hash + Eq + EncodeLabelKey,
    LV: Clone + std::hash::Hash + Eq + EncodeLabelValue,
    M: EncodeMetric + TypedMetric,
    C: MetricConstructor<M>,
{
    fn encode(&self, mut encoder: MetricEncoder) -> Result<(), std::fmt::Error> {
        self.family.encode(encoder)
    }

    fn metric_type(&self) -> MetricType {
        <Self as TypedMetric>::TYPE
    }
}

impl<'a, M, C> LabelBuilder for VecFamilyWithLabelKeys<String, String, M, C>
where
    C: MetricConstructor<M> + Clone,
    M: Clone,
{
    type Metric = M;
    type LabelSet = Vec<String>;

    fn get_or_create(&self, label_set: &Self::LabelSet) -> Self::Metric {
        self.with_label_values(&label_set.into_label_set())
    }

    fn remove(&self, label_set: &Self::LabelSet) -> bool {
        self.remove(&label_set.into_label_set())
    }
}

impl<S: Clone + Hash + Eq, M, C: MetricConstructor<M>> LabelBuilder for Family<S, M, C>
where
    M: Clone,
    C: MetricConstructor<M>,
    C: Clone,
{
    type Metric = M;
    type LabelSet = S;

    fn get_or_create(&self, label_set: &Self::LabelSet) -> Self::Metric {
        self.get_or_create(label_set).clone()
    }

    fn remove(&self, label_set: &Self::LabelSet) -> bool {
        self.remove(label_set)
    }
}

use prometheus_client::metrics::{counter::Counter, gauge::Gauge, histogram::Histogram};

// JUST METRICS
/// The unsigned integer version of [`Gauge`]. Provides better performance if
/// metric values are all unsigned integers.
pub type UIntGauge = Gauge<u64, AtomicU64>;
pub type IntCounter = Counter;

// FAMILIES
/// Delete-on-drop shadow of Prometheus [prometheus::CounterVec].
pub type CounterVec =
    DeleteOnDropWrapper<VecFamilyWithLabelKeys<String, String, Counter<f64, AtomicU64>>>;
/// Delete-on-drop shadow of Prometheus [prometheus::Gauge].
pub type GaugeVec =
    DeleteOnDropWrapper<VecFamilyWithLabelKeys<String, String, Gauge<f64, AtomicU64>>>;
/// Delete-on-drop shadow of Prometheus [prometheus::HistogramVec].
pub type HistogramVec = DeleteOnDropWrapper<VecFamilyWithLabelKeys<String, String, Histogram>>;
/// Delete-on-drop shadow of Prometheus [prometheus::IntCounterVec].
pub type IntCounterVec = DeleteOnDropWrapper<raw::IntCounterVec>;
/// Delete-on-drop shadow of Prometheus [prometheus::IntGaugeVec].
pub type IntGaugeVec = DeleteOnDropWrapper<raw::IntGaugeVec>;
/// Delete-on-drop shadow of Prometheus [raw::UIntGaugeVec].
pub type UIntGaugeVec = DeleteOnDropWrapper<raw::UIntGaugeVec>;

pub type DeleteOnDropCounter<'a> = DeleteOnDropMetric<'a, IntCounterVec>;
pub type DeleteOnDropUGauge<'a> = DeleteOnDropMetric<'a, UIntGaugeVec>;
pub type DeleteOnDropIGauge<'a> = DeleteOnDropMetric<'a, IntGaugeVec>;

/// Access to non-delete-on-drop vector types
pub mod raw {
    // lalala
    pub type IntCounterVec = super::VecFamilyWithLabelKeys<String, String, super::Counter>;

    /// The unsigned integer version of [`GaugeVec`](prometheus::GaugeVec).
    /// Provides better performance if metric values are all unsigned integers.
    pub type UIntGaugeVec =
        super::VecFamilyWithLabelKeys<String, String, super::Gauge<u64, super::AtomicU64>>;

    pub type IntGaugeVec =
        super::VecFamilyWithLabelKeys<String, String, super::Gauge<i64, super::AtomicI64>>;
}

impl MetricsRegistry {
    /// Creates a new metrics registry.
    pub fn new() -> Self {
        MetricsRegistry {
            inner: Arc::new(parking_lot::RwLock::new(Registry::default())),
        }
    }

    /// Register a metric defined with the [`metric`] macro.
    pub fn register<M>(&self, opts: MakeCollectorOpts<M::Options>) -> M
    where
        M: MakeMetric,
    {
        let metric = M::make_metric(opts.more);
        self.inner
            .write()
            .register(opts.name, opts.help, metric.clone());
        metric
    }

    /*
    /// Registers a gauge whose value is computed when observed.
    pub fn register_computed_gauge<F, P>(
        &self,
        opts: MakeCollectorOpts,
        f: F,
    ) -> ComputedGenericGauge<P>
    where
        F: Fn() -> P::T + Send + Sync + 'static,
        P: Atomic + 'static,
    {
        let gauge = ComputedGenericGauge {
            gauge: GenericGauge::make_collector(opts),
            f: Arc::new(f),
        };
        self.inner.register(Box::new(gauge.clone())).unwrap();
        gauge
    }
    */

    /// Gather all the metrics from the metrics registry for reporting.
    ///
    /// See also [`prometheus::Registry::gather`].
    pub fn registry(&self) -> parking_lot::RwLockReadGuard<'_, Registry> {
        self.inner.read()
    }
}

use prometheus_client::metrics::counter::Atomic as CounterAtomic;
use prometheus_client::metrics::gauge::Atomic as GaugeAtomic;
use prometheus_client::registry::Metric;

/// A wrapper for creating prometheus metrics more conveniently.
///
/// Together with the [`metric`] macro, this trait is mainly used by [`MetricsRegistry`] and should
/// not normally be used outside the metric registration flow.
pub trait MakeMetric: Metric + Clone + 'static {
    type Options;
    /// Creates a new collector.
    fn make_metric(opts: Self::Options) -> Self;
}

impl<N, A> MakeMetric for Counter<N, A>
where
    A: CounterAtomic<N> + Send + Sync + Debug + Default + 'static,
    N: EncodeCounterValue + Debug + Clone + Send + Sync + 'static,
{
    type Options = ();

    fn make_metric(mk_opts: Self::Options) -> Self {
        Self::default()
    }
}

impl<N, A> MakeMetric for Gauge<N, A>
where
    A: GaugeAtomic<N> + Send + Sync + Debug + Default + 'static,
    N: EncodeGaugeValue + Debug + Clone + Send + Sync + 'static,
{
    type Options = ();

    fn make_metric(mk_opts: Self::Options) -> Self {
        Self::default()
    }
}

impl<M> MakeMetric for VecFamilyWithLabelKeys<String, String, M>
where
    M: TypedMetric + EncodeMetric + Send + Sync + Debug + Clone + Default + 'static,
{
    type Options = Vec<String>;
    fn make_metric(mk_opts: Self::Options) -> Self {
        VecFamilyWithLabelKeys {
            family: Default::default(),
            label_keys: mk_opts,
        }
    }
}

/*
impl MakeCollector for raw::HistogramVec {
    fn make_collector(mk_opts: MakeCollectorOpts) -> Self {
        assert!(mk_opts.buckets.is_some());
        let labels = mk_opts.opts.variable_labels.clone();
        let labels = &labels.iter().map(|x| x.as_str()).collect::<Vec<_>>();
        Self::new(
            HistogramOpts {
                common_opts: mk_opts.opts,
                buckets: mk_opts.buckets.unwrap(),
            },
            labels,
        )
        .expect("defining a histogram vec")
    }
}
*/

/*

/// A [`Gauge`] whose value is computed whenever it is observed.
pub struct ComputedGenericGauge<P>
where
    P: Atomic,
{
    gauge: GenericGauge<P>,
    f: Arc<dyn Fn() -> P::T + Send + Sync>,
}

impl<P> fmt::Debug for ComputedGenericGauge<P>
where
    P: Atomic + fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("ComputedGenericGauge")
            .field("gauge", &self.gauge)
            .finish_non_exhaustive()
    }
}

impl<P> Clone for ComputedGenericGauge<P>
where
    P: Atomic,
{
    fn clone(&self) -> ComputedGenericGauge<P> {
        ComputedGenericGauge {
            gauge: self.gauge.clone(),
            f: Arc::clone(&self.f),
        }
    }
}

impl<T> Collector for ComputedGenericGauge<T>
where
    T: Atomic,
{
    fn desc(&self) -> Vec<&prometheus::core::Desc> {
        self.gauge.desc()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        self.gauge.set((self.f)());
        self.gauge.collect()
    }
}

impl<P> ComputedGenericGauge<P>
where
    P: Atomic,
{
    /// Computes the current value of the gauge.
    pub fn get(&self) -> P::T {
        (self.f)()
    }
}

/// A [`ComputedGenericGauge`] for 64-bit floating point numbers.
pub type ComputedGauge = ComputedGenericGauge<AtomicF64>;

/// A [`ComputedGenericGauge`] for 64-bit signed integers.
pub type ComputedIntGauge = ComputedGenericGauge<AtomicI64>;

/// A [`ComputedGenericGauge`] for 64-bit unsigned integers.
pub type ComputedUIntGauge = ComputedGenericGauge<AtomicU64>;

#[cfg(test)]
*/
mod tests;
