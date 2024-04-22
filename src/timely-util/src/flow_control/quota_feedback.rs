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

//! `streaming_chunks`, an operator for granular-izing input data.

use std::convert::Infallible;
use std::ops::AddAssign;

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::Scope;
use timely::dataflow::Stream;
use timely::progress::timestamp::Timestamp;
use timely::progress::Antichain;

use super::GranularChild;

/// Flow control configuration.
#[derive(Debug)]
pub struct FlowControl<G: Scope> {
    /// Stream providing in-flight frontier updates.
    ///
    /// As implied by its type, this stream never emits data, only progress updates.
    ///
    /// TODO: Replace `Infallible` with `!` once the latter is stabilized.
    pub progress_stream: Stream<G, Infallible>,
}

pub trait Quotable<Q: Copy + PartialOrd + Ord + AddAssign<Q>> {
    fn quota(&self) -> Q;
}
pub trait Quota<Q: Copy> {
    fn take_quota(&mut self, q: Q);
    fn retire_quota(&mut self, q: Q);
}

pub fn quota_feedback<'c, G, D, Q, Qut>(
    scope: G,
    child: GranularChild<'c, G>,
    name: String,
    data: &Stream<GranularChild<'c, G>, D>,
    flow_control: FlowControl<GranularChild<'c, G>>,
    mut quota: Qut,
) -> Stream<GranularChild<'c, G>, D>
where
    G: Scope,
    G::Timestamp: Timestamp + Ord,
    D: timely::Data,
    D: Quotable<Q>,
    Q: Copy + PartialOrd + Ord + AddAssign<Q> + Default + 'static + std::fmt::Debug,
    Qut: Quota<Q> + 'static,
{
    let worker_index = scope.index();

    let mut builder = OperatorBuilder::new(name, child.clone());

    let (mut data_output, data_stream) = builder.new_output();

    let mut data_input = builder.new_input(data, Pipeline);
    let _flow_input = builder.new_input_connection(
        &flow_control.progress_stream,
        Pipeline,
        vec![Antichain::new()],
    );

    builder.build(move |caps| {
        drop(caps);

        let mut vector = Vec::new();
        let mut quotas = Vec::new();
        move |frontiers| {
            // Forward data, with times delayed to the subtime. Subtimes
            // are incremented per-container of data.

            // Forward the data, claiming quota.
            let mut data_output = data_output.activate();
            while let Some((cap, data)) = data_input.next() {
                data.swap(&mut vector);

                let mut q: Q = Default::default();
                for d in vector.iter() {
                    q += d.quota();
                }

                quota.take_quota(q);
                quotas.push((cap.time().clone(), q));

                let mut session = data_output.session(&cap);
                session.give_container(&mut vector);
            }

            let feedback_upper = frontiers[1].frontier();
            tracing::trace!(
                "{}, feedback:{:?}, quotas:{:?}",
                worker_index,
                feedback_upper.to_owned(),
                quotas
            );

            quotas.retain(|(t, q)| {
                if !feedback_upper.less_equal(t) {
                    quota.retire_quota(*q);
                    false
                } else {
                    true
                }
            });
            tracing::trace!(
                "{}, AFTER feedback:{:?}, quotas:{:?}",
                worker_index,
                feedback_upper.to_owned(),
                quotas
            );
        }
    });
    data_stream
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::builder_async::OperatorBuilder as AsyncOperatorBuilder;
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::sync::Arc;
    use timely::dataflow::operators::core::feedback::{ConnectLoop, Feedback};
    use timely::dataflow::operators::Probe;
    use timely::PartialOrder;
    use timely::{CommunicationConfig, Config};
    use tokio::sync::Notify;

    use crate::flow_control::streaming_chunks::{streaming_chunks, tests::input_operator};
    use crate::flow_control::Subtime;
    use crate::order::Partitioned;
    use Step::*;

    /// Test the general case.
    #[test]
    fn test_flow_quota() {
        runner(
            2,
            vec![
                TsChunk {
                    containers: vec![vec![Chunk {}], vec![Chunk {}]],
                },
                ExactFrontier {
                    frontier: Antichain::from(vec![
                        (0, Partitioned::new_singleton(0, 2)),
                        (0, Partitioned::new_singleton(1, 2)),
                    ]),
                },
                AssertOutstandingQuota {
                    worker_id: 0,
                    quota: 2,
                },
                AssertOutstandingQuota {
                    worker_id: 1,
                    quota: 2,
                },
                DowngradeConsumer {
                    worker_id: 0,
                    containers: 2,
                },
                ExactFeedbackFrontier {
                    frontier: Antichain::from(vec![
                        (0, Partitioned::new_singleton(0, 2)),
                        (0, Partitioned::new_singleton(1, 0)),
                    ]),
                },
                AssertOutstandingQuota {
                    worker_id: 0,
                    quota: 0,
                },
                AssertOutstandingQuota {
                    worker_id: 1,
                    quota: 2,
                },
                DowngradeConsumer {
                    worker_id: 1,
                    containers: 2,
                },
                ExactFeedbackFrontier {
                    frontier: Antichain::from(vec![
                        (0, Partitioned::new_singleton(0, 2)),
                        (0, Partitioned::new_singleton(1, 2)),
                    ]),
                },
                AssertOutstandingQuota {
                    worker_id: 0,
                    quota: 0,
                },
                AssertOutstandingQuota {
                    worker_id: 1,
                    quota: 0,
                },
            ],
        );
    }

    #[derive(Clone)]
    pub struct Chunk {}

    impl Quotable<usize> for Chunk {
        fn quota(&self) -> usize {
            1
        }
    }

    #[derive(Clone)]
    struct TestQuota {
        shared: Rc<RefCell<usize>>,
    }

    impl TestQuota {
        fn new() -> Self {
            Self {
                shared: Default::default(),
            }
        }

        fn get_quota(&self) -> usize {
            self.shared.borrow().clone()
        }
    }

    impl Quota<usize> for TestQuota {
        fn take_quota(&mut self, q: usize) {
            *self.shared.borrow_mut() += q;
        }
        fn retire_quota(&mut self, q: usize) {
            *self.shared.borrow_mut() -= q;
        }
    }

    /// A step in a test
    #[derive(Clone)]
    pub enum Step {
        /// Emit the following chunks of data, increasing the emission ts each time.
        ///
        /// The frontier is downgraded on the next `TsChunk`.
        TsChunk { containers: Vec<Vec<Chunk>> },
        /// Assert that the _streaming_chunks frontier is EXACTLY this frontier.
        ExactFrontier { frontier: Antichain<Subtime<u64>> },
        /// Downgrade the consumer with the feedback frontier to this exact frontier.
        DowngradeConsumer { worker_id: usize, containers: usize },
        /// Assert that the _streaming_chunks frontier is EXACTLY this frontier.
        ExactFeedbackFrontier { frontier: Antichain<Subtime<u64>> },
        /// Assert that, on this worker, we see `values_per_ts` values per ts, and that there
        /// are `unique_times` granular timestamps.
        AssertOutstandingQuota { worker_id: usize, quota: usize },
    }

    /// A runner for testing `streaming_chunks` using a list of `Step`'s.
    fn runner(workers: usize, input: Vec<Step>) {
        let config = Config {
            communication: CommunicationConfig::Process(workers),
            worker: Default::default(),
        };
        timely::execute::execute(config, move |worker| {
            let my_worker_id = worker.index();
            let steps = input.clone();
            let input_commands: Vec<_> = input
                .clone()
                .into_iter()
                .filter_map(|f| match f {
                    Step::TsChunk { containers } => Some(containers),
                    _ => None,
                })
                .collect();
            let consumer_commands: Vec<_> = input
                .clone()
                .into_iter()
                .filter_map(|f| match f {
                    Step::DowngradeConsumer {
                        worker_id,
                        containers,
                    } if worker_id == my_worker_id => Some(containers),
                    _ => None,
                })
                .collect();

            let test_quota = TestQuota::new();
            let assert_quota = test_quota.clone();

            let (stream_probe, feedback_probe, notify, consumer_notify) = worker
                .dataflow::<u64, _, _>(move |scope| {
                    let (data, notify) =
                        input_operator(scope.clone(), input_commands.into_iter(), false);
                    let inner_s = scope.clone();
                    let (stream_probe, feedback_probe, consumer_notify) =
                        scope.scoped::<_, _, _>("granular", move |child| {
                            let stream_data = streaming_chunks(
                                inner_s.clone(),
                                child.clone(),
                                "test".to_string(),
                                &data,
                            );

                            let (feedback_handle, feedback) = child.feedback(Default::default());

                            let output = quota_feedback(
                                inner_s.clone(),
                                child.clone(),
                                "test".to_string(),
                                &stream_data,
                                FlowControl {
                                    progress_stream: feedback,
                                },
                                test_quota,
                            );
                            let stream_probe = output.probe();

                            let (output, consumer_notify) = consumer_operator(
                                child.clone(),
                                consumer_commands.into_iter(),
                                &output,
                            );
                            output.connect_loop(feedback_handle);

                            (stream_probe, output.probe(), consumer_notify)
                        });
                    (stream_probe, feedback_probe, notify, consumer_notify)
                });

            for step in steps {
                match step {
                    TsChunk { .. } => notify.notify_one(),
                    ExactFrontier { frontier } => {
                        while stream_probe.with_frontier(|front| {
                            eprintln!("current frontier: {front:?}");
                            PartialOrder::less_than(&front, &frontier.borrow())
                        }) {
                            worker.step();
                            std::thread::sleep(std::time::Duration::from_millis(25));
                        }
                        stream_probe.with_frontier(|front| {
                            eprintln!("found frontier: {front:?}");
                            assert_eq!(front, frontier.borrow());
                        });
                    }
                    ExactFeedbackFrontier { frontier } => {
                        while feedback_probe.with_frontier(|front| {
                            eprintln!("Xcurrent frontier: {front:?}");
                            PartialOrder::less_than(&front, &frontier.borrow())
                        }) {
                            worker.step();
                            std::thread::sleep(std::time::Duration::from_millis(25));
                        }
                        feedback_probe.with_frontier(|front| {
                            eprintln!("Xfound frontier: {front:?}");
                            assert_eq!(front, frontier.borrow());
                        });
                    }
                    DowngradeConsumer {
                        worker_id,
                        containers: _,
                    } => {
                        if my_worker_id == worker_id {
                            consumer_notify.notify_one();
                        }
                    }
                    AssertOutstandingQuota { worker_id, quota } => {
                        if my_worker_id == worker_id {
                            assert_eq!(quota, assert_quota.get_quota());
                        }
                    }
                }
            }
            notify.notify_one();
            consumer_notify.notify_one();
        })
        .unwrap();

        pub(crate) fn consumer_operator<
            G: Scope<Timestamp = Subtime<u64>>,
            I: Iterator<Item = usize> + 'static,
        >(
            scope: G,
            mut commands: I,
            stream: &Stream<G, Chunk>,
        ) -> (Stream<G, Infallible>, Arc<Notify>) {
            let mut iterator = AsyncOperatorBuilder::new("input".to_string(), scope);

            let (output_handle, output) = iterator.new_output::<Vec<Infallible>>();
            let mut input = iterator.new_input_for(stream, Pipeline, &output_handle);

            let notif = Arc::new(Notify::new());
            let in_notif = Arc::clone(&notif);
            iterator.build(|caps| async move {
                drop(caps);

                loop {
                    in_notif.notified().await;
                    if let Some(chunks) = commands.next() {
                        let mut found = 0;
                        while found < chunks {
                            if let Some(crate::builder_async::Event::Data(cap, ..)) =
                                input.next().await
                            {
                                eprintln!("HUH:{:?}", cap.time());
                                found += 1;
                            }
                        }
                    } else {
                        // final frontier UGH
                        break;
                    }
                }
            });

            (output, notif)
        }
    }
    /// Basic validation to ensure `Partitioned` frontiers make sense.
    #[test]
    fn test_partitioned_frontier() {
        let ts: Subtime<u64> = (10, Partitioned::new_singleton(3, 5));

        let everything_ahead = Antichain::from(vec![
            (11, Partitioned::new_singleton(0, 0)),
            (11, Partitioned::new_singleton(1, 0)),
            (11, Partitioned::new_singleton(2, 0)),
            (11, Partitioned::new_singleton(3, 0)),
            (11, Partitioned::new_singleton(4, 0)),
        ]);
        // The frontier is in advance of the ts.
        assert!(!everything_ahead.less_equal(&ts));

        let three_behind = Antichain::from(vec![
            (11, Partitioned::new_singleton(0, 0)),
            (11, Partitioned::new_singleton(1, 0)),
            (11, Partitioned::new_singleton(2, 0)),
            (10, Partitioned::new_singleton(3, 0)),
            (11, Partitioned::new_singleton(4, 0)),
        ]);
        // The frontier is NOT in advance of the ts.
        assert!(three_behind.less_equal(&ts));

        let three_just_ahead = Antichain::from(vec![
            (11, Partitioned::new_singleton(0, 0)),
            (11, Partitioned::new_singleton(1, 0)),
            (11, Partitioned::new_singleton(2, 0)),
            (10, Partitioned::new_singleton(3, 6)),
            (11, Partitioned::new_singleton(4, 0)),
        ]);
        // The frontier is in advance of the ts.
        assert!(!three_just_ahead.less_equal(&ts));

        let close = Antichain::from(vec![
            (10, Partitioned::new_singleton(0, 100)),
            (10, Partitioned::new_singleton(1, 100)),
            (10, Partitioned::new_singleton(2, 100)),
            (10, Partitioned::new_singleton(3, 0)),
            (10, Partitioned::new_singleton(4, 100)),
        ]);
        // The frontier is NOT in advance of the ts.
        assert!(close.less_equal(&ts));

        let closer = Antichain::from(vec![
            (10, Partitioned::new_singleton(0, 100)),
            (10, Partitioned::new_singleton(1, 100)),
            (10, Partitioned::new_singleton(2, 100)),
            (10, Partitioned::new_singleton(3, 5)),
            (10, Partitioned::new_singleton(4, 100)),
        ]);
        // The frontier is NOT in advance of the ts.
        assert!(closer.less_equal(&ts));

        let just_enough = Antichain::from(vec![
            (10, Partitioned::new_singleton(0, 100)),
            (10, Partitioned::new_singleton(1, 100)),
            (10, Partitioned::new_singleton(2, 100)),
            (10, Partitioned::new_singleton(3, 6)),
            (10, Partitioned::new_singleton(4, 100)),
        ]);
        // The frontier is in advance of the ts.
        assert!(!just_enough.less_equal(&ts));

        let not_even_close = Antichain::from(vec![
            (9, Partitioned::new_singleton(0, usize::MAX)),
            (9, Partitioned::new_singleton(1, usize::MAX)),
            (9, Partitioned::new_singleton(2, usize::MAX)),
            (9, Partitioned::new_singleton(3, usize::MAX)),
            (9, Partitioned::new_singleton(4, usize::MAX)),
        ]);
        // The frontier is in advance of the ts.
        assert!(not_even_close.less_equal(&ts));

        // Proof that we might want to specialize the `Partitioned` to not accept an arbitrary order
        // between worker ids.
        let broken_frontier = Antichain::from(vec![(10, Partitioned::new_singleton(4, 6))]);
        // The frontier is in advance of the ts.
        assert!(!broken_frontier.less_equal(&ts));
        let broken_frontier2 = Antichain::from(vec![(10, Partitioned::new_singleton(2, 6))]);
        // The frontier is in advance of the ts.
        assert!(!broken_frontier2.less_equal(&ts));
    }
}
