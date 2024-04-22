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

use std::collections::BTreeMap;

use differential_dataflow::collection::AsCollection;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::Collection;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::CapabilitySet;
use timely::dataflow::operators::Enter;
use timely::dataflow::Scope;
use timely::dataflow::Stream;
use timely::progress::timestamp::Timestamp;
use timely::progress::Antichain;
use timely::PartialOrder;

use super::{GranularChild, Subtime};
use crate::order::Partitioned;

/// Given an input stream of data within `T: Timestamp` scope, granular-ize the output
/// into distrinct timestamps and frontiers for each container of input data.
///
/// Note that the granular timestamp is not a simple `usize`, it is partitioned by worker id, so
/// each worker can make progress even if they emit different amounts of containers. This also
/// ensure downstream feedback edges have frontiers that can interpreted by individual workers,
/// without some sort of global coordination (well, in some sense, timely's progress tracking IS
/// this global coordination.
pub fn streaming_chunks<'c, G, D>(
    scope: G,
    child: GranularChild<'c, G>,
    name: String,
    data: &Stream<G, D>,
) -> Stream<GranularChild<'c, G>, D>
where
    G: Scope,
    G::Timestamp: Timestamp + Ord,
    D: timely::Data,
{
    let data = data.enter(&child);
    _streaming_chunks(scope, child, name, &data, |_, _| {})
}

/// The same as `streaming_chunks`, but for `Collection`s.
///
// TODO(guswynn): test this ugh
pub fn streaming_chunks_collection<'c, G, D, Diff>(
    scope: G,
    child: GranularChild<'c, G>,
    name: String,
    data: &Collection<G, D, Diff>,
) -> Collection<GranularChild<'c, G>, D, Diff>
where
    G: Scope,
    G::Timestamp: Timestamp + Ord,
    D: timely::Data,
    Diff: Semigroup,
{
    let data = data.enter(&child);

    _streaming_chunks(scope, child, name, &data.inner, |(_, base, _), new_ts| {
        *base = new_ts.clone()
    })
    .as_collection()
}

pub fn _streaming_chunks<'c, G, D, F>(
    scope: G,
    child: GranularChild<'c, G>,
    name: String,
    data: &Stream<GranularChild<'c, G>, D>,
    ts_mapper: F,
) -> Stream<GranularChild<'c, G>, D>
where
    G: Scope,
    G::Timestamp: Timestamp + Ord,
    D: timely::Data,
    F: Fn(&mut D, &Subtime<G::Timestamp>) + 'static,
{
    let worker_index = scope.index();

    let mut builder = OperatorBuilder::new(name, child.clone());

    let (mut data_output, data_stream) = builder.new_output();
    let mut data_input = builder.new_input_connection(&data, Pipeline, vec![Antichain::new()]);

    builder.build(move |mut caps| {
        // The output capability.
        let mut cap_set = CapabilitySet::from_elem(caps.pop().unwrap());

        // A map of outer times to the subtimes this worker is maintaining for them.
        let mut subtimes = BTreeMap::<G::Timestamp, usize>::new();
        let mut output_frontier = Antichain::from_elem((
            G::Timestamp::minimum(),
            Partitioned::new_singleton(worker_index, 0),
        ));
        cap_set.downgrade(output_frontier.iter());

        let mut vector = Vec::new();
        move |frontiers| {
            // Forward data, with times delayed to the subtime. Subtimes
            // are incremented per-container of data.

            let mut data_output = data_output.activate();
            while let Some((cap, data)) = data_input.next() {
                data.swap(&mut vector);

                // This containers outer time.
                let time = &cap.time().0;

                // The intended subtime for this outer time.
                let subtime = subtimes.entry(time.clone()).or_default();
                // Delay this container to its subtime, partitioned to this worker.
                let cap = cap_set.delayed(&(
                    time.clone(),
                    Partitioned::new_singleton(worker_index, *subtime),
                ));
                let mut session = data_output.session(&cap);
                vector.iter_mut().for_each(|d| ts_mapper(d, cap.time()));
                session.give_container(&mut vector);

                // Increment the subtime for this outer time.
                *subtime += 1;
            }

            let new_upper = frontiers[0].frontier();

            tracing::trace!(
                "{}: new upper:{:?}, output_frontier:{:?}",
                worker_index,
                new_upper,
                output_frontier
            );
            if PartialOrder::less_than(&output_frontier.borrow(), &new_upper) {
                // Retire subtimes for outer times that are done.
                subtimes.retain(|k, v| {
                    new_upper.less_equal(&(k.clone(), Partitioned::new_singleton(worker_index, *v)))
                });

                output_frontier = new_upper.to_owned();
            }

            // Downgrade the output frontier to the new subtimes.
            let mut new_output_frontier = Antichain::new();
            for (t, _) in output_frontier.iter() {
                new_output_frontier.insert((
                    t.clone(),
                    Partitioned::new_singleton(
                        worker_index,
                        *subtimes.entry(t.clone()).or_default(),
                    ),
                ));
            }
            output_frontier = new_output_frontier;
            cap_set.downgrade(output_frontier.iter());
            tracing::trace!(
                "{}: downgrade frontier to: {:?}",
                worker_index,
                output_frontier
            );
        }
    });
    data_stream
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    use crate::builder_async::OperatorBuilder as AsyncOperatorBuilder;
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::sync::Arc;
    use timely::dataflow::operators::{Inspect, Probe};
    use timely::{CommunicationConfig, Config};
    use tokio::sync::Notify;

    use crate::flow_control::Subtime;
    use Step::*;

    /// Test the general case.
    #[test]
    fn test_streaming_chunks_general() {
        runner(
            2,
            vec![
                TsChunk {
                    containers: vec![vec!["zero"], vec!["one"]],
                },
                ExactFrontier {
                    frontier: Antichain::from(vec![
                        (0, Partitioned::new_singleton(0, 2)),
                        (0, Partitioned::new_singleton(1, 2)),
                    ]),
                },
                AssertValues {
                    // Each container has its own subtime.
                    values_per_ts: 1,
                    unique_times: 2,
                },
                TsChunk {
                    containers: vec![vec!["zero"], vec!["one"]],
                },
                ExactFrontier {
                    frontier: Antichain::from(vec![
                        (1, Partitioned::new_singleton(0, 2)),
                        (1, Partitioned::new_singleton(1, 2)),
                    ]),
                },
                AssertValues {
                    values_per_ts: 1,
                    unique_times: 4,
                },
            ],
        );
    }

    /// Test that a finished upstream worker does not break things.
    #[test]
    fn test_streaming_chunks_finished_worker() {
        runner(
            2,
            vec![
                FinalizeWorker { worker_id: 0 },
                TsChunk {
                    containers: vec![vec!["zero"], vec!["one"]],
                },
                ExactFrontier {
                    frontier: Antichain::from(vec![
                        (0, Partitioned::new_singleton(0, 0)),
                        (0, Partitioned::new_singleton(1, 2)),
                    ]),
                },
                TsChunk {
                    containers: vec![vec!["zero"], vec!["one"]],
                },
                ExactFrontier {
                    frontier: Antichain::from(vec![
                        // We always make progress on all workers, even if they don't do any worker.
                        // Here worker 0 produces no values, but does downgrade to `1`.
                        (1, Partitioned::new_singleton(0, 0)),
                        (1, Partitioned::new_singleton(1, 2)),
                    ]),
                },
            ],
        );
    }

    /// A step in a test
    #[derive(Clone)]
    pub enum Step {
        /// Emit the following chunks of data, increasing the emission ts each time.
        ///
        /// The frontier is downgraded on the next `TsChunk`.
        TsChunk { containers: Vec<Vec<&'static str>> },
        /// Communicate to `input_operator` that this worker should produce no data and downgrade
        /// to `[]`.
        FinalizeWorker { worker_id: usize },
        /// Assert that the frontier is EXACTLY this frontier.
        ExactFrontier { frontier: Antichain<Subtime<u64>> },
        /// Assert that, on this worker, we see `values_per_ts` values per ts, and that there
        /// are `unique_times` granular timestamps.
        AssertValues {
            values_per_ts: usize,
            unique_times: usize,
        },
    }

    /// A runner for testing `streaming_chunks` using a list of `Step`'s.
    fn runner(workers: usize, input: Vec<Step>) {
        let config = Config {
            communication: CommunicationConfig::Process(workers),
            worker: Default::default(),
        };
        timely::execute::execute(config, move |worker| {
            let worker_id = worker.index();
            let steps = input.clone();
            let mut skip_input = false;
            let input: Vec<_> = input
                .clone()
                .into_iter()
                .filter_map(|f| match f {
                    Step::TsChunk { containers } => Some(containers),
                    FinalizeWorker { worker_id } => {
                        skip_input = worker_id == worker.index();
                        None
                    }
                    _ => None,
                })
                .collect();

            let counts = Rc::new(RefCell::new(BTreeMap::<Subtime<u64>, usize>::new()));
            let assert_counts = Rc::clone(&counts);

            let (probe, notify) = worker.dataflow::<u64, _, _>(move |scope| {
                let (data, notify) = input_operator(scope.clone(), input.into_iter(), skip_input);
                let inner_s = scope.clone();
                let probe = scope.scoped::<_, _, _>("granular", move |child| {
                    let data = streaming_chunks(inner_s, child.clone(), "test".to_string(), &data);

                    let data = data.inspect_batch(move |t, d| {
                        // Assert that we are pipelined!
                        assert_eq!(*t.1.interval().singleton().unwrap(), worker_id);
                        // Track the counts-per-ts on this worker.
                        *(counts.borrow_mut().entry(t.clone()).or_default()) += d.len();
                    });
                    data.probe()
                });
                (probe, notify)
            });

            for step in steps {
                match step {
                    TsChunk { .. } => notify.notify_one(),
                    ExactFrontier { frontier } => {
                        while probe.with_frontier(|front| {
                            eprintln!("current frontier: {front:?}");
                            PartialOrder::less_than(&front, &frontier.borrow())
                        }) {
                            worker.step();
                            std::thread::sleep(std::time::Duration::from_millis(25));
                        }
                        probe.with_frontier(|front| {
                            eprintln!("found frontier: {front:?}");
                            assert_eq!(front, frontier.borrow());
                        });
                    }
                    AssertValues {
                        values_per_ts,
                        unique_times,
                    } => {
                        for (_, count) in assert_counts.borrow().iter() {
                            assert_eq!(values_per_ts, *count);
                        }
                        assert_eq!(assert_counts.borrow().len(), unique_times)
                    }
                    _ => {}
                }
            }
            notify.notify_one();
        })
        .unwrap();
    }

    /// An operator that emits the given containers of data, waiting for notifications
    /// before downgrading the frontier.
    pub(crate) fn input_operator<
        G: Scope<Timestamp = u64>,
        I: Iterator<Item = Vec<Vec<S>>> + 'static,
        S: Clone + 'static,
    >(
        scope: G,
        mut input: I,
        skip_input: bool,
    ) -> (Stream<G, S>, Arc<Notify>) {
        let mut iterator = AsyncOperatorBuilder::new("input".to_string(), scope);
        let (mut output_handle, output) = iterator.new_output::<Vec<S>>();

        let notif = Arc::new(Notify::new());
        let in_notif = Arc::clone(&notif);
        let mut current = 0;
        iterator.build(|mut caps| async move {
            if skip_input {
                return;
            }

            let mut capability = Some(caps.pop().unwrap());
            in_notif.notified().await;
            while let Some(data) = input.next() {
                for mut container in data {
                    output_handle
                        .give_container(
                            &capability.as_ref().unwrap().delayed(&current),
                            &mut container,
                        )
                        .await;
                }

                current += 1;
                in_notif.notified().await;
                capability.as_mut().unwrap().downgrade(&current);
            }

            capability.take();
        });

        (output, notif)
    }
}
