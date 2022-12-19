use std::future::Future;
use std::pin::Pin;
use std::task::Poll;
use std::time::{Duration, Instant};

use clap::Parser;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::Inspect;

use mz_timely_util::builder_async::{Event, OperatorBuilder as AsyncOperatorBuilder};

#[derive(Debug, Parser)]
#[clap(name = "gus")]
struct Cli {}

#[tokio::main]
async fn main() {
    let args = Cli::parse();

    let handle = tokio::runtime::Handle::current();
    timely::execute::execute(timely::Config::process(2), move |worker| {
        let handle1 = handle.clone();
        let handle2 = handle.clone();
        let index = worker.index();

        worker.dataflow::<u64, _, _>(|scope| {
            let mut fast_producer =
                AsyncOperatorBuilder::new("fast_producer".to_string(), scope.clone());
            let (mut producer_output, producer_stream) = fast_producer.new_output();
            fast_producer.build(move |mut caps| async move {
                if index != 1 {
                    return;
                }
                let _guard = handle1.enter();
                let mut cap = caps.pop().unwrap();

                let mut val = 0;
                loop {
                    {
                        let mut out = producer_output.activate();
                        out.session(&cap).give(val);
                    }
                    val += 1;
                    cap.downgrade(&(cap.time() + 1));

                    if val % 50 == 0 {
                        println!("sleeping");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    } else {
                        tokio::task::yield_now().await;
                    }
                }
            });

            let mut cpu_eater = OperatorBuilder::new("cpu_eater".to_string(), scope.clone());
            let mut input = cpu_eater.new_input(&producer_stream, Exchange::new(|_| 0));
            cpu_eater.build(move |_caps| {
                move |_frontiers| {
                    if index != 0 {
                        return;
                    }
                    let mut buffer = vec![];
                    let mut max = 0;
                    input.for_each(|_cap, data| {
                        data.swap(&mut buffer);

                        // eat some cpu
                        std::thread::sleep(Duration::from_millis(10));
                        max = std::cmp::max(*buffer.iter().max().unwrap(), max);
                    });
                }
            });

            let mut async_op = AsyncOperatorBuilder::new("gus".to_string(), scope.clone());
            let mut sleep_fut = None;
            async_op.build(move |mut _caps| {
                let mut now = Instant::now();
                futures::future::poll_fn(move |mut ctx| {
                    if index != 0 {
                        return Poll::Ready(());
                    }

                    let _guard = handle2.enter();
                    loop {
                        if sleep_fut.is_none() {
                            sleep_fut =
                                Some(Box::pin(tokio::time::sleep(Duration::from_millis(100))));
                        }

                        if let Some(sleep_inner) = &mut sleep_fut {
                            println!("polling, {:?} elapsed since wake", now.elapsed());
                            // Note that we see this as output:
                            //
                            // polling, 308.149666ms elapsed since wake
                            // polling, 308.236791ms elapsed since wake
                            // polling, 203.9995ms elapsed since wake
                            // polling, 204.071083ms elapsed since wake
                            // polling, 302.113ms elapsed since wake
                            // polling, 302.220125ms elapsed since wake
                            // polling, 207.203041ms elapsed since wake
                            // polling, 207.281208ms elapsed since wake
                            // polling, 207.150291ms elapsed since wake
                            // polling, 207.220041ms elapsed since wake
                            //
                            //
                            // _Which means that we can fall behind in activations of futures, as we see
                            // that
                            // `cpu_eater` can run TWICE before we actually see a poll after being woken
                            if Pin::new(sleep_inner).poll(&mut ctx) == Poll::Pending {
                                now = Instant::now();
                                return Poll::<()>::Pending;
                            } else {
                                sleep_fut = None;
                            }
                        }
                    }
                })
            });
        })
    })
    .unwrap()
    .join();
}
