// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! An operator for ssh statuses

use std::any::Any;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::Arc;

use mz_ore::error::ErrorExt;
use mz_ssh_util::tunnel::SshTunnelStatus;
use mz_ssh_util::tunnel_manager::SshStatusSubscriptionCallback;
use mz_timely_util::builder_async::OperatorBuilder as AsyncOperatorBuilder;
use timely::dataflow::{Scope, Stream};
use tokio::sync::mpsc::unbounded_channel;
use tokio_stream::StreamExt;

use super::{HealthStatusMessage, HealthStatusUpdate, StatusNamespace};

/// An operator that emits `HealthStatusMessage`'s for the `Ssh` namespace. It returns a
/// `SshStatusSubscriptionCallback` that can be used with the `mz-ssh-util` crate to obtain
/// new status subscriptions. Note that each worker gets an independent
/// `SshStatusSubscriptionCallback`.
pub fn ssh_health_operator<G: Scope<Timestamp = ()>>(
    scope: G,
) -> (
    SshStatusSubscriptionCallback,
    Stream<G, HealthStatusMessage>,
    Rc<dyn Any>,
) {
    let mut op = AsyncOperatorBuilder::new("ssh_health_operator".to_string(), scope.clone());
    let (mut output_handle, output) = op.new_output();

    let (input, mut channels) = unbounded_channel();

    let button = op.build(|mut caps| async move {
        let mut capability = Some(caps.pop().unwrap());

        let mut tunnel_statuses = BTreeMap::new();
        let mut subscriptions = tokio_stream::StreamMap::new();

        loop {
            let closed = matches!(
                channels.try_recv(),
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected)
            );
            let no_subscriptions = subscriptions.is_empty();

            if closed && no_subscriptions {
                break;
            }

            tokio::select! {
                Some((key, channel)) = channels.recv(), if !closed => {
                    // Add a new subscription to the `StreamMap`. We prioritize newer
                    // subscriptions for the same key, but in practice the channels are
                    // the same.
                    subscriptions.insert(key, channel);
                },
                Some((key, status)) = subscriptions.next(), if !no_subscriptions => {
                    tunnel_statuses.insert(key, status);

                    let mut status_to_emit = HealthStatusMessage {
                        index: 0,
                        namespace: StatusNamespace::Ssh,
                        update: HealthStatusUpdate::Running
                    };

                    // Create a single `Ssh`-namespaced errors out of all the different ssh tunnels
                    // involved in this source.
                    //
                    // In practice, this is exactly one, but until we deprecate per-broker ssh
                    // tunnels, we have to support multiple.
                    for (_key, status) in tunnel_statuses.iter() {
                        match (&mut status_to_emit.update, status) {
                            (HealthStatusUpdate::Running, SshTunnelStatus::Errored(e)) => {
                                status_to_emit.update = HealthStatusUpdate::Stalled {
                                    error: e.to_string(),
                                    hint: None,
                                    should_halt: false
                                };
                            }
                            (
                                HealthStatusUpdate::Stalled {
                                    ref mut error,
                                    ..
                                },
                                SshTunnelStatus::Errored(e)
                            ) => {
                                // The inference engine really doesn't like this method called any
                                // other way!
                                error.push_str(&format!(", {}", ErrorExt::display_with_causes(&**e)));
                            }
                            _ => {}
                        }
                    }
                    output_handle
                        .give(
                            capability.as_ref().unwrap(),
                            status_to_emit,
                        )
                        .await;
                }
                else => {
                    // We will `break` on the next iteration.
                    continue;
                }
            }
        }
        capability.take();
    });

    let input_token = Arc::new(input.clone());
    (
        SshStatusSubscriptionCallback::new(
            move |key, subscription| {
                let _ = input.send((key, subscription));
            },
            // A `Sender` as its token so we don't shut down this operator until the
            // user is done with it.
            input_token,
        ),
        output,
        Rc::new(button.press_on_drop()),
    )
}
