// Copyright 2020 - developers of the `grammers` project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Methods to deal with and offer access to updates.

use super::Client;
use crate::types::{ChatMap, Update};
use futures_util::future::{Either, join_all, select};
use grammers_mtsender::utils::sleep_until;
pub use grammers_mtsender::{AuthorizationError, InvocationError};
use grammers_session::channel_id;
pub use grammers_session::{PrematureEndReason, UpdateState};
use grammers_tl_types as tl;
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;
use web_time::Instant;

/// How long to wait after warning the user that the updates limit was exceeded.
const UPDATE_LIMIT_EXCEEDED_LOG_COOLDOWN: Duration = Duration::from_secs(300);

impl Client {
    /// Returns the next update from the buffer where they are queued until used.
    ///
    /// # Example
    ///
    /// ```
    /// # async fn f(client: grammers_client::Client) -> Result<(), Box<dyn std::error::Error>> {
    /// use grammers_client::Update;
    ///
    /// loop {
    ///     let update = client.next_update().await?;
    ///     // Echo incoming messages and ignore everything else
    ///     match update {
    ///         Update::NewMessage(mut message) if !message.outgoing() => {
    ///             message.respond(message.text()).await?;
    ///         }
    ///         _ => {}
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn next_update(&self) -> Result<Update, InvocationError> {
        loop {
            let (update, chats) = self.next_raw_update().await?;

            if let Some(update) = Update::new(self, update, &chats) {
                return Ok(update);
            }
        }
    }

    /// Returns the next raw update and associated chat map from the buffer where they are queued until used.
    ///
    /// # Example
    ///
    /// ```
    /// # async fn f(client: grammers_client::Client) -> Result<(), Box<dyn std::error::Error>> {
    /// loop {
    ///     let (update, chats) = client.next_raw_update().await?;
    ///
    ///     // Print all incoming updates in their raw form
    ///     dbg!(update);
    /// }
    /// # Ok(())
    /// # }
    ///
    /// ```
    ///
    /// P.S. If you don't receive updateBotInlineSend, go to [@BotFather](https://t.me/BotFather), select your bot and click "Bot Settings", then "Inline Feedback" and select probability.
    ///
    pub async fn next_raw_update(
        &self,
    ) -> Result<(tl::enums::Update, Arc<ChatMap>), InvocationError> {
        loop {
            let (deadline, get_diff, channel_requests) = {
                let state = &mut *self.0.state.write().unwrap();
                if let Some(update) = state.updates.pop_front() {
                    return Ok(update);
                }
                (
                    state.message_box.check_deadlines(), // first, as it might trigger differences
                    state.message_box.get_difference(),
                    state.message_box.get_all_channel_differences(&state.chat_hashes),
                )
            };

            // Process ALL channel differences CONCURRENTLY via join_all, matching
            // gotd's goroutine-per-channel model. While waiting, we race against
            // step() so real-time socket updates from unaffected channels are
            // delivered immediately — not blocked by getDifference recovery.
            if !channel_requests.is_empty() {
                if channel_requests.len() > 1 {
                    log::debug!(
                        "fetching {} channel differences concurrently",
                        channel_requests.len()
                    );
                }

                let diff_future = join_all(
                    channel_requests.iter().map(|req| self.invoke(req)),
                );
                let mut diff_future = Box::pin(diff_future);

                // Race getDifference against socket I/O. This ensures real-time
                // messages for channels NOT in getDifference are delivered instantly.
                let results = loop {
                    // Deliver any queued real-time updates before blocking
                    {
                        let state = &mut *self.0.state.write().unwrap();
                        if let Some(update) = state.updates.pop_front() {
                            return Ok(update);
                        }
                    }

                    let step = Box::pin(self.step());
                    match select(diff_future, step).await {
                        Either::Left((results, _)) => break results,
                        Either::Right((step_result, remaining_diff)) => {
                            step_result?;
                            diff_future = remaining_diff;
                            // step() processed socket data → loop back to check queue
                        }
                    }
                };

                let mut all_updates = Vec::new();
                let mut all_users = Vec::new();
                let mut all_chats = Vec::new();
                let mut fatal_error = None;

                for (request, result) in channel_requests.into_iter().zip(results) {
                    match result {
                        Ok(response) => {
                            let (updates, users, chats) = {
                                let state = &mut *self.0.state.write().unwrap();
                                state.message_box.apply_channel_difference(
                                    request,
                                    response,
                                    &mut state.chat_hashes,
                                )
                            };
                            all_updates.extend(updates);
                            all_users.extend(users);
                            all_chats.extend(chats);
                        }
                        Err(e) if e.is("PERSISTENT_TIMESTAMP_OUTDATED") => {
                            log::warn!(
                                "Getting difference for channel caused PersistentTimestampOutdated"
                            );
                            self.0
                                .state
                                .write()
                                .unwrap()
                                .message_box
                                .end_channel_difference(
                                    &request,
                                    PrematureEndReason::TemporaryServerIssues,
                                );
                        }
                        Err(e) if e.is("CHANNEL_PRIVATE") => {
                            log::info!(
                                "Account is now banned in {} so we can no longer fetch updates from it",
                                channel_id(&request)
                                    .map(|i| i.to_string())
                                    .unwrap_or_else(|| "empty channel".into())
                            );
                            self.0
                                .state
                                .write()
                                .unwrap()
                                .message_box
                                .end_channel_difference(
                                    &request,
                                    PrematureEndReason::Banned,
                                );
                        }
                        Err(InvocationError::Rpc(ref rpc_error)) if rpc_error.code == 500 => {
                            log::warn!(
                                "Telegram is having internal issues: {:#?}",
                                rpc_error
                            );
                            self.0
                                .state
                                .write()
                                .unwrap()
                                .message_box
                                .end_channel_difference(
                                    &request,
                                    PrematureEndReason::TemporaryServerIssues,
                                );
                        }
                        Err(e) => {
                            fatal_error = Some(e);
                        }
                    }
                }

                if !all_updates.is_empty() {
                    self.extend_update_queue(
                        all_updates,
                        ChatMap::new(all_users, all_chats),
                    );
                }

                if let Some(e) = fatal_error {
                    return Err(e);
                }

                continue;
            }

            // AccountWide getDifference — one DifferenceSlice per iteration.
            // Race against step() so real-time messages are delivered between
            // slices instead of waiting for the entire multi-round recovery.
            if let Some(request) = get_diff {
                let invoke_future = Box::pin(self.invoke(&request));
                let mut invoke_future = invoke_future;

                let response = loop {
                    {
                        let state = &mut *self.0.state.write().unwrap();
                        if let Some(update) = state.updates.pop_front() {
                            return Ok(update);
                        }
                    }

                    let step = Box::pin(self.step());
                    match select(invoke_future, step).await {
                        Either::Left((result, _)) => break result?,
                        Either::Right((step_result, remaining)) => {
                            step_result?;
                            invoke_future = remaining;
                        }
                    }
                };

                let (updates, users, chats) = {
                    let state = &mut *self.0.state.write().unwrap();
                    state
                        .message_box
                        .apply_difference(response, &mut state.chat_hashes)
                };
                self.extend_update_queue(updates, ChatMap::new(users, chats));
                continue;
            }

            let sleep = pin!(async { sleep_until(deadline).await });
            let step = pin!(async { self.step().await });

            match select(sleep, step).await {
                Either::Left(_) => {}
                Either::Right((step, _)) => step?,
            }
        }
    }

    pub(crate) fn process_socket_updates(&self, all_updates: Vec<tl::enums::Updates>) {
        if all_updates.is_empty() {
            return;
        }

        let mut result = Option::<(Vec<_>, Vec<_>, Vec<_>)>::None;
        {
            let state = &mut *self.0.state.write().unwrap();

            for updates in all_updates {
                if state
                    .message_box
                    .ensure_known_peer_hashes(&updates, &mut state.chat_hashes)
                    .is_err()
                {
                    continue;
                }
                match state
                    .message_box
                    .process_updates(updates, &state.chat_hashes)
                {
                    Ok(tup) => {
                        if let Some(res) = result.as_mut() {
                            res.0.extend(tup.0);
                            res.1.extend(tup.1);
                            res.2.extend(tup.2);
                        } else {
                            result = Some(tup);
                        }
                    }
                    // Continue processing remaining updates in the batch instead of
                    // stopping entirely. A gap in one update shouldn't cause all
                    // subsequent updates in the batch to be silently dropped.
                    Err(_) => continue,
                }
            }
        }

        if let Some((updates, users, chats)) = result {
            self.extend_update_queue(updates, ChatMap::new(users, chats));
        }
    }

    fn extend_update_queue(&self, mut updates: Vec<tl::enums::Update>, chat_map: Arc<ChatMap>) {
        let mut state = self.0.state.write().unwrap();

        if let Some(limit) = self.0.config.params.update_queue_limit {
            if let Some(exceeds) = (state.updates.len() + updates.len()).checked_sub(limit + 1) {
                let exceeds = exceeds + 1;
                let now = Instant::now();
                let notify = match state.last_update_limit_warn {
                    None => true,
                    Some(instant) => now - instant > UPDATE_LIMIT_EXCEEDED_LOG_COOLDOWN,
                };

                updates.truncate(updates.len() - exceeds);
                if notify {
                    log::warn!(
                        "{} updates were dropped because the update_queue_limit was exceeded",
                        exceeds
                    );
                }

                state.last_update_limit_warn = Some(now);
            }
        }

        state
            .updates
            .extend(updates.into_iter().map(|u| (u, chat_map.clone())));
    }

    /// Synchronize the updates state to the session.
    pub fn sync_update_state(&self) {
        let state = self.0.state.read().unwrap();
        self.0
            .config
            .session
            .set_state(state.message_box.session_state());
    }

    /// Mark a channel for aggressive polling (100ms interval).
    ///
    /// By default, channels use the standard 15-minute polling interval and rely on
    /// socket pushes for real-time updates. Large broadcast channels (100k+ subscribers)
    /// may not receive socket pushes, so they need aggressive polling via
    /// `getChannelDifference` to receive messages promptly.
    ///
    /// Only call this for channels you actively monitor — each watched channel generates
    /// ~10 API requests/second.
    pub fn watch_channel(&self, channel_id: i64) {
        self.0
            .state
            .write()
            .unwrap()
            .message_box
            .watch_channel(channel_id);
    }

    /// Stop aggressively polling a channel. Reverts to the standard 15-minute interval.
    pub fn unwatch_channel(&self, channel_id: i64) {
        self.0
            .state
            .write()
            .unwrap()
            .message_box
            .unwatch_channel(channel_id);
    }

    /// Seed a channel's update state so that `getChannelDifference` runs on the
    /// next call to [`Client::next_update`].
    ///
    /// Large broadcast channels may not receive real-time socket pushes from
    /// Telegram's server. Calling this method on startup for each monitored
    /// channel triggers a one-time `getChannelDifference` that establishes the
    /// channel's pts. After that, the server should push future updates via
    /// socket, eliminating the need for continuous polling.
    ///
    /// Unlike [`Client::watch_channel`], this does **not** set up recurring
    /// polling — it is a one-shot subscription.
    pub fn subscribe_to_channel(&self, channel_id: i64) {
        self.0
            .state
            .write()
            .unwrap()
            .message_box
            .subscribe_to_channel(channel_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::future::Future;

    fn get_client() -> Client {
        panic!()
    }

    #[test]
    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    fn ensure_next_update_future_impls_send() {
        if false {
            // We just want it to type-check, not actually run.
            fn typeck(_: impl Future + Send) {}
            typeck(get_client().next_update());
        }
    }
}
