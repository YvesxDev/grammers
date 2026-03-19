// Copyright 2020 - developers of the `grammers` project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! This module deals with correct handling of updates, including gaps, and knowing when the code
//! should "get difference" (the set of updates that the client should know by now minus the set
//! of updates that it actually knows).
//!
//! Each chat has its own [`Entry`] in the [`MessageBox`] (this `struct` is the "entry point").
//! At any given time, the message box may be either getting difference for them (entry is in
//! [`MessageBox::getting_diff_for`]) or not. If not getting difference, a possible gap may be
//! found for the updates (entry is in [`MessageBox::possible_gaps`]). Otherwise, the entry is
//! on its happy path.
//!
//! Gaps are cleared when they are either resolved on their own (by waiting for a short time)
//! or because we got the difference for the corresponding entry.
//!
//! While there are entries for which their difference must be fetched,
//! [`MessageBox::check_deadlines`] will always return [`Instant::now`], since "now" is the time
//! to get the difference.
mod adaptor;
mod defs;

use super::ChatHashCache;
use crate::UpdateState;
use crate::generated::enums::ChannelState as ChannelStateEnum;
use crate::generated::types::ChannelState;
pub(crate) use defs::Entry;
pub use defs::{Gap, MessageBox};
use defs::{NO_DATE, NO_PTS, NO_SEQ, PtsInfo, State};
use grammers_tl_types as tl;
use log::{debug, info, trace, warn};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::mem;
use std::time::Duration;
use tl::enums::InputChannel;
use web_time::Instant;

fn next_updates_deadline() -> Instant {
    Instant::now() + defs::NO_UPDATES_TIMEOUT
}

fn next_channel_updates_deadline() -> Instant {
    Instant::now() + defs::CHANNEL_NO_UPDATES_TIMEOUT
}

#[allow(clippy::new_without_default)]
/// Creation, querying, and setting base state.
impl MessageBox {
    /// Create a new, empty [`MessageBox`].
    ///
    /// This is the only way it may return `true` from [`MessageBox::is_empty`].
    pub fn new() -> Self {
        trace!("created new message box with no previous state");
        Self {
            map: HashMap::new(),
            date: 1, // non-zero or getting difference will fail
            seq: NO_SEQ,
            possible_gaps: HashMap::new(),
            getting_diff_for: HashSet::new(),
            next_deadline: None,
            tmp_entries: HashSet::new(),
            pending_during_diff: HashMap::new(),
            delivered_during_diff: HashMap::new(),
            watched_channels: HashSet::new(),
        }
    }

    /// Returns the appropriate deadline for a channel based on whether it's watched.
    fn channel_deadline(&self, channel_id: i64) -> Instant {
        if self.watched_channels.contains(&channel_id) {
            Instant::now() + defs::WATCHED_CHANNEL_TIMEOUT
        } else {
            next_channel_updates_deadline()
        }
    }

    /// Returns the appropriate deadline for any entry.
    fn deadline_for(&self, entry: &Entry) -> Instant {
        match entry {
            Entry::Channel(id) => self.channel_deadline(*id),
            _ => next_updates_deadline(),
        }
    }

    /// Mark a channel for aggressive polling (100ms interval).
    /// Only watched channels are polled frequently; all others use the standard 15-min interval.
    pub fn watch_channel(&mut self, channel_id: i64) {
        info!("watching channel {} for aggressive polling", channel_id);
        self.watched_channels.insert(channel_id);
        // Reset deadline to the aggressive interval immediately
        if let Some(state) = self.map.get_mut(&Entry::Channel(channel_id)) {
            state.deadline = Instant::now() + defs::WATCHED_CHANNEL_TIMEOUT;
        }
    }

    /// Stop aggressively polling a channel. It will revert to the standard interval.
    pub fn unwatch_channel(&mut self, channel_id: i64) {
        self.watched_channels.remove(&channel_id);
    }

    /// Seed a channel's state and trigger a one-time `getChannelDifference`.
    ///
    /// Creates an entry with pts=1 and immediately marks it for getDifference.
    /// The server will respond with the channel's current state, establishing
    /// the correct pts. After that, socket pushes should deliver future updates.
    pub fn subscribe_to_channel(&mut self, channel_id: i64) {
        let entry = Entry::Channel(channel_id);
        if self.map.contains_key(&entry) {
            return; // Already has state, no need to seed
        }
        info!("subscribing to channel {} (seeding pts for getChannelDifference)", channel_id);
        let dl = self.deadline_for(&entry);
        self.map.insert(entry, State { pts: 1, deadline: dl });
        self.getting_diff_for.insert(entry);
    }

    /// Create a [`MessageBox`] from a previously known update state.
    pub fn load(state: UpdateState) -> Self {
        trace!("created new message box with state: {:?}", state);
        let deadline = next_updates_deadline();
        let mut map = HashMap::with_capacity(2 + state.channels.len());
        let mut getting_diff_for = HashSet::with_capacity(2 + state.channels.len());

        map.insert(
            Entry::AccountWide,
            State {
                pts: state.pts,
                deadline,
            },
        );
        if state.pts != NO_PTS {
            getting_diff_for.insert(Entry::AccountWide);
        }

        map.insert(
            Entry::SecretChats,
            State {
                pts: state.qts,
                deadline,
            },
        );
        if state.qts != NO_PTS {
            getting_diff_for.insert(Entry::SecretChats);
        }

        // Use the default channel deadline on load; watched channels will get
        // their aggressive deadline once watch_channel() is called after load.
        let channel_deadline = next_channel_updates_deadline();
        map.extend(state.channels.iter().map(|ChannelStateEnum::State(c)| {
            (
                Entry::Channel(c.channel_id),
                State {
                    pts: c.pts,
                    deadline: channel_deadline,
                },
            )
        }));
        // Do NOT add channels to getting_diff_for on load.
        // Channel updates are delivered immediately (even on pts gap),
        // so getDifference would only dump stale messages that cause
        // multi-second latency. Any truly missed messages during
        // downtime are already too old to be actionable.
        info!(
            "MessageBox loaded: {} channels tracked (call watch_channel to enable aggressive polling)",
            state.channels.len(),
        );

        Self {
            map,
            date: state.date,
            seq: state.seq,
            possible_gaps: HashMap::new(),
            getting_diff_for,
            next_deadline: Some(Entry::AccountWide),
            tmp_entries: HashSet::new(),
            pending_during_diff: HashMap::new(),
            delivered_during_diff: HashMap::new(),
            watched_channels: HashSet::new(),
        }
    }

    /// Return the current state in a format that sessions understand.
    ///
    /// This should be used for persisting the state.
    pub fn session_state(&self) -> UpdateState {
        UpdateState {
            pts: self
                .map
                .get(&Entry::AccountWide)
                .map(|s| s.pts)
                .unwrap_or(NO_PTS),
            qts: self
                .map
                .get(&Entry::SecretChats)
                .map(|s| s.pts)
                .unwrap_or(NO_PTS),
            date: self.date,
            seq: self.seq,
            channels: self
                .map
                .iter()
                .filter_map(|(entry, s)| match entry {
                    Entry::Channel(id) => Some(
                        ChannelState {
                            channel_id: *id,
                            pts: s.pts,
                        }
                        .into(),
                    ),
                    _ => None,
                })
                .collect(),
        }
    }

    /// Return true if the message box is empty and has no state yet.
    pub fn is_empty(&self) -> bool {
        self.map
            .get(&Entry::AccountWide)
            .map(|s| s.pts)
            .unwrap_or(NO_PTS)
            == NO_PTS
    }

    /// Return the next deadline when receiving updates should timeout.
    ///
    /// If a deadline expired, the corresponding entries will be marked as needing to get its difference.
    /// While there are entries pending of getting their difference, this method returns the current instant.
    pub fn check_deadlines(&mut self) -> Instant {
        let now = Instant::now();

        // Always check channel deadlines, even while account-wide getDifference
        // is in progress. Large broadcast channels need their own getDifference
        // polling and shouldn't be blocked by account-wide recovery.
        let mut added_channels = false;
        for (entry, state) in self.map.iter() {
            if matches!(entry, Entry::Channel(_))
                && now >= state.deadline
                && !self.getting_diff_for.contains(entry)
            {
                info!("channel {:?} deadline expired, triggering getDifference", entry);
                added_channels = true;
            }
        }
        if added_channels {
            // Collect expired channel entries (can't mutate during iteration above)
            let expired_channels: Vec<Entry> = self
                .map
                .iter()
                .filter_map(|(entry, state)| {
                    if matches!(entry, Entry::Channel(_))
                        && now >= state.deadline
                        && !self.getting_diff_for.contains(entry)
                    {
                        Some(*entry)
                    } else {
                        None
                    }
                })
                .collect();
            self.getting_diff_for.extend(expired_channels);
        }

        if !self.getting_diff_for.is_empty() {
            return now;
        }

        let deadline = next_updates_deadline();

        // Most of the time there will be zero or one gap in flight so finding the minimum is cheap.
        let deadline =
            if let Some(gap_deadline) = self.possible_gaps.values().map(|gap| gap.deadline).min() {
                deadline.min(gap_deadline)
            } else if let Some(state) = self.next_deadline.and_then(|entry| self.map.get(&entry)) {
                deadline.min(state.deadline)
            } else {
                deadline
            };

        if now >= deadline {
            // Check all expired entries and add them to the list that needs getting difference.
            // Skip channels — their gaps are delivered immediately in apply_pts_info,
            // so there's nothing to recover via getDifference.
            self.getting_diff_for
                .extend(self.possible_gaps.iter().filter_map(|(entry, gap)| {
                    if matches!(entry, Entry::Channel(_)) {
                        return None;
                    }
                    if now >= gap.deadline {
                        info!("gap was not resolved after waiting for {:?}", entry);
                        Some(entry)
                    } else {
                        None
                    }
                }));

            // Apply NO_UPDATES_TIMEOUT to all entries (including channels).
            // Channels were already checked above, but non-channel entries
            // still need checking here.
            self.getting_diff_for
                .extend(self.map.iter().filter_map(|(entry, state)| {
                    if now >= state.deadline {
                        debug!("too much time has passed without updates for {:?}", entry);
                        Some(entry)
                    } else {
                        None
                    }
                }));

            // When extending `getting_diff_for`, it's important to have the moral equivalent of
            // `begin_get_diff` (that is, clear possible gaps if we're now getting difference).
            let possible_gaps = &mut self.possible_gaps;
            self.getting_diff_for.iter().for_each(|entry| {
                possible_gaps.remove(entry);
            });
        }

        deadline
    }

    /// Reset the deadline for the periods without updates for all input entries.
    ///
    /// It also updates the next deadline time to reflect the new closest deadline.
    fn reset_deadlines(&mut self, entries: &HashSet<Entry>, deadline: Instant) {
        if entries.is_empty() {
            return;
        }
        for entry in entries {
            if let Some(state) = self.map.get_mut(entry) {
                state.deadline = deadline;
                debug!("reset deadline {:?} for {:?}", deadline, entry);
            } else {
                panic!("did not reset deadline for {entry:?} as it had no entry");
            }
        }

        if self
            .next_deadline
            .as_ref()
            .map(|next| entries.contains(next))
            .unwrap_or(false)
        {
            // If the updated deadline was the closest one, recalculate the new minimum.
            self.next_deadline = Some(
                self.map
                    .iter()
                    .min_by_key(|(_, state)| state.deadline)
                    .map(|i| *i.0)
                    .expect("deadline should exist"),
            );
        } else if self
            .next_deadline
            .map(|e| deadline < self.map[&e].deadline)
            .unwrap_or(false)
        {
            // If the updated deadline is smaller than the next deadline, change the next deadline to be the new one.
            // An unrelated deadline was updated, so the closest one remains unchanged.
            // Any entry will do, as they all share the same new deadline.
            self.next_deadline = Some(*entries.iter().next().unwrap());
        }
    }

    /// Convenience to reset a single entry's deadline.
    fn reset_deadline(&mut self, entry: Entry, deadline: Instant) {
        let mut entries = mem::take(&mut self.tmp_entries);
        entries.insert(entry);
        self.reset_deadlines(&entries, deadline);
        entries.clear();
        self.tmp_entries = entries;
    }

    /// Convenience to reset a channel's deadline, with optional timeout.
    fn reset_channel_deadline(&mut self, channel_id: i64, timeout: Option<i32>) {
        let default_timeout = if self.watched_channels.contains(&channel_id) {
            defs::WATCHED_CHANNEL_TIMEOUT
        } else {
            defs::CHANNEL_NO_UPDATES_TIMEOUT
        };
        self.reset_deadline(
            Entry::Channel(channel_id),
            Instant::now()
                + timeout
                    .map(|t| Duration::from_secs(t as _))
                    .unwrap_or(default_timeout),
        );
    }

    /// Sets the update state.
    ///
    /// Should be called right after login if [`MessageBox::new`] was used, otherwise undesirable
    /// updates will be fetched.
    pub fn set_state(&mut self, state: tl::enums::updates::State) {
        trace!("setting state {:?}", state);
        let deadline = next_updates_deadline();
        let state: tl::types::updates::State = state.into();
        self.map.insert(
            Entry::AccountWide,
            State {
                pts: state.pts,
                deadline,
            },
        );
        self.map.insert(
            Entry::SecretChats,
            State {
                pts: state.qts,
                deadline,
            },
        );
        self.date = state.date;
        self.seq = state.seq;
    }

    /// Like [`MessageBox::set_state`], but for channels. Useful when getting dialogs.
    ///
    /// The update state will only be updated if no entry was known previously.
    pub fn try_set_channel_state(&mut self, id: i64, pts: i32) {
        trace!("trying to set channel state for {}: {}", id, pts);
        let dl = self.channel_deadline(id);
        self.map.entry(Entry::Channel(id)).or_insert_with(|| State {
            pts,
            deadline: dl,
        });
    }

    /// Try to begin getting difference for the given entry.
    /// Fails if the entry does not have a previously-known state that can be used to get its difference.
    ///
    /// Clears any previous gaps.
    fn try_begin_get_diff(&mut self, entry: Entry) {
        if !self.map.contains_key(&entry) {
            if self.possible_gaps.contains_key(&entry) {
                panic!(
                    "Should not have a possible_gap for an entry {entry:?} not in the state map"
                );
            }
            // Only create entries for channels that were explicitly subscribed/watched.
            // Creating entries for ALL unknown channels causes a snowball: over time
            // hundreds of channels accumulate, their 15-min timeouts fire, and their
            // getChannelDifference catch-ups from pts=1 block watched channel polling.
            // Explicitly subscribed channels already have map entries via subscribe_to_channel.
            return;
        }

        self.getting_diff_for.insert(entry);
        self.possible_gaps.remove(&entry);
    }

    /// Finish getting difference for the given entry.
    ///
    /// It also resets the deadline and replays any updates that were buffered
    /// while getDifference was in progress.
    fn end_get_diff(&mut self, entry: Entry) -> Vec<tl::enums::Update> {
        if !self.getting_diff_for.remove(&entry) {
            panic!("Called end_get_diff on an entry which was not getting diff for");
        };
        let deadline = self.deadline_for(&entry);
        self.reset_deadline(entry, deadline);

        // Clean up socket-delivered message ID tracking for this entry
        self.delivered_during_diff.remove(&entry);

        // Replay updates that arrived during getDifference.
        // For channel entries, updates were delivered immediately (not buffered),
        // so this will typically be empty. For non-channel entries, buffered
        // updates are replayed now.
        let mut result = Vec::new();
        if let Some(buffered) = self.pending_during_diff.remove(&entry) {
            if !buffered.is_empty() {
                debug!(
                    "replaying {} buffered updates for {:?} after getDifference",
                    buffered.len(),
                    entry
                );
                for update in buffered {
                    let (_, applied) = self.apply_pts_info(update);
                    if let Some(update) = applied {
                        result.push(update);
                    }
                }
            }
        }

        result
    }
}

// "Normal" updates flow (processing and detection of gaps).
impl MessageBox {
    /// Make sure all peer hashes contained in the update are known by the client
    /// (either by checking if they were already known, or by extending the hash cache
    /// with those that were not known).
    ///
    /// If a peer is found, but it doesn't contain a non-`min` hash and no hash for it
    /// is known, it is treated as a gap.
    pub fn ensure_known_peer_hashes(
        &mut self,
        updates: &tl::enums::Updates,
        chat_hashes: &mut ChatHashCache,
    ) -> Result<(), Gap> {
        // Try to extend hashes from the update. If some peers have min constructors
        // without cached access hashes, we proceed anyway instead of triggering
        // getDifference. This prevents cascading gap recovery cycles that cause
        // message loss in active channels with many unique senders.
        //
        // The message content (text, chat_id, etc.) is still fully available even
        // when some referenced peers lack access hashes. Access hashes are only
        // needed for API calls to interact with those peers.
        if !chat_hashes.extend_from_updates(updates) {
            info!("received an update referencing an unknown peer; proceeding without triggering getDifference");
        }
        Ok(())
    }

    /// Process an update and return what should be done with it.
    ///
    /// Updates corresponding to entries for which their difference is currently being fetched
    /// will be ignored. While according to the [updates' documentation]:
    ///
    /// > Implementations \[have\] to postpone updates received via the socket while
    /// > filling gaps in the event and `Update` sequences, as well as avoid filling
    /// > gaps in the same sequence.
    ///
    /// In practice, these updates should have also been retrieved through getting difference.
    ///
    /// [updates' documentation]: https://core.telegram.org/api/updates
    pub fn process_updates(
        &mut self,
        updates: tl::enums::Updates,
        chat_hashes: &ChatHashCache,
    ) -> Result<defs::UpdateAndPeers, Gap> {
        trace!("processing updates: {:?}", updates);
        // Top level, when handling received `updates` and `updatesCombined`.
        // `updatesCombined` groups all the fields we care about, which is why we use it.
        //
        // This assumes all access hashes are already known to the client (socket updates are
        // expected to use `ensure_known_peer_hashes`, and the result from getting difference
        // has to deal with the peers in a different way).
        let tl::types::UpdatesCombined {
            date,
            seq_start,
            seq,
            mut updates,
            users,
            chats,
        } = match adaptor::adapt(updates, chat_hashes) {
            Ok(combined) => combined,
            Err(Gap) => {
                self.try_begin_get_diff(Entry::AccountWide);
                return Err(Gap);
            }
        };

        // > For all the other [not `updates` or `updatesCombined`] `Updates` type constructors
        // > there is no need to check `seq` or change a local state.
        if seq_start != NO_SEQ {
            match (self.seq + 1).cmp(&seq_start) {
                // Apply
                Ordering::Equal => {}
                // Ignore
                Ordering::Greater => {
                    debug!(
                        "skipping updates that were already handled at seq = {}",
                        self.seq
                    );
                    return Ok((Vec::new(), users, chats));
                }
                Ordering::Less => {
                    info!(
                        "seq gap detected (local {}, remote {}); delivering updates and triggering recovery",
                        self.seq, seq_start
                    );
                    self.try_begin_get_diff(Entry::AccountWide);
                    // Fall through to process individual updates immediately.
                    // getDifference will recover any truly missed updates in the background.
                    // Seq will be updated at the end of this method if updates are delivered.
                }
            }
        }

        fn update_sort_key(update: &tl::enums::Update) -> i32 {
            match PtsInfo::from_update(update) {
                Some(pts) => pts.pts - pts.pts_count,
                None => NO_PTS,
            }
        }

        // Telegram can send updates out of order (e.g. `ReadChannelInbox` first
        // and then `NewChannelMessage`, both with the same `pts`, but the `count`
        // is `0` and `1` respectively), so we sort them first.
        updates.sort_by_key(update_sort_key);

        // Adding `possible_gaps.len()` is a guesstimate. Often it's just one update.
        let mut result = Vec::with_capacity(updates.len() + self.possible_gaps.len());

        // This loop does a lot at once to reduce the amount of times we need to iterate over
        // the updates as an optimization.
        //
        // It mutates the local pts state, remembers possible gaps, builds a set of entries for
        // which the deadlines should be reset, and determines whether any local pts was changed
        // so that the seq can be updated too (which could otherwise have been done earlier).
        let mut reset_deadlines_for = mem::take(&mut self.tmp_entries);
        for update in updates {
            let (entry, update) = self.apply_pts_info(update);
            if let Some(entry) = entry {
                // As soon as we receive an update of any form related to messages (has `PtsInfo`),
                // the "no updates" period for that entry is reset. All the deadlines are reset at
                // once via the temporary entries buffer as an optimization.
                reset_deadlines_for.insert(entry);
            }
            if let Some(update) = update {
                result.push(update);
            }
        }
        // Split entries by type: watched channels get aggressive deadline,
        // unwatched channels get standard, non-channels get account-wide.
        let watched: HashSet<Entry> = reset_deadlines_for
            .iter()
            .filter(|e| matches!(e, Entry::Channel(id) if self.watched_channels.contains(id)))
            .copied()
            .collect();
        let unwatched_channels: HashSet<Entry> = reset_deadlines_for
            .iter()
            .filter(|e| matches!(e, Entry::Channel(id) if !self.watched_channels.contains(id)))
            .copied()
            .collect();
        let non_channel_entries: HashSet<Entry> = reset_deadlines_for
            .iter()
            .filter(|e| !matches!(e, Entry::Channel(_)))
            .copied()
            .collect();
        if !non_channel_entries.is_empty() {
            self.reset_deadlines(&non_channel_entries, next_updates_deadline());
        }
        if !unwatched_channels.is_empty() {
            self.reset_deadlines(&unwatched_channels, next_channel_updates_deadline());
        }
        if !watched.is_empty() {
            self.reset_deadlines(&watched, Instant::now() + defs::WATCHED_CHANNEL_TIMEOUT);
        }
        reset_deadlines_for.clear();
        self.tmp_entries = reset_deadlines_for;

        if !self.possible_gaps.is_empty() {
            // For each update in possible gaps, see if the gap has been resolved already.
            let keys = self.possible_gaps.keys().copied().collect::<Vec<_>>();
            for key in keys {
                self.possible_gaps
                    .get_mut(&key)
                    .unwrap()
                    .updates
                    .sort_by_key(update_sort_key);

                for _ in 0..self.possible_gaps[&key].updates.len() {
                    let update = self.possible_gaps.get_mut(&key).unwrap().updates.remove(0);
                    // If this fails to apply, it will get re-inserted at the end.
                    // All should fail, so the order will be preserved (it would've cycled once).
                    if let (_, Some(update)) = self.apply_pts_info(update) {
                        result.push(update);
                    }
                }
            }

            // Clear now-empty gaps.
            self.possible_gaps.retain(|_, v| !v.updates.is_empty());
            if self.possible_gaps.is_empty() {
                debug!("successfully resolved gap by waiting");
            }
        }

        if !result.is_empty() && self.possible_gaps.is_empty() {
            // > If the updates were applied, local *Updates* state must be updated
            // > with `seq` (unless it's 0) and `date` from the constructor.
            if date != NO_DATE {
                self.date = date;
            }
            if seq != NO_SEQ {
                self.seq = seq;
            }
        }

        Ok((result, users, chats))
    }

    /// Extract the message ID from a message update, for deduplication tracking.
    fn extract_message_id(update: &tl::enums::Update) -> Option<i32> {
        let message = match update {
            tl::enums::Update::NewMessage(u) => Some(&u.message),
            tl::enums::Update::NewChannelMessage(u) => Some(&u.message),
            tl::enums::Update::EditMessage(u) => Some(&u.message),
            tl::enums::Update::EditChannelMessage(u) => Some(&u.message),
            _ => None,
        }?;
        match message {
            tl::enums::Message::Message(m) => Some(m.id),
            tl::enums::Message::Service(m) => Some(m.id),
            tl::enums::Message::Empty(m) => Some(m.id),
        }
    }

    /// Tries to apply the input update if its `PtsInfo` follows the correct order.
    ///
    /// Updates are always delivered immediately regardless of gaps or getDifference state.
    /// Gaps trigger background recovery via getDifference, whose results are deduplicated
    /// against already-delivered updates via `delivered_during_diff` tracking.
    fn apply_pts_info(
        &mut self,
        update: tl::enums::Update,
    ) -> (Option<Entry>, Option<tl::enums::Update>) {
        if let tl::enums::Update::ChannelTooLong(u) = update {
            let entry = Entry::Channel(u.channel_id);

            // For large broadcast channels, Telegram sends ChannelTooLong instead
            // of individual message updates. Trigger getDifference to fetch the
            // actual messages. Since all updates are delivered immediately (never
            // blocked by getDifference), this won't cause delays for other channels.
            if let Some(remote_pts) = u.pts {
                if let Some(state) = self.map.get_mut(&entry) {
                    info!(
                        "ChannelTooLong for {}; triggering getDifference (local pts {}, remote {})",
                        u.channel_id, state.pts, remote_pts
                    );
                }
            }
            self.try_begin_get_diff(entry);
            return (None, None);
        }

        let pts = match PtsInfo::from_update(&update) {
            Some(pts) => pts,
            // No pts means that the update can be applied in any order.
            None => return (None, Some(update)),
        };

        // During getDifference: deliver immediately for ALL entries.
        // getDifference results are deduplicated via delivered_during_diff tracking.
        if self.getting_diff_for.contains(&pts.entry) {
            debug!(
                "delivering update during getDifference for {:?} (count {:?}, remote {:?})",
                pts.entry, pts.pts_count, pts.pts
            );

            if let Some(msg_id) = Self::extract_message_id(&update) {
                self.delivered_during_diff
                    .entry(pts.entry)
                    .or_default()
                    .insert(msg_id);
            }

            if let Some(state) = self.map.get_mut(&pts.entry) {
                if pts.pts > state.pts {
                    state.pts = pts.pts;
                }
            }

            return (Some(pts.entry), Some(update));
        }

        if let Some(state) = self.map.get(&pts.entry) {
            let local_pts = state.pts;
            match (local_pts + pts.pts_count).cmp(&pts.pts) {
                // Apply
                Ordering::Equal => {}
                // Ignore
                Ordering::Greater => {
                    debug!(
                        "skipping update for {:?} (local {:?}, count {:?}, remote {:?})",
                        pts.entry, local_pts, pts.pts_count, pts.pts
                    );
                    return (Some(pts.entry), None);
                }
                // Gap: deliver immediately, advance pts, trigger background recovery.
                Ordering::Less => {
                    info!(
                        "gap for {:?} (local {:?}, count {:?}, remote {:?}); delivering immediately",
                        pts.entry, local_pts, pts.pts_count, pts.pts
                    );

                    if let Some(msg_id) = Self::extract_message_id(&update) {
                        self.delivered_during_diff
                            .entry(pts.entry)
                            .or_default()
                            .insert(msg_id);
                    }

                    self.map.get_mut(&pts.entry).unwrap().pts = pts.pts;

                    // Trigger getDifference to recover potentially missed updates.
                    // Since updates are always delivered immediately (never blocked
                    // by getDifference), this is safe for all entries including channels.
                    self.try_begin_get_diff(pts.entry);

                    return (Some(pts.entry), Some(update));
                }
            }
        }
        // else, there is no previous `pts` known, and because this update has to be "right"
        // (it's the first one) our `local_pts` must be `pts - pts_count`.

        let dl = self.deadline_for(&pts.entry);
        self.map
            .entry(pts.entry)
            .or_insert_with(|| State {
                pts: NO_PTS,
                deadline: dl,
            })
            .pts = pts.pts;

        (Some(pts.entry), Some(update))
    }
}

/// Getting and applying account difference.
impl MessageBox {
    /// Return the request that needs to be made to get the difference, if any.
    pub fn get_difference(&mut self) -> Option<tl::functions::updates::GetDifference> {
        for entry in [Entry::AccountWide, Entry::SecretChats] {
            if self.getting_diff_for.contains(&entry) {
                if !self.map.contains_key(&entry) {
                    panic!(
                        "Should not try to get difference for an entry {entry:?} without known state"
                    );
                }

                let gd = tl::functions::updates::GetDifference {
                    pts: self.map[&Entry::AccountWide].pts,
                    pts_limit: None,
                    pts_total_limit: None,
                    date: self.date,
                    qts: if self.map.contains_key(&Entry::SecretChats) {
                        self.map[&Entry::SecretChats].pts
                    } else {
                        NO_PTS
                    },
                    qts_limit: None,
                };
                trace!("requesting {:?}", gd);
                return Some(gd);
            }
        }
        None
    }

    /// Similar to [`MessageBox::process_updates`], but using the result from getting difference.
    pub fn apply_difference(
        &mut self,
        difference: tl::enums::updates::Difference,
        chat_hashes: &mut ChatHashCache,
    ) -> defs::UpdateAndPeers {
        trace!("applying account difference: {:?}", difference);
        let finish: bool;
        let mut result = match difference {
            tl::enums::updates::Difference::Empty(diff) => {
                debug!(
                    "handling empty difference (date = {}, seq = {}); no longer getting diff",
                    diff.date, diff.seq
                );
                finish = true;
                self.date = self.date.max(diff.date);
                self.seq = self.seq.max(diff.seq);
                (Vec::new(), Vec::new(), Vec::new())
            }
            tl::enums::updates::Difference::Difference(diff) => {
                // TODO return Err(attempt to find users)
                let _ = chat_hashes.extend(&diff.users, &diff.chats);

                debug!(
                    "handling full difference {:?}; no longer getting diff",
                    diff.state
                );
                finish = true;
                self.apply_difference_type(diff, chat_hashes)
            }
            tl::enums::updates::Difference::Slice(tl::types::updates::DifferenceSlice {
                new_messages,
                new_encrypted_messages,
                other_updates,
                chats,
                users,
                intermediate_state: state,
            }) => {
                // TODO return Err(attempt to find users)
                let _ = chat_hashes.extend(&users, &chats);

                debug!("handling partial difference {:?}", state);
                finish = false;
                self.apply_difference_type(
                    tl::types::updates::Difference {
                        new_messages,
                        new_encrypted_messages,
                        other_updates,
                        chats,
                        users,
                        state,
                    },
                    chat_hashes,
                )
            }
            tl::enums::updates::Difference::TooLong(diff) => {
                warn!(
                    "handling too-long difference (pts = {}); fast-forwarding pts, some messages may be lost",
                    diff.pts
                );
                finish = true;
                // the deadline will be reset once the diff ends
                let aw = self.map.get_mut(&Entry::AccountWide).unwrap();
                aw.pts = aw.pts.max(diff.pts);
                (Vec::new(), Vec::new(), Vec::new())
            }
        };

        if finish {
            let account = self.getting_diff_for.contains(&Entry::AccountWide);
            let secret = self.getting_diff_for.contains(&Entry::SecretChats);

            if !account && !secret {
                panic!(
                    "Should not be applying the difference when neither account or secret diff was active"
                )
            }

            if account {
                let replayed = self.end_get_diff(Entry::AccountWide);
                result.0.extend(replayed);
            }
            if secret {
                let replayed = self.end_get_diff(Entry::SecretChats);
                result.0.extend(replayed);
            }
        }

        result
    }

    fn apply_difference_type(
        &mut self,
        tl::types::updates::Difference {
            new_messages,
            new_encrypted_messages,
            other_updates: updates,
            chats,
            users,
            state: tl::enums::updates::State::State(state),
        }: tl::types::updates::Difference,
        chat_hashes: &mut ChatHashCache,
    ) -> defs::UpdateAndPeers {
        // Use max() for all state fields — socket-delivered updates during getDifference
        // may have advanced these beyond what the getDifference response contains.
        // Rewinding would cause cascading false gaps.
        let aw = self.map.get_mut(&Entry::AccountWide).unwrap();
        aw.pts = aw.pts.max(state.pts);
        let sc = self.map
            .entry(Entry::SecretChats)
            // AccountWide affects SecretChats, but this may not have been initialized yet (#258)
            .or_insert_with(|| State {
                pts: NO_PTS,
                deadline: next_updates_deadline(),
            });
        sc.pts = sc.pts.max(state.qts);
        self.date = self.date.max(state.date);
        self.seq = self.seq.max(state.seq);

        // other_updates can contain things like UpdateChannelTooLong and UpdateNewChannelMessage.
        // We need to process those as if they were socket updates to discard any we have already handled.
        let us = tl::enums::Updates::Updates(tl::types::Updates {
            updates,
            users,
            chats,
            date: NO_DATE,
            seq: NO_SEQ,
        });

        // It is possible that the result from `GetDifference` includes users with `min = true`.
        // TODO in that case, we will have to resort to getUsers.
        let (mut result_updates, users, chats) = self
            .process_updates(us, chat_hashes)
            .expect("gap is detected while applying difference");

        // Filter out messages already delivered via socket during getDifference
        let new_messages = self.filter_already_delivered(&Entry::AccountWide, new_messages);

        result_updates.extend(
            new_messages
                .into_iter()
                .map(|message| {
                    tl::types::UpdateNewMessage {
                        message,
                        pts: NO_PTS,
                        pts_count: 0,
                    }
                    .into()
                })
                .chain(new_encrypted_messages.into_iter().map(|message| {
                    tl::types::UpdateNewEncryptedMessage {
                        message,
                        qts: NO_PTS,
                    }
                    .into()
                })),
        );

        (result_updates, users, chats)
    }
}

/// Getting and applying channel difference.
impl MessageBox {
    /// Return the request that needs to be made to get a channel's difference, if any.
    pub fn get_channel_difference(
        &mut self,
        chat_hashes: &ChatHashCache,
    ) -> Option<tl::functions::updates::GetChannelDifference> {
        let (entry, id) = self
            .getting_diff_for
            .iter()
            .find_map(|&entry| match entry {
                Entry::Channel(id) => Some((entry, id)),
                _ => None,
            })?;

        if let Some(packed) = chat_hashes.get(id) {
            let channel = tl::types::InputChannel {
                channel_id: packed.id,
                access_hash: packed
                    .access_hash
                    .expect("chat_hashes had chat without hash"),
            }
            .into();
            if let Some(state) = self.map.get(&entry) {
                let gd = tl::functions::updates::GetChannelDifference {
                    force: false,
                    channel,
                    filter: tl::enums::ChannelMessagesFilter::Empty,
                    pts: state.pts,
                    limit: if chat_hashes.is_self_bot() {
                        defs::BOT_CHANNEL_DIFF_LIMIT
                    } else {
                        defs::USER_CHANNEL_DIFF_LIMIT
                    },
                };
                trace!("requesting {:?}", gd);
                Some(gd)
            } else {
                panic!(
                    "Should not try to get difference for an entry {entry:?} without known state"
                );
            }
        } else {
            warn!(
                "cannot getChannelDifference for {} as we're missing its hash; fast-forwarding pts",
                id
            );
            // Don't remove the map entry — that would cause an infinite cycle of:
            // gap detected → hash missing → entry removed → new entry → gap detected.
            // Instead, end getDiff (which replays buffered updates) and keep the pts
            // so future updates can be applied from wherever we are now.
            let _ = self.end_get_diff(entry);
            None
        }
    }

    /// Return ALL pending channel difference requests at once, for concurrent processing.
    ///
    /// Unlike [`get_channel_difference`] which returns one request at a time,
    /// this collects all pending channel entries and returns their requests together.
    /// The caller can then fire them concurrently (e.g. via `join_all`), matching
    /// gotd's parallel goroutine approach.
    pub fn get_all_channel_differences(
        &mut self,
        chat_hashes: &ChatHashCache,
    ) -> Vec<tl::functions::updates::GetChannelDifference> {
        let all_channel_entries: Vec<(Entry, i64)> = self
            .getting_diff_for
            .iter()
            .filter_map(|&entry| match entry {
                Entry::Channel(id) => Some((entry, id)),
                _ => None,
            })
            .collect();

        // Prioritize watched channels: if any watched channels need polling,
        // only process those. This prevents slow non-watched channel catch-ups
        // from blocking watched channel polls in join_all.
        let watched_entries: Vec<(Entry, i64)> = all_channel_entries
            .iter()
            .filter(|(_, id)| self.watched_channels.contains(id))
            .copied()
            .collect();

        let channel_entries = if !watched_entries.is_empty() {
            // Defer non-watched channels to next iteration
            watched_entries
        } else {
            all_channel_entries
        };

        if !channel_entries.is_empty() {
            info!(
                "get_all_channel_differences: {} channels pending getDifference",
                channel_entries.len()
            );
        }

        let mut requests = Vec::new();
        let mut missing_hash_entries = Vec::new();

        for (entry, id) in channel_entries {
            if let Some(packed) = chat_hashes.get(id) {
                let channel = tl::types::InputChannel {
                    channel_id: packed.id,
                    access_hash: packed
                        .access_hash
                        .expect("chat_hashes had chat without hash"),
                }
                .into();
                if let Some(state) = self.map.get(&entry) {
                    info!(
                        "requesting getChannelDifference for channel {} (pts = {})",
                        id, state.pts
                    );
                    let gd = tl::functions::updates::GetChannelDifference {
                        force: false,
                        channel,
                        filter: tl::enums::ChannelMessagesFilter::Empty,
                        pts: state.pts,
                        limit: if chat_hashes.is_self_bot() {
                            defs::BOT_CHANNEL_DIFF_LIMIT
                        } else {
                            defs::USER_CHANNEL_DIFF_LIMIT
                        },
                    };
                    requests.push(gd);
                }
            } else {
                missing_hash_entries.push((entry, id));
            }
        }

        for (entry, id) in missing_hash_entries {
            warn!(
                "cannot getChannelDifference for {} as we're missing its hash; fast-forwarding pts",
                id
            );
            let _ = self.end_get_diff(entry);
        }

        requests
    }

    /// Extract a message ID from a `tl::enums::Message`.
    fn message_id(message: &tl::enums::Message) -> i32 {
        match message {
            tl::enums::Message::Message(m) => m.id,
            tl::enums::Message::Service(m) => m.id,
            tl::enums::Message::Empty(m) => m.id,
        }
    }

    /// Filter new_messages from getDifference to exclude those already delivered via socket.
    /// Returns the filtered list, removing duplicates.
    fn filter_already_delivered(
        &self,
        entry: &Entry,
        messages: Vec<tl::enums::Message>,
    ) -> Vec<tl::enums::Message> {
        if let Some(delivered_ids) = self.delivered_during_diff.get(entry) {
            if !delivered_ids.is_empty() {
                let before = messages.len();
                let filtered: Vec<_> = messages
                    .into_iter()
                    .filter(|msg| !delivered_ids.contains(&Self::message_id(msg)))
                    .collect();
                let skipped = before - filtered.len();
                if skipped > 0 {
                    debug!(
                        "filtered {} already-delivered messages from getDifference for {:?}",
                        skipped, entry
                    );
                }
                return filtered;
            }
        }
        messages
    }

    /// When setting pts from getDifference, use the max of response and current.
    /// Socket-delivered updates may have advanced pts beyond what getDifference returns.
    fn set_channel_pts_max(&mut self, entry: &Entry, response_pts: i32) {
        if let Some(state) = self.map.get_mut(entry) {
            state.pts = response_pts.max(state.pts);
        }
    }

    /// Similar to [`MessageBox::process_updates`], but using the result from getting difference.
    pub fn apply_channel_difference(
        &mut self,
        request: tl::functions::updates::GetChannelDifference,
        difference: tl::enums::updates::ChannelDifference,
        chat_hashes: &mut ChatHashCache,
    ) -> defs::UpdateAndPeers {
        let channel_id = channel_id(&request).expect("request had wrong input channel");
        let diff_type = match &difference {
            tl::enums::updates::ChannelDifference::Empty(_) => "Empty",
            tl::enums::updates::ChannelDifference::TooLong(d) => {
                info!(
                    "channel {} getDifference returned TooLong ({} messages)",
                    channel_id, d.messages.len()
                );
                "TooLong"
            }
            tl::enums::updates::ChannelDifference::Difference(d) => {
                info!(
                    "channel {} getDifference returned {} new messages, {} other updates (final={})",
                    channel_id, d.new_messages.len(), d.other_updates.len(), d.r#final
                );
                "Difference"
            }
        };
        debug!(
            "applying channel difference for {} (type={})",
            channel_id, diff_type
        );
        let entry = Entry::Channel(channel_id);

        self.possible_gaps.remove(&entry);

        match difference {
            tl::enums::updates::ChannelDifference::Empty(diff) => {
                assert!(diff.r#final);
                debug!(
                    "handling empty channel {} difference (pts = {}); no longer getting diff",
                    channel_id, diff.pts
                );
                let replayed = self.end_get_diff(entry);
                self.set_channel_pts_max(&entry, diff.pts);
                (replayed, Vec::new(), Vec::new())
            }
            tl::enums::updates::ChannelDifference::TooLong(diff) => {
                let _ = chat_hashes.extend(&diff.users, &diff.chats);

                assert!(diff.r#final);
                info!(
                    "handling too long channel {} difference; no longer getting diff",
                    channel_id
                );
                let too_long_pts = match diff.dialog {
                    tl::enums::Dialog::Dialog(d) => d.pts.expect(
                        "channelDifferenceTooLong dialog did not actually contain a pts",
                    ),
                    tl::enums::Dialog::Folder(_) => {
                        panic!("received a folder on channelDifferenceTooLong")
                    }
                };

                // Filter messages already delivered via socket during getDifference
                let new_messages = self.filter_already_delivered(&entry, diff.messages);

                // End getDifference and replay buffered updates
                let replayed = self.end_get_diff(entry);
                self.set_channel_pts_max(&entry, too_long_pts);
                self.reset_channel_deadline(channel_id, diff.timeout);

                let mut result_updates: Vec<tl::enums::Update> = new_messages
                    .into_iter()
                    .map(|message| {
                        tl::types::UpdateNewChannelMessage {
                            message,
                            pts: NO_PTS,
                            pts_count: 0,
                        }
                        .into()
                    })
                    .collect();
                result_updates.extend(replayed);
                (result_updates, diff.users, diff.chats)
            }
            tl::enums::updates::ChannelDifference::Difference(
                tl::types::updates::ChannelDifference {
                    r#final,
                    pts,
                    timeout,
                    new_messages,
                    other_updates: updates,
                    chats,
                    users,
                },
            ) => {
                let _ = chat_hashes.extend(&users, &chats);

                // Filter messages already delivered via socket during getDifference
                let new_messages = self.filter_already_delivered(&entry, new_messages);

                if r#final {
                    debug!(
                        "handling channel {} difference; no longer getting diff",
                        channel_id
                    );
                    let replayed = self.end_get_diff(entry);

                    self.set_channel_pts_max(&entry, pts);
                    let us = tl::enums::Updates::Updates(tl::types::Updates {
                        updates,
                        users,
                        chats,
                        date: NO_DATE,
                        seq: NO_SEQ,
                    });
                    let (mut result_updates, users, chats) = self
                        .process_updates(us, chat_hashes)
                        .expect("gap is detected while applying channel difference");

                    result_updates.extend(new_messages.into_iter().map(|message| {
                        tl::types::UpdateNewChannelMessage {
                            message,
                            pts: NO_PTS,
                            pts_count: 0,
                        }
                        .into()
                    }));
                    result_updates.extend(replayed);
                    self.reset_channel_deadline(channel_id, timeout);

                    (result_updates, users, chats)
                } else {
                    debug!("handling channel {} difference", channel_id);

                    self.set_channel_pts_max(&entry, pts);
                    let us = tl::enums::Updates::Updates(tl::types::Updates {
                        updates,
                        users,
                        chats,
                        date: NO_DATE,
                        seq: NO_SEQ,
                    });
                    let (mut result_updates, users, chats) = self
                        .process_updates(us, chat_hashes)
                        .expect("gap is detected while applying channel difference");

                    result_updates.extend(new_messages.into_iter().map(|message| {
                        tl::types::UpdateNewChannelMessage {
                            message,
                            pts: NO_PTS,
                            pts_count: 0,
                        }
                        .into()
                    }));
                    self.reset_channel_deadline(channel_id, timeout);

                    (result_updates, users, chats)
                }
            }
        }
    }

    pub fn end_channel_difference(
        &mut self,
        request: &tl::functions::updates::GetChannelDifference,
        reason: PrematureEndReason,
    ) {
        if let Some(channel_id) = channel_id(request) {
            trace!(
                "ending channel difference for {} because {:?}",
                channel_id, reason
            );
            let entry = Entry::Channel(channel_id);
            match reason {
                PrematureEndReason::TemporaryServerIssues => {
                    self.possible_gaps.remove(&entry);
                    // Discard replayed updates on error — they can't be delivered without context
                    let _ = self.end_get_diff(entry);
                }
                PrematureEndReason::Banned => {
                    self.possible_gaps.remove(&entry);
                    let _ = self.end_get_diff(entry);
                    self.map.remove(&entry);
                }
            }
        };
    }
}

pub fn channel_id(request: &tl::functions::updates::GetChannelDifference) -> Option<i64> {
    match request.channel {
        InputChannel::Channel(ref c) => Some(c.channel_id),
        InputChannel::FromMessage(ref c) => Some(c.channel_id),
        InputChannel::Empty => None,
    }
}

#[derive(Debug)]
pub enum PrematureEndReason {
    TemporaryServerIssues,
    Banned,
}
