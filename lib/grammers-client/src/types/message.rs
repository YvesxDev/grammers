// Copyright 2020 - developers of the `grammers` project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.
use crate::ChatMap;
#[cfg(any(feature = "markdown", feature = "html"))]
use crate::parsers;
use crate::types::reactions::InputReactions;
use crate::types::{InputMessage, Media, Photo};
use crate::{Client, types};
use crate::{InputMedia, utils};
use chrono::{DateTime, Utc};
use grammers_mtsender::InvocationError;
use grammers_session::PackedChat;
use grammers_tl_types as tl;
use std::fmt;
use std::sync::Arc;
use types::Chat;

#[cfg(feature = "fs")]
use std::{io, path::Path};

pub(crate) const EMPTY_MESSAGE: tl::types::Message = tl::types::Message {
    out: false,
    mentioned: false,
    media_unread: false,
    silent: false,
    post: false,
    from_scheduled: false,
    legacy: false,
    edit_hide: false,
    pinned: false,
    noforwards: false,
    invert_media: false,
    offline: false,
    video_processing_pending: false,
    id: 0,
    from_id: None,
    from_boosts_applied: None,
    peer_id: tl::enums::Peer::User(tl::types::PeerUser { user_id: 0 }),
    saved_peer_id: None,
    fwd_from: None,
    via_bot_id: None,
    via_business_bot_id: None,
    reply_to: None,
    date: 0,
    message: String::new(),
    media: None,
    reply_markup: None,
    entities: None,
    views: None,
    forwards: None,
    replies: None,
    edit_date: None,
    post_author: None,
    grouped_id: None,
    reactions: None,
    restriction_reason: None,
    ttl_period: None,
    quick_reply_shortcut_id: None,
    effect: None,
    factcheck: None,
    report_delivery_until_date: None,
};

/// Represents a Telegram message, which includes text messages, messages with media, and service
/// messages.
///
/// This message should be treated as a snapshot in time, that is, if the message is edited while
/// using this object, those changes won't alter this structure.
#[derive(Clone)]
pub struct Message {
    // Message services are a trimmed-down version of normal messages, but with `action`.
    //
    // Using `enum` just for that would clutter all methods with `match`, so instead service
    // messages are interpreted as messages and their action stored separatedly.
    pub raw: tl::types::Message,
    pub raw_action: Option<tl::enums::MessageAction>,
    pub(crate) client: Client,
    // When fetching messages or receiving updates, a set of chats will be present. A single
    // server response contains a lot of chats, and some might be related to deep layers of
    // a message action for instance. Keeping the entire set like this allows for cheaper clones
    // and moves, and saves us from worrying about picking out all the chats we care about.
    pub(crate) chats: Arc<ChatMap>,
}

impl Message {
    pub fn from_raw(
        client: &Client,
        message: tl::enums::Message,
        chats: &Arc<ChatMap>,
    ) -> Option<Self> {
        match message {
            // Don't even bother to expose empty messages to the user, even if they have an ID.
            tl::enums::Message::Empty(_) => None,
            tl::enums::Message::Message(msg) => Some(Message {
                raw: msg,
                raw_action: None,
                client: client.clone(),
                chats: Arc::clone(chats),
            }),
            tl::enums::Message::Service(msg) => Some(Message {
                raw: tl::types::Message {
                    out: msg.out,
                    mentioned: msg.mentioned,
                    media_unread: msg.media_unread,
                    silent: msg.silent,
                    post: msg.post,
                    from_scheduled: false,
                    legacy: msg.legacy,
                    edit_hide: false,
                    pinned: false,
                    noforwards: false,
                    invert_media: false,
                    video_processing_pending: false,
                    id: msg.id,
                    from_id: msg.from_id,
                    from_boosts_applied: None,
                    peer_id: msg.peer_id,
                    saved_peer_id: None,
                    fwd_from: None,
                    via_bot_id: None,
                    reply_to: msg.reply_to,
                    date: msg.date,
                    message: String::new(),
                    media: None,
                    reply_markup: None,
                    entities: None,
                    views: None,
                    forwards: None,
                    replies: None,
                    edit_date: None,
                    post_author: None,
                    grouped_id: None,
                    restriction_reason: None,
                    ttl_period: msg.ttl_period,
                    reactions: None,
                    quick_reply_shortcut_id: None,
                    via_business_bot_id: None,
                    offline: false,
                    effect: None,
                    factcheck: None,
                    report_delivery_until_date: None,
                },
                raw_action: Some(msg.action),
                client: client.clone(),
                chats: Arc::clone(chats),
            }),
        }
    }

    pub fn from_raw_short_updates(
        client: &Client,
        updates: tl::types::UpdateShortSentMessage,
        input: InputMessage,
        chat: PackedChat,
    ) -> Self {
        Self {
            raw: tl::types::Message {
                out: updates.out,
                mentioned: false,
                media_unread: false,
                silent: input.silent,
                post: false, // TODO true if sent to broadcast channel
                from_scheduled: false,
                legacy: false,
                edit_hide: false,
                pinned: false,
                noforwards: false, // TODO true if channel has noforwads?
                video_processing_pending: false,
                invert_media: input.invert_media,
                id: updates.id,
                from_id: None, // TODO self
                from_boosts_applied: None,
                peer_id: chat.to_peer(),
                saved_peer_id: None,
                fwd_from: None,
                via_bot_id: None,
                reply_to: input.reply_to.map(|reply_to_msg_id| {
                    tl::types::MessageReplyHeader {
                        reply_to_scheduled: false,
                        forum_topic: false,
                        quote: false,
                        reply_to_msg_id: Some(reply_to_msg_id),
                        reply_to_peer_id: None,
                        reply_from: None,
                        reply_media: None,
                        reply_to_top_id: None,
                        quote_text: None,
                        quote_entities: None,
                        quote_offset: None,
                    }
                    .into()
                }),
                date: updates.date,
                message: input.text,
                media: updates.media,
                reply_markup: input.reply_markup,
                entities: updates.entities,
                views: None,
                forwards: None,
                replies: None,
                edit_date: None,
                post_author: None,
                grouped_id: None,
                restriction_reason: None,
                ttl_period: updates.ttl_period,
                reactions: None,
                quick_reply_shortcut_id: None,
                via_business_bot_id: None,
                offline: false,
                effect: None,
                factcheck: None,
                report_delivery_until_date: None,
            },
            raw_action: None,
            client: client.clone(),
            chats: ChatMap::single(Chat::unpack(chat)),
        }
    }

    /// Whether the message is outgoing (i.e. you sent this message to some other chat) or
    /// incoming (i.e. someone else sent it to you or the chat).
    pub fn outgoing(&self) -> bool {
        self.raw.out
    }

    /// Whether you were mentioned in this message or not.
    ///
    /// This includes @username mentions, text mentions, and messages replying to one of your
    /// previous messages (even if it contains no mention in the message text).
    pub fn mentioned(&self) -> bool {
        self.raw.mentioned
    }

    /// Whether you have read the media in this message or not.
    ///
    /// Most commonly, these are voice notes that you have not played yet.
    pub fn media_unread(&self) -> bool {
        self.raw.media_unread
    }

    /// Whether the message should notify people with sound or not.
    pub fn silent(&self) -> bool {
        self.raw.silent
    }

    /// Whether this message is a post in a broadcast channel or not.
    pub fn post(&self) -> bool {
        self.raw.post
    }

    /// Whether this message was originated from a previously-scheduled message or not.
    pub fn from_scheduled(&self) -> bool {
        self.raw.from_scheduled
    }

    // `legacy` is not exposed, though it can be if it proves to be useful

    /// Whether the edited mark of this message is edited should be hidden (e.g. in GUI clients)
    /// or shown.
    pub fn edit_hide(&self) -> bool {
        self.raw.edit_hide
    }

    /// Whether this message is currently pinned or not.
    pub fn pinned(&self) -> bool {
        self.raw.pinned
    }

    /// The ID of this message.
    ///
    /// Message identifiers are counters that start at 1 and grow by 1 for each message produced.
    ///
    /// Every channel has its own unique message counter. This counter is the same for all users,
    /// but unique to each channel.
    ///
    /// Every account has another unique message counter which is used for private conversations
    /// and small group chats. This means different accounts will likely have different message
    /// identifiers for the same message in a private conversation or small group chat. This also
    /// implies that the message identifier alone is enough to uniquely identify the message,
    /// without the need to know the chat ID.
    ///
    /// **You cannot use the message ID of User A when running as User B**, unless this message
    /// belongs to a megagroup or broadcast channel. Beware of this when using methods like
    /// [`Client::delete_messages`], which **cannot** validate the chat where the message
    /// should be deleted for those cases.
    pub fn id(&self) -> i32 {
        self.raw.id
    }

    /// The sender of this message, if any.
    pub fn sender(&self) -> Option<types::Chat> {
        self.raw
            .from_id
            .as_ref()
            .or({
                // Incoming messages in private conversations don't include `from_id` since
                // layer 119, but the sender can only be the chat we're in.
                if !self.raw.out && matches!(self.raw.peer_id, tl::enums::Peer::User(_)) {
                    Some(&self.raw.peer_id)
                } else {
                    None
                }
            })
            .map(|from| utils::always_find_entity(from, &self.chats, &self.client))
    }

    /// The chat where this message was sent to.
    ///
    /// This might be the user you're talking to for private conversations, or the group or
    /// channel where the message was sent.
    pub fn chat(&self) -> types::Chat {
        utils::always_find_entity(&self.raw.peer_id, &self.chats, &self.client)
    }

    /// If this message was forwarded from a previous message, return the header with information
    /// about that forward.
    pub fn forward_header(&self) -> Option<tl::enums::MessageFwdHeader> {
        self.raw.fwd_from.clone()
    }

    /// If this message was sent @via some inline bot, return the bot's user identifier.
    pub fn via_bot_id(&self) -> Option<i64> {
        self.raw.via_bot_id
    }

    /// If this message is replying to a previous message, return the header with information
    /// about that reply.
    pub fn reply_header(&self) -> Option<tl::enums::MessageReplyHeader> {
        self.raw.reply_to.clone()
    }

    /// The date when this message was produced.
    pub fn date(&self) -> DateTime<Utc> {
        utils::date(self.raw.date)
    }

    /// The message's text.
    ///
    /// For service messages, this will be the empty strings.
    ///
    /// If the message has media, this text is the caption commonly displayed underneath it.
    pub fn text(&self) -> &str {
        &self.raw.message
    }

    /// Like [`text`](Self::text), but with the [`fmt_entities`](Self::fmt_entities)
    /// applied to produce a markdown string instead.
    ///
    /// Some formatting entities automatically added by Telegram, such as bot commands or
    /// clickable emails, are ignored in the generated string, as those do not need to be
    /// sent for Telegram to include them in the message.
    ///
    /// Formatting entities which cannot be represented in CommonMark without resorting to HTML,
    /// such as underline, are also ignored.
    #[cfg(feature = "markdown")]
    pub fn markdown_text(&self) -> String {
        if let Some(entities) = self.raw.entities.as_ref() {
            parsers::generate_markdown_message(&self.raw.message, entities)
        } else {
            self.raw.message.clone()
        }
    }

    /// Like [`text`](Self::text), but with the [`fmt_entities`](Self::fmt_entities)
    /// applied to produce a HTML string instead.
    ///
    /// Some formatting entities automatically added by Telegram, such as bot commands or
    /// clickable emails, are ignored in the generated string, as those do not need to be
    /// sent for Telegram to include them in the message.
    #[cfg(feature = "html")]
    pub fn html_text(&self) -> String {
        if let Some(entities) = self.raw.entities.as_ref() {
            parsers::generate_html_message(&self.raw.message, entities)
        } else {
            self.raw.message.clone()
        }
    }

    /// The media displayed by this message, if any.
    ///
    /// This not only includes photos or videos, but also contacts, polls, documents, locations
    /// and many other types.
    pub fn media(&self) -> Option<types::Media> {
        self.raw.media.clone().and_then(Media::from_raw)
    }

    /// If the message has a reply markup (which can happen for messages produced by bots),
    /// returns said markup.
    pub fn reply_markup(&self) -> Option<tl::enums::ReplyMarkup> {
        self.raw.reply_markup.clone()
    }

    /// The formatting entities used to format this message, such as bold, italic, with their
    /// offsets and lengths.
    pub fn fmt_entities(&self) -> Option<&Vec<tl::enums::MessageEntity>> {
        // TODO correct the offsets and lengths to match the byte offsets
        self.raw.entities.as_ref()
    }

    /// How many views does this message have, when applicable.
    ///
    /// The same user account can contribute to increment this counter indefinitedly, however
    /// there is a server-side cooldown limitting how fast it can happen (several hours).
    pub fn view_count(&self) -> Option<i32> {
        self.raw.views
    }

    /// How many times has this message been forwarded, when applicable.
    pub fn forward_count(&self) -> Option<i32> {
        self.raw.forwards
    }

    /// How many replies does this message have, when applicable.
    pub fn reply_count(&self) -> Option<i32> {
        match &self.raw.replies {
            None => None,
            Some(replies) => {
                let tl::enums::MessageReplies::Replies(replies) = replies;
                Some(replies.replies)
            }
        }
    }

    /// React to this message.
    ///
    /// # Examples
    ///
    /// ```
    /// # async fn f(message: grammers_client::types::Message, client: grammers_client::Client) -> Result<(), Box<dyn std::error::Error>> {
    /// message.react("👍").await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Make animation big & Add to recent
    ///
    /// ```
    /// # async fn f(message: grammers_client::types::Message, client: grammers_client::Client) -> Result<(), Box<dyn std::error::Error>> {
    /// use grammers_client::types::InputReactions;
    ///
    /// let reactions = InputReactions::emoticon("🤯").big().add_to_recent();
    ///
    /// message.react(reactions).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Remove reactions
    ///
    /// ```
    /// # async fn f(message: grammers_client::types::Message, client: grammers_client::Client) -> Result<(), Box<dyn std::error::Error>> {
    /// use grammers_client::types::InputReactions;
    ///
    /// message.react(InputReactions::remove()).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn react<R: Into<InputReactions>>(
        &self,
        reactions: R,
    ) -> Result<(), InvocationError> {
        self.client
            .send_reactions(self.chat(), self.id(), reactions)
            .await?;
        Ok(())
    }

    /// How many reactions does this message have, when applicable.
    pub fn reaction_count(&self) -> Option<i32> {
        match &self.raw.reactions {
            None => None,
            Some(reactions) => {
                let tl::enums::MessageReactions::Reactions(reactions) = reactions;
                let count = reactions
                    .results
                    .iter()
                    .map(|reaction: &tl::enums::ReactionCount| {
                        let tl::enums::ReactionCount::Count(reaction) = reaction;
                        reaction.count
                    })
                    .sum();
                Some(count)
            }
        }
    }

    /// The date when this message was last edited.
    pub fn edit_date(&self) -> Option<DateTime<Utc>> {
        self.raw.edit_date.map(utils::date)
    }

    /// If this message was sent to a channel, return the name used by the author to post it.
    pub fn post_author(&self) -> Option<&str> {
        self.raw.post_author.as_ref().map(|author| author.as_ref())
    }

    /// If this message belongs to a group of messages, return the unique identifier for that
    /// group.
    ///
    /// This applies to albums of media, such as multiple photos grouped together.
    ///
    /// Note that there may be messages sent in between the messages forming a group.
    pub fn grouped_id(&self) -> Option<i64> {
        self.raw.grouped_id
    }

    /// A list of reasons on why this message is restricted.
    ///
    /// The message is not restricted if the return value is `None`.
    pub fn restriction_reason(&self) -> Option<&Vec<tl::enums::RestrictionReason>> {
        self.raw.restriction_reason.as_ref()
    }

    /// If this message is a service message, return the service action that occured.
    pub fn action(&self) -> Option<&tl::enums::MessageAction> {
        self.raw_action.as_ref()
    }

    /// If this message is replying to another message, return the replied message ID.
    pub fn reply_to_message_id(&self) -> Option<i32> {
        match &self.raw.reply_to {
            Some(tl::enums::MessageReplyHeader::Header(m)) => m.reply_to_msg_id,
            Some(tl::enums::MessageReplyHeader::MessageReplyHeader(m)) => m.reply_to_msg_id,
            _ => None,
        }
    }
    
    /// If this message is in a forum topic, return whether it's a forum topic message.
    ///
    /// This method checks the `forum_topic` flag in the message's reply header.
    /// Forum topics are a Telegram feature that allows organizing conversations into topics
    /// within a group or channel.
    pub fn is_forum_topic(&self) -> bool {
        match &self.raw.reply_to {
            Some(tl::enums::MessageReplyHeader::Header(m)) => m.forum_topic,
            Some(tl::enums::MessageReplyHeader::MessageReplyHeader(m)) => m.forum_topic,
            _ => false,
        }
    }

    /// If this message is in a forum topic, return the topic ID.
    ///
    /// This method extracts the topic ID from the message's reply header.
    /// The topic ID should be in the `reply_to_top_id` field, but in some cases
    /// Telegram may use `reply_to_msg_id` for the topic ID, especially for the
    /// initial message in a topic.
    ///
    /// Returns `None` if the message is not in a forum topic or the topic ID is not available.
    pub fn topic_id(&self) -> Option<i32> {
        match &self.raw.reply_to {
            Some(header) => {
                let (forum_topic, reply_to_top_id, reply_to_msg_id) = match header {
                    tl::enums::MessageReplyHeader::Header(m) => 
                        (m.forum_topic, m.reply_to_top_id, m.reply_to_msg_id),
                    tl::enums::MessageReplyHeader::MessageReplyHeader(m) => 
                        (m.forum_topic, m.reply_to_top_id, m.reply_to_msg_id),
                    _ => (false, None, None),
                };

                // If this is a forum topic message
                if forum_topic {
                    // First try to use reply_to_top_id if available
                    if let Some(top_id) = reply_to_top_id {
                        return Some(top_id);
                    }
                    
                    // Fall back to reply_to_msg_id which is often the topic ID
                    // for the initial message in a topic
                    return reply_to_msg_id;
                }
                None
            }
            _ => None,
        }
    }
    
    /// Check if this message belongs to a specific topic ID.
    /// 
    /// This is useful for filtering messages by topic.
    pub fn is_in_topic(&self, topic_id: i32) -> bool {
        match self.topic_id() {
            Some(id) => id == topic_id,
            None => false,
        }
    }
    
    /// Check if this message belongs to any of the specified topic IDs.
    /// 
    /// This is useful for filtering messages by multiple topics.
    pub fn is_in_topics(&self, topic_ids: &[i32]) -> bool {
        match self.topic_id() {
            Some(id) => topic_ids.contains(&id),
            None => false,
        }
    }
    
    /// Returns detailed information about the topic status of this message.
    ///
    /// This method is useful for debugging and understanding how Telegram is structuring
    /// forum topic information in messages.
    pub fn debug_topic_info(&self) -> String {
        match &self.raw.reply_to {
            Some(header) => {
                let (variant, forum_topic, reply_to_top_id, reply_to_msg_id) = match header {
                    tl::enums::MessageReplyHeader::Header(m) => 
                        ("Header", m.forum_topic, m.reply_to_top_id, m.reply_to_msg_id),
                    tl::enums::MessageReplyHeader::MessageReplyHeader(m) => 
                        ("MessageReplyHeader", m.forum_topic, m.reply_to_top_id, m.reply_to_msg_id),
                    _ => ("Unknown", false, None, None),
                };
                
                let is_in_forum = if let Some(chat) = self.chat().as_channel() {
                    chat.is_forum()
                } else {
                    false
                };
                
                format!(
                    "Topic Debug Info:\n\
                     - Message ID: {}\n\
                     - Chat is forum-enabled: {}\n\
                     - Reply header variant: {}\n\
                     - forum_topic flag: {}\n\
                     - reply_to_top_id: {:?}\n\
                     - reply_to_msg_id: {:?}\n\
                     - topic_id() returns: {:?}",
                    self.raw.id,
                    is_in_forum,
                    variant,
                    forum_topic,
                    reply_to_top_id,
                    reply_to_msg_id,
                    self.topic_id()
                )
            },
            None => "No reply header found".to_string(),
        }
    }

    /// Fetch the message that this message is replying to, or `None` if this message is not a
    /// reply to a previous message.
    ///
    /// Shorthand for `Client::get_reply_to_message`.
    pub async fn get_reply(&self) -> Result<Option<Self>, InvocationError> {
        self.client
            .clone() // TODO don't clone
            .get_reply_to_message(self)
            .await
    }

    /// Respond to this message by sending a new message in the same chat, but without directly
    /// replying to it.
    ///
    /// Shorthand for `Client::send_message`.
    pub async fn respond<M: Into<InputMessage>>(
        &self,
        message: M,
    ) -> Result<Self, InvocationError> {
        self.client.send_message(&self.chat(), message).await
    }

    /// Respond to this message by sending a album in the same chat, but without directly
    /// replying to it.
    ///
    /// Shorthand for `Client::send_album`.
    pub async fn respond_album(
        &self,
        medias: Vec<InputMedia>,
    ) -> Result<Vec<Option<Self>>, InvocationError> {
        self.client.send_album(&self.chat(), medias).await
    }

    /// Directly reply to this message by sending a new message in the same chat that replies to
    /// it. This methods overrides the `reply_to` on the `InputMessage` to point to `self`.
    ///
    /// Shorthand for `Client::send_message`.
    pub async fn reply<M: Into<InputMessage>>(&self, message: M) -> Result<Self, InvocationError> {
        let message = message.into();
        self.client
            .send_message(&self.chat(), message.reply_to(Some(self.raw.id)))
            .await
    }

    /// Directly reply to this message by sending a album in the same chat that replies to
    /// it. This methods overrides the `reply_to` on the first `InputMedia` to point to `self`.
    ///
    /// Shorthand for `Client::send_album`.
    pub async fn reply_album(
        &self,
        mut medias: Vec<InputMedia>,
    ) -> Result<Vec<Option<Self>>, InvocationError> {
        medias.first_mut().unwrap().reply_to = Some(self.raw.id);
        self.client.send_album(&self.chat(), medias).await
    }

    /// Forward this message to another (or the same) chat.
    ///
    /// Shorthand for `Client::forward_messages`. If you need to forward multiple messages
    /// at once, consider using that method instead.
    pub async fn forward_to<C: Into<PackedChat>>(&self, chat: C) -> Result<Self, InvocationError> {
        // TODO return `Message`
        // When forwarding a single message, if it fails, Telegram should respond with RPC error.
        // If it succeeds we will have the single forwarded message present which we can unwrap.
        self.client
            .forward_messages(chat, &[self.raw.id], &self.chat())
            .await
            .map(|mut msgs| msgs.pop().unwrap().unwrap())
    }

    /// Edit this message to change its text or media.
    ///
    /// Shorthand for `Client::edit_message`.
    pub async fn edit<M: Into<InputMessage>>(&self, new_message: M) -> Result<(), InvocationError> {
        self.client
            .edit_message(&self.chat(), self.raw.id, new_message)
            .await
    }

    /// Delete this message for everyone.
    ///
    /// Shorthand for `Client::delete_messages`. If you need to delete multiple messages
    /// at once, consider using that method instead.
    pub async fn delete(&self) -> Result<(), InvocationError> {
        self.client
            .delete_messages(&self.chat(), &[self.raw.id])
            .await
            .map(drop)
    }

    /// Mark this message and all messages above it as read.
    ///
    /// Unlike `Client::mark_as_read`, this method only will mark the chat as read up to
    /// this message, not the entire chat.
    pub async fn mark_as_read(&self) -> Result<(), InvocationError> {
        let chat = self.chat().pack();
        if let Some(channel) = chat.try_to_input_channel() {
            self.client
                .invoke(&tl::functions::channels::ReadHistory {
                    channel,
                    max_id: self.raw.id,
                })
                .await
                .map(drop)
        } else {
            self.client
                .invoke(&tl::functions::messages::ReadHistory {
                    peer: chat.to_input_peer(),
                    max_id: self.raw.id,
                })
                .await
                .map(drop)
        }
    }

    /// Pin this message in the chat.
    ///
    /// Shorthand for `Client::pin_message`.
    pub async fn pin(&self) -> Result<(), InvocationError> {
        self.client.pin_message(&self.chat(), self.raw.id).await
    }

    /// Unpin this message from the chat.
    ///
    /// Shorthand for `Client::unpin_message`.
    pub async fn unpin(&self) -> Result<(), InvocationError> {
        self.client.unpin_message(&self.chat(), self.raw.id).await
    }

    /// Refetch this message, mutating all of its properties in-place.
    ///
    /// No changes will be made to the message if it fails to be fetched.
    ///
    /// Shorthand for `Client::get_messages_by_id`.
    pub async fn refetch(&self) -> Result<(), InvocationError> {
        // When fetching a single message, if it fails, Telegram should respond with RPC error.
        // If it succeeds we will have the single message present which we can unwrap.
        self.client
            .get_messages_by_id(&self.chat(), &[self.raw.id])
            .await?
            .pop()
            .unwrap()
            .unwrap();
        todo!("actually mutate self after get_messages_by_id returns `Message`")
    }

    /// Download the message media in this message if applicable.
    ///
    /// Returns `true` if there was media to download, or `false` otherwise.
    ///
    /// Shorthand for `Client::download_media`.
    #[cfg(feature = "fs")]
    pub async fn download_media<P: AsRef<Path>>(&self, path: P) -> Result<bool, io::Error> {
        // TODO probably encode failed download in error
        if let Some(media) = self.media() {
            self.client.download_media(&media, path).await.map(|_| true)
        } else {
            Ok(false)
        }
    }

    /// Get photo attached to the message if any.
    pub fn photo(&self) -> Option<Photo> {
        if let Media::Photo(photo) = self.media()? {
            return Some(photo);
        }

        None
    }
}

impl fmt::Debug for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Message")
            .field("id", &self.id())
            .field("outgoing", &self.outgoing())
            .field("date", &self.date())
            .field("text", &self.text())
            .field("chat", &self.chat())
            .field("sender", &self.sender())
            .field("reply_to_message_id", &self.reply_to_message_id())
            .field("via_bot_id", &self.via_bot_id())
            .field("media", &self.media())
            .field("mentioned", &self.mentioned())
            .field("media_unread", &self.media_unread())
            .field("silent", &self.silent())
            .field("post", &self.post())
            .field("from_scheduled", &self.from_scheduled())
            .field("edit_hide", &self.edit_hide())
            .field("pinned", &self.pinned())
            .field("forward_header", &self.forward_header())
            .field("reply_header", &self.reply_header())
            .field("reply_markup", &self.reply_markup())
            .field("fmt_entities", &self.fmt_entities())
            .field("view_count", &self.view_count())
            .field("forward_count", &self.forward_count())
            .field("reply_count", &self.reply_count())
            .field("edit_date", &self.edit_date())
            .field("post_author", &self.post_author())
            .field("grouped_id", &self.grouped_id())
            .field("restriction_reason", &self.restriction_reason())
            .field("action", &self.action())
            .finish()
    }
}
