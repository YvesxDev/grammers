//! Example to demonstrate forum topic filtering.
//!
//! The `TG_ID` and `TG_HASH` environment variables must be set (learn how to do it for
//! [Windows](https://ss64.com/nt/set.html) or [Linux](https://ss64.com/bash/export.html))
//! to Telegram's API ID and API hash respectively.
//!
//! Then, run it as:
//!
//! ```sh
//! cargo run --example forum-topic-filter -- BOT_TOKEN TOPIC_ID
//! ```
//!
//! This example demonstrates how to filter messages by forum topic ID.

use futures_util::future::{Either, select};
use grammers_client::session::Session;
use grammers_client::{Client, Config, InitParams, Update};
use simple_logger::SimpleLogger;
use std::env;
use std::pin::pin;
use tokio::{runtime, task};

type Result = std::result::Result<(), Box<dyn std::error::Error>>;

const SESSION_FILE: &str = "forum-topic-filter.session";

async fn handle_update(client: Client, update: Update, allowed_topics: &[i32]) -> Result {
    match update {
        Update::NewMessage(message) if !message.outgoing() => {
            let chat = message.chat();
            
            // Check if the message is from a forum topic
            if message.is_forum_topic() {
                println!(
                    "Received forum topic message from {}",
                    chat.name().unwrap_or(&format!("id {}", chat.id()))
                );
                
                // Get the topic ID
                if let Some(topic_id) = message.topic_id() {
                    println!("Message is in topic ID: {}", topic_id);
                    
                    // Check if the topic ID is in our allowed list
                    if message.is_in_topics(allowed_topics) {
                        println!("Topic ID is allowed, responding to message");
                        client.send_message(&chat, format!("You said: {} (in topic {})", message.text(), topic_id)).await?;
                    } else {
                        println!("Topic ID is not in allowed list, ignoring message");
                    }
                } else {
                    println!("Message is in a forum but topic ID is unknown");
                }
            } else {
                println!(
                    "Received non-forum message from {}",
                    chat.name().unwrap_or(&format!("id {}", chat.id()))
                );
            }
        }
        _ => {}
    }

    Ok(())
}

async fn async_main() -> Result {
    SimpleLogger::new()
        .with_level(log::LevelFilter::Debug)
        .init()
        .unwrap();

    let api_id = env!("TG_ID").parse().expect("TG_ID invalid");
    let api_hash = env!("TG_HASH").to_string();
    let token = env::args().nth(1).expect("token missing");
    
    // Get topic IDs from command line arguments
    let mut allowed_topics = Vec::new();
    if let Some(topic_id) = env::args().nth(2) {
        allowed_topics.push(topic_id.parse::<i32>().expect("topic_id must be a number"));
    }
    println!("Filtering for topic IDs: {:?}", allowed_topics);

    println!("Connecting to Telegram...");
    let client = Client::connect(Config {
        session: Session::load_file_or_create(SESSION_FILE)?,
        api_id,
        api_hash: api_hash.clone(),
        params: InitParams {
            // Fetch the updates we missed while we were offline
            catch_up: true,
            ..Default::default()
        },
    })
    .await?;
    println!("Connected!");

    if !client.is_authorized().await? {
        println!("Signing in...");
        client.bot_sign_in(&token).await?;
        client.session().save_to_file(SESSION_FILE)?;
        println!("Signed in!");
    }

    println!("Waiting for messages...");

    // This code uses `select` on Ctrl+C to gracefully stop the client and have a chance to
    // save the session.
    loop {
        let exit = pin!(async { tokio::signal::ctrl_c().await });
        let upd = pin!(async { client.next_update().await });

        let update = match select(exit, upd).await {
            Either::Left(_) => break,
            Either::Right((u, _)) => u?,
        };

        let handle = client.clone();
        let topics_clone = allowed_topics.clone();
        task::spawn(async move {
            match handle_update(handle, update, &topics_clone).await {
                Ok(_) => {}
                Err(e) => eprintln!("Error handling updates: {e}"),
            }
        });
    }

    println!("Saving session file and exiting...");
    client.session().save_to_file(SESSION_FILE)?;
    Ok(())
}

fn main() -> Result {
    runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async_main())
}