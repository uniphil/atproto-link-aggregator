use redb::ReadableTable;
use std::sync::Arc;
use std::fmt;
use std::io::{Cursor, Read};
use std::thread;
use std::time;
use std::collections::HashMap;
use std::mem;
use serde::Deserialize;
use serde_json;
use flume;
use tungstenite;
use tungstenite::{Message, Error as TError};
use zstd::dict::DecoderDictionary;
use redb;

const JETSTREAM_ZSTD_DICTIONARY: &[u8] = include_bytes!("../zstd/dictionary");

const WS_URLS: [&str; 4] = [
    "wss://jetstream2.us-east.bsky.network/subscribe?compress=true&wantedCollections=app.bsky.feed.like", //&cursor=0",
    "wss://jetstream1.us-east.bsky.network/subscribe?compress=true&wantedCollections=app.bsky.feed.like", //&cursor=0",
    "wss://jetstream1.us-west.bsky.network/subscribe?compress=true&wantedCollections=app.bsky.feed.like", //&cursor=0",
    "wss://jetstream2.us-west.bsky.network/subscribe?compress=true&wantedCollections=app.bsky.feed.like", //&cursor=0",
];

pub fn consume(
    db: Arc<redb::Database>,
) {

    // the core of synchronization between consuming the firehose and writing to sqlite is this channel
    // - it's sync, so memory consumption by the channel itself is bounded
    // - capacity 1: as something is ready to write, the sqlite writer never has to wait
    // - firehose consumer always does try_send, aggregating into an update object if the queue is full
    // -> ie., the consumer never waits for the channel
    // assuming the overhad of using the channel itself is small then the intention here is that
    // - the sqlite writer only ever waits if there is nothing ready for it to persist
    // - the jetstream consumer only ever waits if there are no new messages for it
    let (sender, receiver) = flume::bounded(1);

    let jetstreamer_handle = thread::spawn(move || consume_jetstream(sender));
    let sqlizer_handle = thread::spawn(move || persist_links(db, receiver));

    for t in [jetstreamer_handle, sqlizer_handle] {
        let _ = t.join();
    }
}

fn consume_jetstream(sender: flume::Sender<Update>) {
    let dict = DecoderDictionary::copy(JETSTREAM_ZSTD_DICTIONARY);
    let mut connect_retries = 0;
    let mut update = Update::new();
    'outer: loop {
        let stream = WS_URLS[connect_retries % WS_URLS.len()];
        println!("jetstream connecting, attempt #{connect_retries}: {stream}...");
        let mut socket = match tungstenite::connect(stream) {
            Ok((socket, _)) => {
                println!("jetstream connected.");
                connect_retries = 0;
                socket
            }
            Err(e) => {
                connect_retries += 1;
                if connect_retries >= 7 {
                    break
                }
                let backoff = time::Duration::from_secs(connect_retries.try_into().unwrap());
                eprintln!("jetstream failed to connect: {e:?}. backing off {backoff:?} before retrying...");
                thread::sleep(backoff);
                continue
            }
        };

        loop {
            let b = match socket.read() {
                Ok(Message::Binary(b)) => b,
                Ok(Message::Text(_)) => {
                    eprintln!("jetstream: unexpected text message, should be binary for compressed (ignoring)");
                    continue
                }
                Ok(Message::Close(f)) => {
                    println!("jetstream: closing the connection: {f:?}");
                    continue
                }
                Ok(m) => {
                    eprintln!("jetstream: unexpected from read (ignoring): {m:?}");
                    continue
                }
                Err(TError::ConnectionClosed) => { // clean exit
                    println!("jetstream closed the websocket cleanly.");
                    break
                }
                Err(TError::AlreadyClosed) => { // programming error
                    eprintln!("jetstream: got AlreadyClosed trying to .read() websocket. probably a bug.");
                    break
                }
                Err(TError::Capacity(e)) => {
                    eprintln!("jetstream: capacity error (ignoring): {e:?}");
                    continue
                }
                Err(TError::Utf8) => {
                    eprintln!("jetstream: utf8 error (ignoring)");
                    continue
                }
                Err(e) => {
                    eprintln!("jetstream: could not read message from socket. closing: {e:?}");
                    match socket.close(None) {
                        Err(TError::ConnectionClosed) => {
                            println!("jetstream closed the websocket cleanly.");
                            break
                        }
                        r => eprintln!("jetstream: close result after error: {r:?}"),
                    }
                    // if we didn't immediately get ConnectionClosed, we should keep polling read
                    // until we get it.
                    continue
                }
            };
            let mut cursor = Cursor::new(b);
            let mut decoder = match zstd::stream::Decoder::with_prepared_dictionary(&mut cursor, &dict) {
                Ok(d) => d,
                Err(e) => {
                    eprintln!("jetstream: failed to decompress zstd message: {e:?}");
                    continue
                }
            };
            let mut s = String::new();
            if let Err(e) = decoder.read_to_string(&mut s) {
                eprintln!("jetstream: failed to decode zstd: {e:?}");
                continue
            };

            match serde_json::from_str::<Like>(&s) {
                Ok(like) => {
                    update.add(like)
                }
                Err(e) => {
                    if !s.contains("\"identity\"") &&
                       !s.contains("\"account\"") {
                        println!("failed on {:?} for {:?}", s, e)
                    }
                }
            }
            if sender.is_full() {
                continue // sqlite is not ready, keep accumulating
            }
            if let Err(flume::SendError(rejected)) = sender.send(mem::take(&mut update)) {
                if sender.is_disconnected() {
                    eprintln!("send channel disconnected -- nothing to do, bye.");
                    break 'outer
                }
                eprintln!("send channel check said not full but failed to send. a bug / are there somehow multiple senders?");
                update = rejected;
            }
        }
    }
}

fn persist_links(db: Arc<redb::Database>, receiver: flume::Receiver<Update>) {
    let mut nnn = 0;

    println!("receiver ready.");
    for update in receiver.into_iter() {
        nnn += 1;
        let unlikes = update.unlikes;
        let likes = update.likes
            .into_iter()
            .map(|(uri, likers)| (uri, likers.join(";"))) // semicolon is disallowed in both dids and rkeys
            .collect::<Vec<(String, String)>>();

        let mut tx = db.begin_write().unwrap();
        if nnn & 0b11111 == 0 {
            tx.set_durability(redb::Durability::Eventual)
        } else {
            tx.set_durability(redb::Durability::None);
        }
        {
            let mut likes_table = tx.open_table(crate::LIKES).unwrap();
            let mut unlikes_table = tx.open_table(crate::UNLIKES).unwrap();

            for (uri, likes) in likes {
                let combined = match likes_table.get(&*uri).unwrap() {
                    Some(existing) => {
                        let ex = existing.value();
                        format!("{ex};{likes}")
                    }
                    None => likes,
                };
                likes_table.insert(&*uri, &*combined).unwrap();
            }

            for rkey_did in unlikes {
                unlikes_table.insert(&*rkey_did, ()).unwrap();
            }
        }

        if let Err(e) = tx.commit() {
            eprintln!("failed to commit transaction. we will lose a batch of updates: {e:?}");
        }
    }
}

#[derive(Default)]
struct Update {
    likes: HashMap<String, Vec<String>>,
    unlikes: Vec<String>,
}

impl Update {
    fn new() -> Self {
        Default::default()
    }
    fn add(&mut self, like: Like) {
        match &like.commit {
            LikeCommit::Create { record, rkey } => {
                let rkey_did = format!("{}!{}", rkey, like.did); // ! is not allowed in at-uri or record keys
                (*self.likes
                    .entry(record.subject.uri.clone())
                    .or_insert_with(|| Vec::new())
                ).push(rkey_did);
            }
            LikeCommit::Delete { rkey } => {
                let rkey_did = format!("{}!{}", rkey, like.did);
                self.unlikes.push(rkey_did);
            }
        }
    }
}


#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename = "commit")]
struct Like {
    did: String,
    commit: LikeCommit
}

#[derive(Debug, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
enum LikeCommit {
    Create {
        rkey: String,
        record: CreateLikeCommitRecord,
    },
    Delete {
        rkey: String,
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "$type", rename = "app.bsky.feed.like")]
struct CreateLikeCommitRecord {
    subject: LikeCommitSubject
}

#[derive(Debug, Deserialize)]
struct LikeCommitSubject {
    uri: String
}

impl fmt::Display for Like {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.commit {
            LikeCommit::Create { record, .. } => write!(f, "{} liked {}", self.did, record.subject.uri),
            LikeCommit::Delete { rkey } => write!(f, "{} unliked {}", self.did, rkey)
        }
    }
}
