use std::sync::atomic::Ordering;
use std::sync::atomic::AtomicUsize;
use rusqlite::Connection;
use std::fmt::Display;
use std::fmt;
use serde::Deserialize;
use futures_util::StreamExt;
// use tokio::io::AsyncWriteExt;
use std::sync::mpsc::{sync_channel, Receiver};
use std::thread;
use tokio_tungstenite::connect_async;

const WS_URL: &str = "wss://jetstream1.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.like";

static GLOBAL_ERR_COUNT: AtomicUsize = AtomicUsize::new(0);

#[tokio::main]
async fn main() {
    let (sender, receiver) = sync_channel(200); // absolute min 40 required w/ release build on my computer (drops some in dev build @ 40)

    thread::spawn(move || receive(receiver));

    let (ws_stream, _) = connect_async(WS_URL).await.expect("Failed to connect");
    let (_write, read) = ws_stream.split();
    read.for_each( |message| async {
        let js: String = message.unwrap().into_text().unwrap();
        match serde_json::from_str::<Like>(&js) {
            //Ok(like) => println!("{}", like),
            Ok(like) => match sender.try_send(like) {
                Ok(_) => {}
                Err(_) => {
                    let n = GLOBAL_ERR_COUNT.fetch_add(1, Ordering::Relaxed);
                    println!("like not sent (full prob) {}", n);
                    if n > 100 {
                        panic!("bye");
                    }
                }
            }
            Err(e) => {
                if !js.contains("\"identity\"") &&
                   !js.contains("\"account\"") {
                    println!("failed on {:?} for {:?}", js, e)
                }
            }
        }
        // tokio::io::stdout().write_all(&format!("{:?}",x).as_bytes()).await.unwrap();
    }).await;
    println!("Hello, world!");
}

// #[derive(Debug)]
// struct StoredLike {
//     uri: String,
//     did: String,
//     rkey: String,
// }

fn receive(receiver: Receiver<Like>) {
    let conn = Connection::open("./links.db").expect("open sqlite3 db");
    conn.pragma_update(None, "journal_mode", "WAL").expect("wal");
    conn.pragma_update(None, "synchronous", "NORMAL").expect("synchronous normal");
    conn.pragma_update(None, "cache_size", "100000000").expect("cache bigger");
    conn.pragma_update(None, "busy_timeout", "100").expect("quick timeout");
    conn.execute(
        "CREATE TABLE IF NOT EXISTS like (
            uri   TEXT NOT NULL,
            did   TEXT NOT NULL,
            rkey  TEXT NOT NULL
        )",
        (),
    ).expect("create table");
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS like_did_rkey
            ON like (did, rkey)",
        (),
    ).expect("create did + rkey index");
    conn.execute(
        "CREATE INDEX IF NOT EXISTS like_uri
            ON like (uri)",
        (),
    ).expect("create did + rkey index");

    let mut insert_stmt = conn.prepare("INSERT INTO like (uri, did, rkey) VALUES (?1, ?2, ?3)").expect("prepared insert");
    let mut del_stmt = conn.prepare("DELETE FROM like WHERE did = ?1 AND rkey = ?2").expect("prepared delete");

    for like in receiver.iter() {
        match &like.commit {
            LikeCommit::Create { record, rkey } => {
                insert_stmt.insert((record.subject.uri.clone(), like.did, rkey)).unwrap();
            }
            LikeCommit::Delete { rkey } => {
                del_stmt.execute((like.did, rkey)).unwrap();
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

impl Display for Like {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.commit {
            LikeCommit::Create { record, .. } => write!(f, "{} liked {}", self.did, record.subject.uri),
            LikeCommit::Delete { rkey } => write!(f, "{} unliked {}", self.did, rkey)
        }
    }
}
