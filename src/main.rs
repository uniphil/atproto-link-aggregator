use std::sync::atomic::Ordering;
use std::sync::atomic::AtomicUsize;
use rusqlite::Connection;
use std::fmt::Display;
use std::fmt;
use serde::Deserialize;
use futures_util::StreamExt;
use tokio::sync::mpsc;
use tokio_tungstenite::connect_async;

const WS_URL: &str = "wss://jetstream1.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.like";

static GLOBAL_ERR_COUNT: AtomicUsize = AtomicUsize::new(0);

#[tokio::main]
async fn main() {
    let (tx, rx) = mpsc::channel(300); // absolute min 40 required w/ release build on my computer (drops some in dev build @ 40)

    tokio::task::spawn_blocking(|| { receive(rx) });
    let t = tokio::spawn(async move { consume(tx) });
    let consumer_join = t.await.expect("...");
    println!("Jetstream consumer started...");

    consumer_join.await
}

fn receive(mut receiver: mpsc::Receiver<Like>) {
    let conn = Connection::open("./links.db").expect("open sqlite3 db");
    conn.pragma_update(None, "journal_mode", "WAL").expect("wal");
    conn.pragma_update(None, "synchronous", "NORMAL").expect("synchronous normal");
    conn.pragma_update(None, "cache_size", "100000000").expect("cache bigger");
    conn.pragma_update(None, "busy_timeout", "100").expect("quick timeout");
    conn.execute(
        "CREATE TABLE IF NOT EXISTS likes (
            uri   TEXT PRIMARY KEY,
            likes BLOB NOT NULL  -- jsonb
        )",
        (),
    ).expect("create likes table");
    conn.execute(
        "CREATE TABLE IF NOT EXISTS unlikes (
            did_rkey TEXT PRIMARY KEY,
            unlikes  INTEGER NOT NULL DEFAULT 1 -- account for like -> unlike -> like -> unlike etc
        )",
        (),
    ).expect("create unlikes table");

    let mut add_stmt = conn.prepare(
        "INSERT INTO likes (uri, likes) VALUES (?1, jsonb_array(?2))
           ON CONFLICT DO UPDATE
           SET likes = json_insert(likes, '$[#]', ?2)
        "
    ).expect("prepared insert");

    let mut del_stmt = conn.prepare(
        "INSERT INTO unlikes (did_rkey) VALUES (?1)
           ON CONFLICT DO UPDATE
           SET unlikes = unlikes + 1
        "
    ).expect("prepared delete");

    let mut n = 0;

    while let Some(like) = receiver.blocking_recv() {
        match &like.commit {
            LikeCommit::Create { record, rkey } => {
                let did_rkey = format!("{}_{}", like.did, rkey);
                add_stmt.insert((record.subject.uri.clone(), did_rkey)).unwrap();
            }
            LikeCommit::Delete { rkey } => {
                let did_rkey = format!("{}_{}", like.did, rkey);
                del_stmt.execute((did_rkey,)).unwrap();
            }
        }
        n += 1;
        if n > 200 {
            println!("queued: {}", receiver.len());
            n = 0;
        }
    }
}

async fn consume(tx: mpsc::Sender<Like>) {
    let (ws_stream, _) = connect_async(WS_URL).await.expect("Failed to connect");
    let (_write, read) = ws_stream.split();
    read.for_each(|message| async {
        let js: String = message.unwrap().into_text().unwrap();
        match serde_json::from_str::<Like>(&js) {
            Ok(like) => {
                if let Err(e) = tx.send(like).await {
                    let n = GLOBAL_ERR_COUNT.fetch_add(1, Ordering::Relaxed);
                    println!("like not sent {:?} {}", e, n);
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
    }).await;
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
