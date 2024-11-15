use std::ffi::OsString;
use std::sync::atomic::Ordering;
use std::sync::atomic::AtomicUsize;
use rusqlite::Connection;
use std::collections::HashSet;
use std::env;
use std::fmt::Display;
use std::fmt;
use serde::{Serialize, Deserialize};
use futures_util::StreamExt;
use tokio::sync::mpsc;
use tokio::task::{spawn_blocking, block_in_place};
use tokio_tungstenite::connect_async;
use axum::{Json, Router, extract, http, routing};

const WS_URL: &str = "wss://jetstream1.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.like";

static GLOBAL_ERR_COUNT: AtomicUsize = AtomicUsize::new(0);

#[derive(Clone)]
struct Config {
    db_path: OsString,
}

#[tokio::main]
async fn main() {
    let config = Config {
        db_path: env::var_os("DB_PATH").unwrap_or("./links.db".into()),
    };

    let (tx, rx) = mpsc::channel(300); // absolute min 40 required w/ release build on my computer (drops some in dev build @ 40)
    spawn_blocking({
        let config = config.clone();
        || { receive(rx, config) }
    });
    let t = tokio::spawn(async move { consume(tx) });
    let consumer_join = t.await.expect("...");
    println!("Jetstream consumer started...");

    let t_api = tokio::spawn(async { serve(config) });
    let api_join = t_api.await.expect(".....");
    println!("API server started...");

    tokio::select! {
        _ = consumer_join => {
            println!("consumer stopped");
        }
        _ = api_join => {
            println!("api server stopped");
        }
    }
}

async fn serve(config: Config) {
    println!("api starting...");
    let app = Router::new()
        .route("/", routing::get(hello))
        .route("/likes", routing::get({
            let config = config.clone();
            move |query| async { block_in_place(|| { get_likes_sync(query, config) }) }
        }));

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.expect("tcp works");
    println!("api ready.");
    axum::serve(listener, app).await.expect("axum starts");
}
async fn hello() -> &'static str {
    "hi\n"
}

#[derive(Deserialize)]
struct GetLikesQuery {
    uri: String
}
#[derive(Serialize)]
struct LikesSummary {
    total_likes: usize,
    latest_dids: Vec<String>,
}
fn get_likes_sync(query: extract::Query<GetLikesQuery>, config: Config) -> Result<Json<LikesSummary>, http::StatusCode> {
    let conn = Connection::open(config.db_path).expect("open sqlite3 db");
    conn.pragma_update(None, "cache_size", "10000000").expect("cache a bit bigger");
    conn.pragma_update(None, "busy_timeout", "1000").expect("some timeout");

    let mut like_did_rkeys = conn.prepare("
        SELECT likes.did_rkey
        FROM (
            SELECT did_rkey.value AS 'did_rkey'
            FROM likes, json_each(likes.likes) AS did_rkey
            WHERE uri = ?1) AS likes
        LEFT OUTER JOIN unlikes ON (unlikes.did_rkey = likes.did_rkey)
        WHERE unlikes.did_rkey Is NULL;
        ").unwrap();
    let Ok(did_rkey_rows) = like_did_rkeys.query_map((&query.uri,), |row| row.get(0)) else {
        return Err(http::StatusCode::INTERNAL_SERVER_ERROR)
    };

    let dids = {
        let mut dids: Vec<String> = vec![];
        let mut seen_dids: HashSet<String> = HashSet::new();

        for did_rkey in did_rkey_rows {
            let Ok(did_rkey): Result<String, rusqlite::Error> = did_rkey else {
                continue
            };
            let Some((did, _)) = did_rkey.split_once("!") else {
                continue
            };
            if seen_dids.contains(did) {
                continue
            }
            dids.push(did.to_string());
            seen_dids.insert(did.to_string());
        }
        dids
    };

    let total_likes = dids.len();
    let latest_dids = if total_likes <= 6 {
        dids
    } else {
        dids[total_likes - 6..].to_vec()
    };

    Ok(axum::Json(LikesSummary { total_likes, latest_dids }))
}

fn receive(mut receiver: mpsc::Receiver<Like>, config: Config) {
    let conn = Connection::open(config.db_path).expect("open sqlite3 db");
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
            did_rkey TEXT PRIMARY KEY
        )",
        (),
    ).expect("create unlikes table");

    let mut add_stmt = conn.prepare(
        "INSERT INTO likes (uri, likes) VALUES (?1, jsonb_array(?2))
           ON CONFLICT DO UPDATE
           SET likes = jsonb_insert(likes, '$[#]', ?2)
        "
    ).expect("prepared insert");

    let mut del_stmt = conn.prepare(
        "INSERT INTO unlikes (did_rkey) VALUES (?1)
           ON CONFLICT DO NOTHING
        "
    ).expect("prepared delete");

    println!("receiver ready.");

    while let Some(like) = receiver.blocking_recv() {
        match &like.commit {
            LikeCommit::Create { record, rkey } => {
                let did_rkey = format!("{}!{}", like.did, rkey); // ! is not allowed in at-uri or record keys
                add_stmt.insert((record.subject.uri.clone(), did_rkey)).unwrap();
            }
            LikeCommit::Delete { rkey } => {
                let did_rkey = format!("{}!{}", like.did, rkey);
                del_stmt.execute((did_rkey,)).unwrap();
            }
        }
    }
}

async fn consume(tx: mpsc::Sender<Like>) {
    println!("jetstream connecting...");
    let (ws_stream, res) = connect_async(WS_URL).await.expect("Failed to connect");
    println!("jetstream connected. server replied: {:?}", res);
    let (_write, read) = ws_stream.split();
    println!("consuming jetstream...");
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
