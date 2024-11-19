use tokio::task::block_in_place;
use rusqlite::Connection;
use std::collections::HashSet;
use std::ffi::OsString;

use serde::{Serialize, Deserialize};

use axum::{Json, Router, extract, http, routing};

#[derive(Clone)]
pub struct ApiConfig {
    pub db_path: OsString,
    pub read_db_cache_kb: i64,
}

pub async fn serve(config: ApiConfig) {
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
#[derive(Serialize, Default)]
struct LikesSummary {
    total_likes: usize,
    latest_dids: Vec<String>,
}
fn get_likes_sync(query: extract::Query<GetLikesQuery>, config: ApiConfig) -> Result<Json<LikesSummary>, http::StatusCode> {
    let conn = Connection::open(config.db_path).expect("open sqlite3 db");
    conn.pragma_update(None, "cache_size", (-config.read_db_cache_kb).to_string()).expect("cache a bit bigger"); // positive = *pages*, neg = bytes
    conn.pragma_update(None, "busy_timeout", "1000").expect("some timeout");

    let mut unliked_stmt = conn.prepare("SELECT 1 FROM unlikes WHERE did_rkey = ?1").expect("prepare unlike statemtn");

    let mut seen_dids: HashSet<String> = HashSet::new();

    let dids: Vec<String> = match conn.query_row(
        "SELECT likes.likes FROM likes WHERE uri = ?1",
        (&query.uri,),
        |row| row.get::<usize, String>(0),
    ) {
        Ok(did_rkeys) => did_rkeys
            .split(';')
            .filter_map(|did_rkey| {
                if let Ok(true) = unliked_stmt.exists((did_rkey,)) {
                    return None
                }
                let Some((did, _)) = did_rkey.split_once("!") else {
                    return None
                };
                if seen_dids.contains(did) {
                    return None
                }
                seen_dids.insert(did.to_string());
                Some(did.to_string())
            })
            .collect(),
        Err(rusqlite::Error::QueryReturnedNoRows) => {
            return Ok(axum::Json(Default::default()))
        }
        Err(e) => {
            eprintln!("failed to get get likes from sqlite: {e:?}");
            return Err(http::StatusCode::INTERNAL_SERVER_ERROR)
        }
    };

    let total_likes = dids.len();
    let latest_dids = if total_likes <= 6 {
        dids
    } else {
        dids[total_likes - 6..].to_vec()
    };

    Ok(axum::Json(LikesSummary { total_likes, latest_dids }))
}