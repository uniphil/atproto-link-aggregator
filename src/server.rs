use tokio::task::block_in_place;
use std::collections::HashSet;

use serde::{Serialize, Deserialize};

use axum::{Json, Router, extract, http, routing};

#[derive(Clone)]
pub struct ApiConfig {
    pub likes: fjall::TransactionalPartitionHandle,
    pub unlikes: fjall::TransactionalPartitionHandle,
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

    let mut seen_dids: HashSet<String> = HashSet::new();

    let dids: Vec<String> = match config.likes.get(&query.uri) {
        Ok(Some(rkey_dids)) => std::str::from_utf8(&rkey_dids)
            .expect("utf8 key")
            .split(';')
            .filter_map(|rkey_did| {
                if let Ok(true) = config.unlikes.contains_key(rkey_did) {
                    return None
                }
                let Some((_, did)) = rkey_did.split_once("!") else {
                    return None
                };
                if seen_dids.contains(did) {
                    return None
                }
                seen_dids.insert(did.to_string());
                Some(did.to_string())
            })
            .collect(),
        Ok(None) => vec![],
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