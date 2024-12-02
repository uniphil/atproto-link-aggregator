use std::sync::Arc;
use std::env;
use std::thread;
use tokio::runtime;

mod server;
mod consumer;

const LIKES: redb::TableDefinition<&str, &str> = redb::TableDefinition::new("likes");
const UNLIKES: redb::TableDefinition<&str, ()> = redb::TableDefinition::new("unlikes");


fn main() {
    let rdb_path = env::var_os("RDB_PATH").unwrap_or("./links.rdb".into());

    let db = Arc::new(redb::Database::create(rdb_path.clone()).expect("rdb db"));

    thread::spawn({
        let db = db.clone();
        move || consumer::consume(db)
    });

    runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .max_blocking_threads(2)
        .enable_all()
        .build()
        .unwrap()
        .block_on(async { server::serve(db).await });
}
