use std::env;
use std::thread;
use tokio::runtime;

mod server;
mod consumer;

const MB_IN_KB: i64 = 2_i64.pow(10);

fn main() {
    let db_path = env::var_os("DB_PATH").unwrap_or("./links.db".into());
    let write_db_cache_kb = 100 * MB_IN_KB;
    let read_db_cache_kb = 10 * MB_IN_KB;

    thread::spawn({
        let db_path = db_path.clone();
        move || consumer::consume(db_path, write_db_cache_kb)
    });

    runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .max_blocking_threads(2)
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            server::serve(server::ApiConfig { db_path, read_db_cache_kb }).await
        });
}
