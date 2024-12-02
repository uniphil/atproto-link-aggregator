use std::sync::Arc;
use std::env;
use std::ffi::OsString;
use std::fs;
use std::thread;
use std::time;
use tokio::runtime;

mod server;
mod consumer;

const MB_IN_KB: i64 = 2_i64.pow(10);

const LIKES: redb::TableDefinition<&str, &str> = redb::TableDefinition::new("likes");
const UNLIKES: redb::TableDefinition<&str, ()> = redb::TableDefinition::new("unlikes");


fn main() {
    let rdb_path = env::var_os("RDB_PATH").unwrap_or("./links.rdb".into());
    let db_path = env::var_os("DB_PATH").unwrap_or("./links.db".into());

    let db = Arc::new(redb::Database::create(rdb_path.clone()).expect("rdb db"));
    let write_db_cache_kb = 100 * MB_IN_KB;

    thread::spawn({
        let db_path = db_path.clone();
        let db = db.clone();
        move || consumer::consume(db_path, write_db_cache_kb, db)
    });

    thread::spawn({
        move || monitor_sizes(rdb_path, db_path)
    });

    runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .max_blocking_threads(2)
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            server::serve(db).await
        });
}


fn monitor_sizes(rdb_path: OsString, db_path: OsString) {
    let start = time::SystemTime::now();
    let wal_name = {
        let mut n = db_path.clone();
        n.push("-wal");
        n
    };
    loop {
        thread::sleep(time::Duration::from_secs(15));
        let dt = start.elapsed().map(|d| d.as_secs()).unwrap();
        let rd = fs::metadata(&rdb_path).unwrap().len();
        let sq = fs::metadata(&db_path).unwrap().len() + fs::metadata(&wal_name).unwrap().len();
        println!("{dt}\t{rd}\t{sq}");
    }
}
