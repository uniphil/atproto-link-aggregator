use std::env;
use std::ffi::OsString;
use std::fs;
use std::thread;
use std::time;
use tokio::runtime;

mod server;
mod consumer;

const MB_IN_KB: i64 = 2_i64.pow(10);

fn main() {
    let fj_db_path = env::var_os("FJ_DB_PATH").unwrap_or("./links.fjall".into());
    let db_path = env::var_os("DB_PATH").unwrap_or("./links.db".into());

    let keyspace = fjall::Config::new(fj_db_path.clone()).open_transactional().expect("fjall db");
    let likes = keyspace.open_partition("likes", Default::default()).expect("likes partition");
    let unlikes = keyspace.open_partition("unlikes", Default::default()).expect("unlikes partition");
    let write_db_cache_kb = 100 * MB_IN_KB;

    thread::spawn({
        let db_path = db_path.clone();
        let likes = likes.clone();
        let unlikes = unlikes.clone();
        let keyspace = keyspace.clone();
        move || consumer::consume(db_path, write_db_cache_kb, likes, unlikes, keyspace)
    });

    thread::spawn({
        let keyspace = keyspace.clone();
        move || monitor_sizes(keyspace, db_path)
    });

    runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .max_blocking_threads(2)
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            server::serve(server::ApiConfig { likes, unlikes }).await
        });
}


fn monitor_sizes(keyspace: fjall::TransactionalKeyspace, db_path: OsString) {
    let start = time::SystemTime::now();
    let wal_name = {
        let mut n = db_path.clone();
        n.push("-wal");
        n
    };
    loop {
        thread::sleep(time::Duration::from_secs(15));
        keyspace.persist(fjall::PersistMode::Buffer).unwrap();
        let dt = start.elapsed().map(|d| d.as_secs()).unwrap();
        let ks = keyspace.disk_space() + keyspace.write_buffer_size();
        let sq = fs::metadata(&db_path).unwrap().len() + fs::metadata(&wal_name).unwrap().len();
        println!("{dt}\t{ks}\t{sq}");
    }
}
