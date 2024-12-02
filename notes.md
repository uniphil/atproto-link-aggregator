# atproto link aggregator

global backlink lookup for records, aimed at self-hosting. it prioritizes completeness + minimal operational cost and complexity.

## testing the waters

- [x] very bad version, storing in sqlite

it... works? on a cheap fly.io machine with 4 shared vcpus and 2GB ram ($13/mo) it's happily approaching 24hrs of stable operation. this is for network volume at around the 20M bsky user mark. it has a 15GB volume attached (+$2.25/mo) which will fill soon -- it's eating around 3.6GB/day.

i haven't pushed it for read performance but at 100req/s it doesn't break a sweat. this is nice but not surprising because:

- sqlite WAL is enabled
- only two threads actually do the work of ingesting, so two are basically spares ready to serve reads

the app's inherent memory footprint is small, so 100% of the remaining machine mem ends up getting used for the block cache (sqlite, app-level) and the page cache (OS-level). i don't know if the OS or sqlite is better at managing this memory -- unclear what an optimal config is.
    -> sqlite might have a better idea what is important to keep in the cache
    -> might be able to avoid syscalls?
    -> but the os might be able to share cache across multiple connections better?

https://deletions.bsky.bad-example.com is connected to it, fetching like counts for deleted posts. this is a slightly unusual and read-friendly workload since deleted posts bias heavily toward young posts with no likes. it averages around 5 queries per second, with response times holding:

- 1.5ms average
- 2.5ms p50
- 4.6ms p90
- p99 bounces around from 5ms to about 25ms


### bad things about it

- sqlite is used as a very rough kv store. since delete events in atproto don't contain content, there's a O(n) query loop when fetching backlinks to check for each if it has been deleted.

- it only stores likes

- it's keyed by uri (wasted space for at:// etc)

- value is a concatenated string of <did>!<rkey>;<did>!<rkey>... which wastes space



## immediate todos

- [x] larger WAL size
    - this ended up hurting. impossible to tell if due to the WAL change or reboot condition.
        - tail latency shot up
        - cpu iowait went up and stayed up
        - disk io throughput apparently reduced? total io/s so maybe a win, but
        - i/o utilization unchanged
        - average queue depth reduced, but is creeping back
        - memory _quickly_ went back to looking how it was before
            - 3.5mins to fill the page cache
    - or was it a win? after about 20mins,
        - cpu is _down_ 5% points, 15 -> 10%
            - went back up
        - io utiliziation down ~10% points
            - back up or even slightly higher
        - io/s still down from ~33k/s => 22k/s or even just 20?
            - went back up
        - queue depth remains lower, ~12.5 -> 8.5
            - went back up
        - tail latency still high
            - tail latency is expected to get worse since readers have to use the wal more. but this much worse?
            - signs of it coming down
                - tail latency stayed high after 1h
        - this is while the daily event volume is coming down, so part of the improvement might be load reduction
            - no, by network volume things actually went up a bit

- [x] cache size tweaks
    - 2x (200mb) for writer connection
    - .5x change (4MB, 2x default) for readers
    - so far: nothing drastic
        - similar initial changes to last time (hints toward just a restart mode)
        - mem increase as expected
        - cpu up a bit but so is network volume
    - - -
    - 1GB for writer connection
        - not really worse but doesn't seem to be _helping_
        - maybe a small cache is good and let the os use the rest for its page cache
    - i guess it's ultimately limited by just disk io and it's not easy to reduce disk io with heavy writes?
    - - -
    - going down to 40MB
        - nah
    - back to 100mb, and default WAL size
        - wow this is better?

- [x] zstd for jetstream
- [ ] WAL commits in a bg thread?
- [ ] new sqlite schema for universal backlinks
- [ ] track jetstream cursor for replay over the gap after disconnect


## important todos

- [ ] global backfill


## design plans

### sqlite

is probably not the right tool for the job... but it's nice for a few reasons

- broad language and tooling support
- plug-in backup & on-startup replication via litestream
- easy to operate
- multi-process concurrent access

kv stores that are probably better

- rocksdb
    - c++ so a bit more annoying to build the app (fails by default for me)
    - efficient data storage
- lmdb
    - c++ and seemingly even more annoying than rocksdb to compile, + awkward api
    - apparently very fast
    - multi-process concurrent access supported
- redb
    - rust embedded persistent kv (no other lang support)
    - inspired by lmdb
    - no multi-process concurrent access
- pebbledb
    - would be starting with this if writing in golang. rewrite of rocksdb, made for cockroachdb.
    - efficient data storage.
- persy
    - ?? rust, unsure how it compares
    - secondary indexes?
- agatedb
    - still under development
    - port of badger (go) to rust
    - hoeful to become tikv's backend
- :x: sled
    - bad rap online for mem unsafety
- mongodb
    - gets hate but probably fairly good for this
    - not embedded (separate db process)
- fjall
    - wait is this the one?
        - compression built-in
        - rocks-like
    - no multi-process concurrent access
        - multi-thread ok (fine)
- surrealkv/surrealdb?
    - kv
        - has compaction


compact representation on disk is pretty important to keep costs down. i think this is a -1 for sqlite and +1 for most of the key-value stores
