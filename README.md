# redis-java

A small Redis server written in Java 21+ with no dependencies. It speaks the real
RESP2 wire protocol, so `redis-cli` and normal Redis client libraries can talk to it.

## Run it

```powershell
.\build.ps1 test      # compile and run the 70 end-to-end tests
.\build.ps1 run       # start on port 6379 (add --port 6380 to change it)
.\build.ps1           # build redis-java.jar, then: java -jar redis-java.jar
```

`build.ps1` uses `JAVA_HOME`, then `javac` on PATH, then Android Studio's bundled JDK.

## How it works

| File | Role |
|------|------|
| `RespReader.java` | Parses RESP arrays (`*2\r\n$4\r\nECHO\r\n...`) and inline (telnet) commands |
| `Resp.java` | Encodes replies: `+OK`, `-ERR`, `:42`, `$5\r\nhello`, `*N` arrays |
| `Store.java` | The keyspace: a `HashMap` of entries holding a value and an optional expiry time |
| `CommandProcessor.java` | Checks argument counts, dispatches commands, runs MULTI/EXEC |
| `RedisServer.java` | TCP accept loop. Each client gets a virtual thread. Also runs the background expiry sweeper |

**Concurrency.** Real Redis runs every command on one thread, so each command is atomic.
This server gives the same guarantee with one global lock, and handles each client
connection on its own virtual thread. An EXEC block holds the lock for the whole
transaction, so other clients never see half of it.

**Expiry.** Same approach as Redis. A key is deleted lazily when something reads it
after its deadline. A background task also removes expired keys every 100 ms.

**Pipelining.** Replies are buffered and only flushed once the client has no more
input waiting, so 100 pipelined commands go out in a few writes, not 100.

## Supported commands

- **Connection:** `PING`, `ECHO`, `QUIT`
- **Strings:** `SET` (with `EX`/`PX`/`EXAT`/`PXAT`/`NX`/`XX`/`GET`/`KEEPTTL`), `GET`, `MGET`, `MSET`, `INCR`, `DECR`, `INCRBY`, `DECRBY`, `APPEND`, `STRLEN`
- **Keys:** `DEL`, `EXISTS`, `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`, `PERSIST`, `TYPE`, `KEYS` (glob patterns), `DBSIZE`, `FLUSHALL`
- **Lists:** `LPUSH`, `RPUSH`, `LPOP`, `RPOP` (with count), `LLEN`, `LRANGE`
- **Transactions:** `MULTI`, `EXEC`, `DISCARD`
- **Keys (absolute expiry):** `EXPIREAT`, `PEXPIREAT`
- **Persistence:** `SAVE`, `BGSAVE`, `LASTSAVE`, `SHUTDOWN`
- **Replication:** `WAIT`, `REPLCONF`, `PSYNC`
- **Server:** `CONFIG GET`, `INFO [section]`, `SELECT 0`, `COMMAND`

## Persistence (RDB)

The server saves snapshots in Redis's own `dump.rdb` format and loads `<dir>/<dbfilename>` on startup.

```powershell
java -jar redis-java.jar --dir C:\data --dbfilename dump.rdb --save "60 1"
```

- **`SAVE`** writes the file right away and blocks other clients until it's done.
- **`BGSAVE`** copies the keyspace while holding the lock, then writes the file on a background thread. Real Redis uses `fork()` to the same effect.
- **Save points:** `--save "<seconds> <changes> ..."` works like Redis's `save` setting. The default is `3600 1 300 100 60 10000`. `--save ""` turns auto-save off.
- **`SHUTDOWN [SAVE|NOSAVE]`** and Ctrl+C both save before exiting when save points are set.
- **`LASTSAVE`** returns the time of the last save, and `INFO` has a `# Persistence` section.

Files are written to a temp file first and then renamed, so a crash can't leave half a dump. A CRC64 checksum is written at the end and checked on load. If the dump is corrupt, the server refuses to start instead of overwriting it.

**Compatibility:** the reader handles what Redis 7 writes for strings and lists: integer-encoded and LZF-compressed strings, plain lists (type 1), quicklist/listpack lists (type 18), and expiry in seconds or milliseconds. Keys outside db 0 are skipped. Hashes, sets, sorted sets and streams are rejected with a clear error. Values are stored as UTF-8 text, so arbitrary binary values don't round-trip.

| File | Role |
|------|------|
| `Rdb.java` | RDB reader and writer: opcodes, length and string encodings |
| `Lzf.java` | LZF decompression |
| `Listpack.java` | Decodes Redis 7 listpack list nodes |
| `Crc64.java` | CRC-64/Jones checksum |

## Replication

Start any number of read-only replicas of a master:

```powershell
java -jar redis-java.jar --port 6379
java -jar redis-java.jar --port 6380 --replicaof "localhost 6379"
```

**Handshake.** The replica sends `PING`, `REPLCONF listening-port <port>`, `REPLCONF capa psync2`, then `PSYNC ? -1`. The master answers `+FULLRESYNC <replid> <offset>`, followed by a snapshot as `$<len>\r\n<RDB bytes>`. The replica loads the snapshot in place of its data, then applies the replication stream.

**The stream.** After the snapshot, the master sends each write to every replica as a RESP array:
- Writes that changed nothing, like `SET ... XX` on a missing key, aren't sent.
- Relative expiry is rewritten to an absolute time (`SET ... EX 10` becomes `SET ... PXAT <ms>`, and `EXPIRE` becomes `PEXPIREAT`), so a replica that applies it late still expires the key at the same moment as the master.
- Transactions are wrapped in `MULTI`/`EXEC`, and the replica applies them together.
- Each replica has its own send queue and writer thread, so a slow replica never blocks the master. A replica that falls 100,000 commands behind is disconnected.

**Offsets and `WAIT`.** Both sides count the stream in bytes. `WAIT <n> <timeout>` sends `REPLCONF GETACK *` to every replica. Each replica answers `REPLCONF ACK <offset>` with the bytes it had processed before that GETACK. `WAIT` returns once `n` replicas have reached the master's offset at the time of the call, or when the timeout runs out (`0` means wait forever).

**Replicas** refuse writes with `-READONLY`, reconnect every second if the link drops (each time with a full resync), and show their state in `INFO replication` (`role:slave`, `master_link_status`, `slave_repl_offset`).

| File | Role |
|------|------|
| `Replication.java` | Master side: replica links, the replication stream, offsets, ACKs for `WAIT`, `INFO replication` |
| `ReplicaClient.java` | Replica side: handshake, snapshot load, applying the stream, byte-exact offset counting |

**Limits:** every resync is a full resync (`PSYNC ? -1`). There's no replication backlog for partial resync. Each replica expires keys on its own clock instead of waiting for a `DEL` from the master. There's no `REPLICAOF` command: the role is set at startup.

## Not implemented yet

Streams, pub/sub, blocking ops (`BLPOP`), hashes, sets and sorted sets. No AOF.
