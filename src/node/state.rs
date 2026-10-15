use std::path::Path;

use anyhow::{Context, Result};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{FromRow, Row, SqlitePool};

#[derive(Debug, Clone, FromRow)]
pub struct CachedChannel {
    pub idx: i64,
    pub name: String,
    pub config: Option<Vec<u8>>,
}

/// A logged message frame, kept as the exact bytes the radio (or the proxy's
/// synthetic echo) put on the wire, so a client that syncs later can be handed
/// the same frame every other client already saw.
#[derive(Debug, Clone, FromRow)]
pub struct StoredFrame {
    pub id: i64,
    pub raw: Vec<u8>,
}

/// One message to log. A struct rather than an eight-argument call because a
/// frame carries three different clocks/handles that are easy to transpose:
/// `raw` is the payload, `timestamp` is the *sender's* RTC, and `recv_ts` is
/// when the proxy saw it.
pub struct NewMessage<'a> {
    /// `channel` or `contact`. Only `channel` is replayed today.
    pub msg_type: &'a str,
    pub from_key: Option<&'a [u8]>,
    pub channel_idx: Option<i64>,
    pub text: &'a str,
    /// Sender's RTC clock, taken from the frame. Not trustworthy: the firmware
    /// itself notes it "could be wrong" and an unset clock reads as zero.
    pub timestamp: i64,
    /// Whole frame payload, replayed verbatim.
    pub raw: &'a [u8],
    /// Frame code of `raw` (0x08/0x11/...). Recorded so a human reading the
    /// table can tell which frame form `raw` is without re-parsing it.
    pub code: u8,
    /// Proxy receive time. This is the clock retention is measured against;
    /// never `timestamp`.
    pub recv_ts: i64,
}

pub struct NodeState {
    pool: SqlitePool,
}

impl NodeState {
    pub async fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();

        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true);

        // Single connection on purpose: the Core actor serializes all DB
        // access (one task owns &NodeState through Arc and only one task
        // ever calls into it at a time), so concurrent connections would
        // sit idle. Using the pool API instead of a bare SqliteConnection
        // keeps the ergonomic sqlx::query(&self.pool) call sites.
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .context("failed to open SQLite database")?;

        let state = Self { pool };

        state.migrate().await?;
        Ok(state)
    }

    async fn migrate(&self) -> Result<()> {
        // Contacts are deliberately not stored. The radio answers `GET_CONTACTS`
        // with a `since` filter and the client keeps the watermark, so a table
        // here could only ever be a third copy -- one that, once complete, would
        // answer the request itself and therefore never see another delta to
        // refresh it from. Contacts are multiplexed, not cached:
        // `Core::contacts` fans a reply in flight out to everyone asking.
        // Derived state, so dropping the table an earlier build kept is lossless.
        sqlx::query("DROP TABLE IF EXISTS contacts")
            .execute(&self.pool)
            .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS channels (
                idx INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                config BLOB
            )",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                msg_type TEXT NOT NULL,
                from_key BLOB,
                channel_idx INTEGER,
                text TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                raw BLOB,
                code INTEGER,
                recv_ts INTEGER
            )",
        )
        .execute(&self.pool)
        .await?;

        // `raw`/`code`/`recv_ts` arrived with message history. They are
        // declared nullable above only because `ALTER TABLE ADD COLUMN` cannot
        // add a NOT NULL column without a default; every insert supplies all
        // three.
        //
        // Rows written before the history feature carry no `raw` and so can
        // never be replayed. Drop them rather than backfill: nothing has ever
        // read this table, so the deletion is lossless in practice and it also
        // cleans up whatever had accumulated on an existing deployment.
        let legacy_rows = self.ensure_column("messages", "raw", "BLOB").await?;
        self.ensure_column("messages", "code", "INTEGER").await?;
        self.ensure_column("messages", "recv_ts", "INTEGER").await?;
        if legacy_rows {
            let dropped = sqlx::query("DELETE FROM messages WHERE raw IS NULL")
                .execute(&self.pool)
                .await?
                .rows_affected();
            if dropped > 0 {
                tracing::info!(
                    rows = dropped,
                    "dropped unreplayable pre-history message rows"
                );
            }
        }

        // The retention sweep's predicate. Without it every prune is a full
        // table scan, on a pool deliberately pinned to one connection.
        sqlx::query("CREATE INDEX IF NOT EXISTS messages_recv_ts ON messages(recv_ts)")
            .execute(&self.pool)
            .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS kv_store (
                key TEXT PRIMARY KEY,
                value BLOB NOT NULL
            )",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS raw_rx (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts INTEGER NOT NULL,
                code INTEGER NOT NULL,
                payload BLOB NOT NULL
            )",
        )
        .execute(&self.pool)
        .await?;

        // Drop obsolete kv_store entries from prior versions. self_info/
        // device_info/battery_info used to be JSON; identity.* was a SHA256
        // pseudo-keypair that no longer exists.
        sqlx::query(
            "DELETE FROM kv_store WHERE key IN \
             ('self_info', 'device_info', 'battery_info', \
              'identity.seed', 'identity.pk')",
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Add a column if the table predates it. SQLite has no
    /// `ADD COLUMN IF NOT EXISTS`, so the schema has to be inspected first.
    /// Returns true when the column was missing and has just been created.
    async fn ensure_column(&self, table: &str, column: &str, decl: &str) -> Result<bool> {
        let rows = sqlx::query(&format!("PRAGMA table_info({table})"))
            .fetch_all(&self.pool)
            .await?;
        if rows.iter().any(|r| r.get::<String, _>("name") == column) {
            return Ok(false);
        }
        sqlx::query(&format!("ALTER TABLE {table} ADD COLUMN {column} {decl}"))
            .execute(&self.pool)
            .await?;
        Ok(true)
    }

    // --- Channels ---

    pub async fn upsert_channel(&self, channel: &CachedChannel) -> Result<()> {
        sqlx::query("INSERT OR REPLACE INTO channels (idx, name, config) VALUES (?1, ?2, ?3)")
            .bind(channel.idx)
            .bind(&channel.name)
            .bind(&channel.config)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn delete_channel(&self, idx: i64) -> Result<()> {
        sqlx::query("DELETE FROM channels WHERE idx = ?1")
            .bind(idx)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn get_channels(&self) -> Result<Vec<CachedChannel>> {
        let rows = sqlx::query_as::<_, CachedChannel>("SELECT * FROM channels ORDER BY idx")
            .fetch_all(&self.pool)
            .await?;
        Ok(rows)
    }

    // --- Messages ---

    pub async fn insert_message(&self, msg: &NewMessage<'_>) -> Result<i64> {
        let result = sqlx::query(
            "INSERT INTO messages
                 (msg_type, from_key, channel_idx, text, timestamp, raw, code, recv_ts)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        )
        .bind(msg.msg_type)
        .bind(msg.from_key)
        .bind(msg.channel_idx)
        .bind(msg.text)
        .bind(msg.timestamp)
        .bind(msg.raw)
        .bind(msg.code as i64)
        .bind(msg.recv_ts)
        .execute(&self.pool)
        .await?;
        Ok(result.last_insert_rowid())
    }

    /// Newest logged row id, or `None` when the log is empty. This is the
    /// snapshot bound: a client replaying history stops here, because
    /// anything newer was broadcast to it live.
    pub async fn latest_message_id(&self) -> Result<Option<i64>> {
        let row = sqlx::query("SELECT MAX(id) FROM messages")
            .fetch_one(&self.pool)
            .await?;
        Ok(row.get(0))
    }

    /// Oldest replayed channel frame newer than `after_id`, no newer than
    /// `until_id` and no older than `since_ts`. Ordered by `id` so a caller can
    /// walk the log one frame per poll, advancing `after_id` to the returned
    /// id.
    ///
    /// `until_id` is the caller's snapshot bound: replaying past it would hand
    /// a client a frame it already received as a live broadcast.
    ///
    /// Only channel messages are replayed: contact frames come in two
    /// generations with a variable-length sender prefix, which is not
    /// something a history reader should have to unpick.
    pub async fn next_channel_frame(
        &self,
        after_id: i64,
        until_id: i64,
        since_ts: i64,
    ) -> Result<Option<StoredFrame>> {
        let row = sqlx::query_as::<_, StoredFrame>(
            "SELECT id, raw FROM messages
             WHERE msg_type = 'channel' AND raw IS NOT NULL
               AND id > ?1 AND id <= ?2 AND recv_ts >= ?3
             ORDER BY id LIMIT 1",
        )
        .bind(after_id)
        .bind(until_id)
        .bind(since_ts)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    /// Drop logged messages received before `before_ts`. Returns the number of
    /// rows removed.
    pub async fn prune_messages(&self, before_ts: i64) -> Result<u64> {
        let result = sqlx::query("DELETE FROM messages WHERE recv_ts < ?1")
            .bind(before_ts)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected())
    }

    /// Persisted replay cursor for a named listener, as a message row id.
    ///
    /// Keyed by listener *name* rather than port, which is what makes renaming
    /// a listener equivalent to resetting its history.
    pub async fn history_cursor(&self, listener: &str) -> Result<Option<i64>> {
        let Some(bytes) = self.kv_get(&history_cursor_key(listener)).await? else {
            return Ok(None);
        };
        Ok(<[u8; 8]>::try_from(bytes.as_slice())
            .ok()
            .map(i64::from_le_bytes))
    }

    /// Persist a listener's replay cursor.
    ///
    /// Cursors only ever move forward, and every client on a listener writes
    /// the same key. A stale writer — the reaper retiring a client that has
    /// already gone away, or shutdown flushing several entries for one listener
    /// in arbitrary order — must therefore not drag the cursor back to a frame
    /// the listener has already consumed.
    pub async fn set_history_cursor(&self, listener: &str, id: i64) -> Result<()> {
        if let Some(current) = self.history_cursor(listener).await?
            && current >= id
        {
            return Ok(());
        }
        self.kv_set(&history_cursor_key(listener), &id.to_le_bytes())
            .await
    }

    // --- Key-value store ---

    pub async fn kv_set(&self, key: &str, value: &[u8]) -> Result<()> {
        sqlx::query("INSERT OR REPLACE INTO kv_store (key, value) VALUES (?1, ?2)")
            .bind(key)
            .bind(value)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn kv_get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let row = sqlx::query("SELECT value FROM kv_store WHERE key = ?1")
            .bind(key)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|r| r.get(0)))
    }

    // --- Raw RX recording ---

    pub async fn insert_raw_rx(&self, code: u8, payload: &[u8]) -> Result<()> {
        let ts = chrono::Utc::now().timestamp();
        sqlx::query("INSERT INTO raw_rx (ts, code, payload) VALUES (?1, ?2, ?3)")
            .bind(ts)
            .bind(code as i64)
            .bind(payload)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}

fn history_cursor_key(listener: &str) -> String {
    format!("history.cursor.{listener}")
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn test_state(name: &str) -> (NodeState, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!(
            "coresplitter-state-test-{}-{name}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("state.db");
        let state = NodeState::open(&path).await.unwrap();
        (state, path)
    }

    fn message<'a>(raw: &'a [u8], msg_type: &'a str, recv_ts: i64) -> NewMessage<'a> {
        NewMessage {
            msg_type,
            from_key: None,
            channel_idx: Some(0),
            text: "text",
            timestamp: recv_ts,
            raw,
            code: raw[0],
            recv_ts,
        }
    }

    #[tokio::test]
    async fn prune_messages_deletes_only_rows_outside_the_window() {
        let (state, _) = test_state("prune").await;
        let now = chrono::Utc::now().timestamp();
        for (raw, ts) in [
            (b"\x11old".as_slice(), now - 3 * 3600),
            (b"\x11mid".as_slice(), now - 3600),
            (b"\x11new".as_slice(), now),
        ] {
            state
                .insert_message(&message(raw, "channel", ts))
                .await
                .unwrap();
        }

        assert_eq!(state.prune_messages(now - 2 * 3600).await.unwrap(), 1);
        let oldest_kept = state
            .next_channel_frame(0, i64::MAX, 0)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(oldest_kept.raw, b"\x11mid".to_vec());
    }

    #[tokio::test]
    async fn channel_history_skips_contacts_and_honors_cursor_and_window() {
        let (state, _) = test_state("history").await;
        let now = chrono::Utc::now().timestamp();
        state
            .insert_message(&message(b"\x10contact", "contact", now))
            .await
            .unwrap();
        let stale = state
            .insert_message(&message(b"\x11stale", "channel", now - 2 * 3600))
            .await
            .unwrap();
        let fresh = state
            .insert_message(&message(b"\x11fresh", "channel", now))
            .await
            .unwrap();

        // The stale frame is inside the log but outside a one-hour window,
        // and the contact frame is never replayed.
        let frame = state
            .next_channel_frame(0, i64::MAX, now - 3600)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(frame.id, fresh);
        assert_eq!(frame.raw, b"\x11fresh".to_vec());
        assert!(
            state
                .next_channel_frame(frame.id, i64::MAX, now - 3600)
                .await
                .unwrap()
                .is_none()
        );

        // Widening the window reaches the stale frame, still in id order.
        let frame = state
            .next_channel_frame(0, i64::MAX, 0)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(frame.id, stale);

        // The snapshot bound excludes frames newer than it.
        assert!(
            state
                .next_channel_frame(0, stale, 0)
                .await
                .unwrap()
                .unwrap()
                .id
                == stale
        );
        assert!(
            state
                .next_channel_frame(stale, fresh - 1, 0)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn history_cursors_are_independent_per_listener() {
        let (state, _) = test_state("cursor").await;
        assert_eq!(state.history_cursor("phone").await.unwrap(), None);

        state.set_history_cursor("phone", 42).await.unwrap();
        state.set_history_cursor("tablet", 7).await.unwrap();
        assert_eq!(state.history_cursor("phone").await.unwrap(), Some(42));
        assert_eq!(state.history_cursor("tablet").await.unwrap(), Some(7));

        state.set_history_cursor("phone", 43).await.unwrap();
        assert_eq!(state.history_cursor("phone").await.unwrap(), Some(43));
        assert_eq!(state.history_cursor("tablet").await.unwrap(), Some(7));
    }

    /// A listener's clients share one cursor, and the reaper can flush a client
    /// that went away after a later client already advanced it. Writing its
    /// older position back would replay frames the listener already consumed.
    #[tokio::test]
    async fn history_cursor_never_moves_backwards() {
        let (state, _) = test_state("cursor-monotonic").await;

        state.set_history_cursor("phone", 43).await.unwrap();
        state.set_history_cursor("phone", 12).await.unwrap();
        assert_eq!(state.history_cursor("phone").await.unwrap(), Some(43));

        state.set_history_cursor("phone", 44).await.unwrap();
        assert_eq!(state.history_cursor("phone").await.unwrap(), Some(44));
    }

    /// A deployment upgrading from before the history feature must not keep
    /// rows it can never replay, and must end up with the new columns usable.
    #[tokio::test]
    async fn pre_history_rows_are_dropped_and_new_columns_added() {
        let dir =
            std::env::temp_dir().join(format!("coresplitter-state-migrate-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("state.db");

        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(&format!("sqlite:{}?mode=rwc", path.display()))
            .await
            .unwrap();
        sqlx::query(
            "CREATE TABLE messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                msg_type TEXT NOT NULL,
                from_key BLOB,
                channel_idx INTEGER,
                text TEXT NOT NULL,
                timestamp INTEGER NOT NULL
            )",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO messages (msg_type, text, timestamp) VALUES ('channel', 'legacy', 1)",
        )
        .execute(&pool)
        .await
        .unwrap();
        pool.close().await;

        let state = NodeState::open(&path).await.unwrap();
        assert_eq!(state.latest_message_id().await.unwrap(), None);

        state
            .insert_message(&message(b"\x11new", "channel", 1))
            .await
            .unwrap();
        assert!(state.latest_message_id().await.unwrap().is_some());
        assert!(
            state
                .next_channel_frame(0, i64::MAX, 0)
                .await
                .unwrap()
                .is_some()
        );
    }

    /// Contacts are multiplexed rather than cached, so an upgrade must drop the
    /// table an earlier build kept instead of leaving a stale third copy of
    /// state the radio owns. The migration is destructive on purpose.
    #[tokio::test]
    async fn obsolete_contacts_table_is_dropped() {
        let dir = std::env::temp_dir().join(format!(
            "coresplitter-state-contacts-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("state.db");

        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(&format!("sqlite:{}?mode=rwc", path.display()))
            .await
            .unwrap();
        sqlx::query("CREATE TABLE contacts (public_key BLOB PRIMARY KEY, name TEXT)")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO contacts (public_key, name) VALUES (x'01', 'ghost')")
            .execute(&pool)
            .await
            .unwrap();
        pool.close().await;

        let state = NodeState::open(&path).await.unwrap();
        let table: Option<String> = sqlx::query_scalar(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'contacts'",
        )
        .fetch_optional(&state.pool)
        .await
        .unwrap();
        assert_eq!(table, None, "the derived contacts table must be dropped");
    }
}
