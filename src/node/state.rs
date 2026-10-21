use std::path::Path;

use anyhow::{Context, Result};
use sqlx::sqlite::{SqliteAutoVacuum, SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{FromRow, Row, SqlitePool};

#[derive(Debug, Clone, FromRow)]
pub struct CachedContact {
    pub public_key: Vec<u8>,
    pub name: String,
    pub contact_type: i64,
    pub last_advert: Option<i64>,
    pub lat: Option<f64>,
    pub lon: Option<f64>,
    /// The CONTACT (0x03) frame exactly as the radio sent it, served back
    /// verbatim so flags, path and lastmod survive. `None` for rows cached
    /// by older versions, which only kept the decoded fields.
    pub raw: Option<Vec<u8>>,
}

#[derive(Debug, Clone, FromRow)]
pub struct CachedChannel {
    pub idx: i64,
    pub name: String,
    pub config: Option<Vec<u8>>,
}

/// SQL condition for messages that can be replayed to the client bound
/// as ?2 (port) and ?3 (app name): it has a stored frame and wasn't sent
/// by that same client.
const REPLAYABLE_TO: &str = "frame IS NOT NULL \
     AND (origin_port IS NULL OR origin_port != ?2 OR origin_app != ?3)";

pub struct NodeState {
    pool: SqlitePool,
}

impl NodeState {
    pub async fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();

        // Incremental auto-vacuum lets maintain() hand freed pages back to
        // the filesystem after purging, without a full VACUUM each time.
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .auto_vacuum(SqliteAutoVacuum::Incremental);

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
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS contacts (
                public_key BLOB PRIMARY KEY,
                name TEXT NOT NULL,
                contact_type INTEGER NOT NULL,
                last_advert INTEGER,
                lat REAL,
                lon REAL
            )",
        )
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
                timestamp INTEGER NOT NULL
            )",
        )
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

        // Clients are identified by (local port, app name from APP_START).
        // last_seen_id is the highest messages.id the client has seen, either
        // live or replayed; whatever is above it is delivered on reconnect.
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS clients (
                port INTEGER NOT NULL,
                app_name TEXT NOT NULL,
                last_seen_id INTEGER NOT NULL,
                last_connected_at INTEGER NOT NULL,
                PRIMARY KEY (port, app_name)
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

        self.add_column_if_missing("contacts", "raw", "BLOB")
            .await?;
        // When the radio last reported the contact (unix secs, local clock).
        self.add_column_if_missing("contacts", "seen_at", "INTEGER")
            .await?;
        // Local arrival time; `timestamp` is the sender's clock and can't be
        // trusted for retention.
        self.add_column_if_missing("messages", "received_at", "INTEGER")
            .await?;
        // The message frame as clients saw it, replayed to clients that
        // were offline. Rows from older versions have none and aren't.
        self.add_column_if_missing("messages", "frame", "BLOB")
            .await?;
        // Client identity that sent the message through the proxy (NULL for
        // traffic from the radio), so it is never replayed back to it.
        self.add_column_if_missing("messages", "origin_port", "INTEGER")
            .await?;
        self.add_column_if_missing("messages", "origin_app", "TEXT")
            .await?;

        // auto_vacuum only takes effect on a database without tables, or
        // after a VACUUM. Convert databases created before it was set.
        let auto_vacuum: i64 = sqlx::query_scalar("PRAGMA auto_vacuum")
            .fetch_one(&self.pool)
            .await?;
        if auto_vacuum != 2 {
            tracing::info!("converting database to incremental auto-vacuum");
            sqlx::query("VACUUM").execute(&self.pool).await?;
        }

        Ok(())
    }

    /// SQLite has no ADD COLUMN IF NOT EXISTS; check table_info first.
    async fn add_column_if_missing(&self, table: &str, column: &str, decl: &str) -> Result<()> {
        let exists: bool =
            sqlx::query_scalar("SELECT COUNT(*) > 0 FROM pragma_table_info(?1) WHERE name = ?2")
                .bind(table)
                .bind(column)
                .fetch_one(&self.pool)
                .await?;
        if !exists {
            sqlx::query(&format!("ALTER TABLE {table} ADD COLUMN {column} {decl}"))
                .execute(&self.pool)
                .await?;
        }
        Ok(())
    }

    // --- Contacts ---

    pub async fn upsert_contact(&self, contact: &CachedContact) -> Result<()> {
        sqlx::query(
            "INSERT OR REPLACE INTO contacts
                 (public_key, name, contact_type, last_advert, lat, lon, raw, seen_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        )
        .bind(&contact.public_key)
        .bind(&contact.name)
        .bind(contact.contact_type)
        .bind(contact.last_advert)
        .bind(contact.lat)
        .bind(contact.lon)
        .bind(&contact.raw)
        .bind(chrono::Utc::now().timestamp())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get_contacts(&self) -> Result<Vec<CachedContact>> {
        let rows = sqlx::query_as::<_, CachedContact>(
            "SELECT public_key, name, contact_type, last_advert, lat, lon, raw
             FROM contacts ORDER BY name",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
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

    pub async fn delete_contact(&self, public_key: &[u8]) -> Result<()> {
        sqlx::query("DELETE FROM contacts WHERE public_key = ?1")
            .bind(public_key)
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
                 (msg_type, from_key, channel_idx, text, timestamp, received_at, frame,
                  origin_port, origin_app)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        )
        .bind(msg.msg_type)
        .bind(msg.from_key)
        .bind(msg.channel_idx)
        .bind(msg.text)
        .bind(msg.timestamp)
        .bind(chrono::Utc::now().timestamp())
        .bind(msg.frame)
        .bind(msg.origin.map(|(port, _)| i64::from(port)))
        .bind(msg.origin.map(|(_, app)| app))
        .execute(&self.pool)
        .await?;
        Ok(result.last_insert_rowid())
    }

    /// Highest message id stored so far (0 when there are none).
    pub async fn message_head(&self) -> Result<i64> {
        Ok(
            sqlx::query_scalar("SELECT COALESCE(MAX(id), 0) FROM messages")
                .fetch_one(&self.pool)
                .await?,
        )
    }

    /// Ids of messages above `after` to replay to a client, oldest first.
    /// Leaves out those the client itself sent.
    pub async fn message_ids_after(
        &self,
        after: i64,
        port: u16,
        app_name: &str,
    ) -> Result<Vec<i64>> {
        Ok(sqlx::query_scalar(&format!(
            "SELECT id FROM messages WHERE id > ?1 AND {REPLAYABLE_TO} ORDER BY id"
        ))
        .bind(after)
        .bind(i64::from(port))
        .bind(app_name)
        .fetch_all(&self.pool)
        .await?)
    }

    /// Ids of the replayable messages a client seen for the first time
    /// gets, oldest first.
    pub async fn backlog_message_ids(
        &self,
        backlog: Backlog,
        port: u16,
        app_name: &str,
    ) -> Result<Vec<i64>> {
        let ids = match backlog {
            Backlog::Off => Vec::new(),
            Backlog::Messages(n) => {
                sqlx::query_scalar(&format!(
                    "SELECT id FROM (
                         SELECT id FROM messages WHERE ?1 = ?1 AND {REPLAYABLE_TO}
                         ORDER BY id DESC LIMIT ?4
                     ) ORDER BY id"
                ))
                .bind(0)
                .bind(i64::from(port))
                .bind(app_name)
                .bind(i64::from(n))
                .fetch_all(&self.pool)
                .await?
            }
            Backlog::Seconds(secs) => {
                sqlx::query_scalar(&format!(
                    "SELECT id FROM messages
                     WHERE received_at >= ?1 AND {REPLAYABLE_TO} ORDER BY id"
                ))
                .bind(chrono::Utc::now().timestamp() - secs)
                .bind(i64::from(port))
                .bind(app_name)
                .fetch_all(&self.pool)
                .await?
            }
        };
        Ok(ids)
    }

    /// The stored frame of a message, if it still exists.
    pub async fn message_frame(&self, id: i64) -> Result<Option<Vec<u8>>> {
        Ok(
            sqlx::query_scalar("SELECT frame FROM messages WHERE id = ?1 AND frame IS NOT NULL")
                .bind(id)
                .fetch_optional(&self.pool)
                .await?,
        )
    }

    // --- Clients ---

    /// Last message id seen by a client, `None` if it never connected.
    pub async fn client_cursor(&self, port: u16, app_name: &str) -> Result<Option<i64>> {
        Ok(
            sqlx::query_scalar(
                "SELECT last_seen_id FROM clients WHERE port = ?1 AND app_name = ?2",
            )
            .bind(i64::from(port))
            .bind(app_name)
            .fetch_optional(&self.pool)
            .await?,
        )
    }

    /// Move a client's cursor forward to `seen_id`; never backwards, since
    /// two sessions of the same identity may report out of order.
    pub async fn advance_client_cursor(
        &self,
        port: u16,
        app_name: &str,
        seen_id: i64,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO clients (port, app_name, last_seen_id, last_connected_at)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT (port, app_name) DO UPDATE SET
                 last_seen_id = MAX(last_seen_id, excluded.last_seen_id),
                 last_connected_at = excluded.last_connected_at",
        )
        .bind(i64::from(port))
        .bind(app_name)
        .bind(seen_id)
        .bind(chrono::Utc::now().timestamp())
        .execute(&self.pool)
        .await?;
        Ok(())
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

    // --- Maintenance ---

    /// Purge rows older than the given retention (in days; 0 keeps them
    /// forever) and give the freed space back. Returns how many rows each
    /// table lost.
    pub async fn maintain(&self, retention: &Retention) -> Result<PurgeStats> {
        let now = chrono::Utc::now().timestamp();
        let cutoff = |days: u32| now - i64::from(days) * 86_400;
        let mut stats = PurgeStats::default();

        if retention.messages_days > 0 {
            stats.messages =
                sqlx::query("DELETE FROM messages WHERE COALESCE(received_at, timestamp) < ?1")
                    .bind(cutoff(retention.messages_days))
                    .execute(&self.pool)
                    .await?
                    .rows_affected();
        }

        if retention.raw_rx_days > 0 {
            stats.raw_rx = sqlx::query("DELETE FROM raw_rx WHERE ts < ?1")
                .bind(cutoff(retention.raw_rx_days))
                .execute(&self.pool)
                .await?
                .rows_affected();
        }

        // Contacts are a safeguard against the radio losing them (reflash,
        // factory reset), so only drop the ones that were missing from the
        // last full sync *and* haven't been seen for the retention period.
        // Without a recorded sync nothing qualifies as missing.
        if retention.contacts_days > 0
            && let Some(synced_at) = self.contacts_synced_at().await?
        {
            let threshold = synced_at.min(cutoff(retention.contacts_days));
            stats.contacts = sqlx::query("DELETE FROM contacts WHERE COALESCE(seen_at, 0) < ?1")
                .bind(threshold)
                .execute(&self.pool)
                .await?
                .rows_affected();
        }

        // A purged client that comes back is treated as new
        // (CORESPLITTER_NEW_CLIENT_BACKLOG) instead of getting what it missed.
        if retention.clients_days > 0 {
            stats.clients = sqlx::query("DELETE FROM clients WHERE last_connected_at < ?1")
                .bind(cutoff(retention.clients_days))
                .execute(&self.pool)
                .await?
                .rows_affected();
        }

        sqlx::query("PRAGMA incremental_vacuum")
            .execute(&self.pool)
            .await?;
        sqlx::query("PRAGMA optimize").execute(&self.pool).await?;

        Ok(stats)
    }

    /// Record that a full GET_CONTACTS stream started at `started_at`:
    /// every contact the radio holds now has seen_at >= that instant.
    pub async fn set_contacts_synced_at(&self, started_at: i64) -> Result<()> {
        self.kv_set("contacts_synced_at", &started_at.to_le_bytes())
            .await
    }

    async fn contacts_synced_at(&self) -> Result<Option<i64>> {
        Ok(self
            .kv_get("contacts_synced_at")
            .await?
            .and_then(|b| <[u8; 8]>::try_from(b.as_slice()).ok())
            .map(i64::from_le_bytes))
    }
}

/// A message to store: decoded fields plus the frame clients saw.
pub struct NewMessage<'a> {
    pub msg_type: &'a str,
    pub from_key: Option<&'a [u8]>,
    pub channel_idx: Option<i64>,
    pub text: &'a str,
    /// Sender's clock, as carried in the frame.
    pub timestamp: i64,
    pub frame: &'a [u8],
    /// (port, app name) of the client that sent it through the proxy.
    pub origin: Option<(u16, &'a str)>,
}

/// What a client connecting for the first time gets replayed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backlog {
    Off,
    /// The last N messages.
    Messages(u32),
    /// Messages received in the last N seconds.
    Seconds(i64),
}

impl std::str::FromStr for Backlog {
    type Err = String;

    /// `0` disables it, `N` means N messages, `Nh` / `Nd` a time window.
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let s = s.trim();
        let (num, unit) = match s.char_indices().last() {
            Some((i, c)) if c.is_ascii_alphabetic() => (&s[..i], Some(c.to_ascii_lowercase())),
            _ => (s, None),
        };
        let n: u32 = num
            .parse()
            .map_err(|_| format!("invalid backlog {s:?}: expected N, Nh or Nd"))?;
        Ok(match (n, unit) {
            (0, _) => Backlog::Off,
            (n, None) => Backlog::Messages(n),
            (n, Some('h')) => Backlog::Seconds(i64::from(n) * 3600),
            (n, Some('d')) => Backlog::Seconds(i64::from(n) * 86_400),
            (_, Some(u)) => return Err(format!("invalid backlog unit {u:?}: use h or d")),
        })
    }
}

/// Retention periods in days; 0 disables purging for that table.
#[derive(Debug, Clone, Copy)]
pub struct Retention {
    pub messages_days: u32,
    pub raw_rx_days: u32,
    pub contacts_days: u32,
    pub clients_days: u32,
}

#[derive(Debug, Default)]
pub struct PurgeStats {
    pub messages: u64,
    pub raw_rx: u64,
    pub contacts: u64,
    pub clients: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    const DAY: i64 = 86_400;

    async fn open_temp(name: &str) -> NodeState {
        let dir =
            std::env::temp_dir().join(format!("coresplitter-state-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        NodeState::open(dir.join("state.db")).await.unwrap()
    }

    fn contact(key: u8) -> CachedContact {
        CachedContact {
            public_key: vec![key; 32],
            name: format!("c{key}"),
            contact_type: 1,
            last_advert: None,
            lat: None,
            lon: None,
            raw: None,
        }
    }

    async fn set_seen_at(state: &NodeState, key: u8, seen_at: i64) {
        sqlx::query("UPDATE contacts SET seen_at = ?1 WHERE public_key = ?2")
            .bind(seen_at)
            .bind(vec![key; 32])
            .execute(&state.pool)
            .await
            .unwrap();
    }

    async fn contact_keys(state: &NodeState) -> Vec<u8> {
        let mut keys: Vec<u8> = state
            .get_contacts()
            .await
            .unwrap()
            .iter()
            .map(|c| c.public_key[0])
            .collect();
        keys.sort();
        keys
    }

    #[tokio::test]
    async fn messages_and_raw_rx_follow_local_time() {
        let state = open_temp("msgs").await;
        let now = chrono::Utc::now().timestamp();
        // Sender clock in 1970 but received just now: must survive.
        state
            .insert_message(&NewMessage {
                msg_type: "channel",
                from_key: None,
                channel_idx: Some(0),
                text: "fresh",
                timestamp: 0,
                frame: &[0x08],
                origin: None,
            })
            .await
            .unwrap();
        let old = state
            .insert_message(&NewMessage {
                msg_type: "channel",
                from_key: None,
                channel_idx: Some(0),
                text: "old",
                timestamp: now,
                frame: &[0x08],
                origin: None,
            })
            .await
            .unwrap();
        sqlx::query("UPDATE messages SET received_at = ?1 WHERE id = ?2")
            .bind(now - 40 * DAY)
            .bind(old)
            .execute(&state.pool)
            .await
            .unwrap();
        state.insert_raw_rx(0x83, &[0x83]).await.unwrap();
        sqlx::query("UPDATE raw_rx SET ts = ?1")
            .bind(now - 8 * DAY)
            .execute(&state.pool)
            .await
            .unwrap();

        let retention = Retention {
            messages_days: 30,
            raw_rx_days: 7,
            contacts_days: 0,
            clients_days: 0,
        };
        let stats = state.maintain(&retention).await.unwrap();
        assert_eq!((stats.messages, stats.raw_rx), (1, 1));
        let left: Vec<String> = sqlx::query_scalar("SELECT text FROM messages")
            .fetch_all(&state.pool)
            .await
            .unwrap();
        assert_eq!(left, ["fresh"]);
    }

    #[tokio::test]
    async fn contacts_kept_without_a_recorded_sync() {
        let state = open_temp("nosync").await;
        let now = chrono::Utc::now().timestamp();
        state.upsert_contact(&contact(1)).await.unwrap();
        set_seen_at(&state, 1, now - 400 * DAY).await;

        let retention = Retention {
            messages_days: 0,
            raw_rx_days: 0,
            contacts_days: 30,
            clients_days: 0,
        };
        assert_eq!(state.maintain(&retention).await.unwrap().contacts, 0);
        assert_eq!(contact_keys(&state).await, [1]);
    }

    #[tokio::test]
    async fn only_long_missing_contacts_are_purged() {
        let state = open_temp("contacts").await;
        let now = chrono::Utc::now().timestamp();
        for key in 1..=3 {
            state.upsert_contact(&contact(key)).await.unwrap();
        }
        // 1: missing from the last sync for 60 days → purged.
        // 2: missing from the last sync, but only for 5 days → kept.
        // 3: reported by the last sync → kept however old the sync is.
        set_seen_at(&state, 1, now - 60 * DAY).await;
        set_seen_at(&state, 2, now - 5 * DAY).await;
        set_seen_at(&state, 3, now - 2 * DAY).await;
        state.set_contacts_synced_at(now - 2 * DAY).await.unwrap();

        let retention = Retention {
            messages_days: 0,
            raw_rx_days: 0,
            contacts_days: 30,
            clients_days: 0,
        };
        assert_eq!(state.maintain(&retention).await.unwrap().contacts, 1);
        assert_eq!(contact_keys(&state).await, [2, 3]);

        // The proxy ran 45 days without a new full sync: contacts that
        // sync reported are not "missing", so nothing else goes.
        set_seen_at(&state, 2, now - 45 * DAY).await;
        set_seen_at(&state, 3, now - 45 * DAY).await;
        state.set_contacts_synced_at(now - 45 * DAY).await.unwrap();
        assert_eq!(state.maintain(&retention).await.unwrap().contacts, 0);
        assert_eq!(contact_keys(&state).await, [2, 3]);
    }

    #[test]
    fn backlog_parses_count_and_windows() {
        assert_eq!("0".parse::<Backlog>(), Ok(Backlog::Off));
        assert_eq!("0d".parse::<Backlog>(), Ok(Backlog::Off));
        assert_eq!("50".parse::<Backlog>(), Ok(Backlog::Messages(50)));
        assert_eq!("24h".parse::<Backlog>(), Ok(Backlog::Seconds(24 * 3600)));
        assert_eq!("7D".parse::<Backlog>(), Ok(Backlog::Seconds(7 * DAY)));
        assert!("5m".parse::<Backlog>().is_err());
        assert!("h".parse::<Backlog>().is_err());
        assert!("-1".parse::<Backlog>().is_err());
    }

    async fn store(state: &NodeState, text: &str, origin: Option<(u16, &str)>) -> i64 {
        state
            .insert_message(&NewMessage {
                msg_type: "channel",
                from_key: None,
                channel_idx: Some(0),
                text,
                timestamp: 0,
                frame: text.as_bytes(),
                origin,
            })
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn replay_skips_own_messages() {
        let state = open_temp("replay").await;
        let a = store(&state, "from radio", None).await;
        let _ = store(&state, "sent by cli", Some((5000, "cli"))).await;
        let c = store(&state, "sent by app", Some((5000, "app"))).await;
        let d = store(&state, "cli on other port", Some((5001, "cli"))).await;

        let ids = state.message_ids_after(0, 5000, "cli").await.unwrap();
        assert_eq!(ids, [a, c, d]);
        assert_eq!(
            state.message_frame(c).await.unwrap().unwrap(),
            b"sent by app"
        );
    }

    #[tokio::test]
    async fn backlog_by_count_and_time() {
        let state = open_temp("backlog").await;
        let mut ids = Vec::new();
        for i in 0..4 {
            ids.push(store(&state, &format!("m{i}"), None).await);
        }
        sqlx::query("UPDATE messages SET received_at = ?1 WHERE id <= ?2")
            .bind(chrono::Utc::now().timestamp() - 3 * DAY)
            .bind(ids[1])
            .execute(&state.pool)
            .await
            .unwrap();

        let last_two = state
            .backlog_message_ids(Backlog::Messages(2), 5000, "x")
            .await
            .unwrap();
        assert_eq!(last_two, ids[2..]);
        let last_day = state
            .backlog_message_ids(Backlog::Seconds(DAY), 5000, "x")
            .await
            .unwrap();
        assert_eq!(last_day, ids[2..]);
        let off = state
            .backlog_message_ids(Backlog::Off, 5000, "x")
            .await
            .unwrap();
        assert!(off.is_empty());
    }

    #[tokio::test]
    async fn client_cursor_never_goes_back() {
        let state = open_temp("cursor").await;
        assert_eq!(state.client_cursor(5000, "app").await.unwrap(), None);
        state.advance_client_cursor(5000, "app", 10).await.unwrap();
        state.advance_client_cursor(5000, "app", 7).await.unwrap();
        assert_eq!(state.client_cursor(5000, "app").await.unwrap(), Some(10));
        assert_eq!(state.client_cursor(5001, "app").await.unwrap(), None);
    }

    #[tokio::test]
    async fn idle_clients_are_forgotten() {
        let state = open_temp("clients").await;
        state.advance_client_cursor(5000, "old", 1).await.unwrap();
        state
            .advance_client_cursor(5000, "recent", 1)
            .await
            .unwrap();
        sqlx::query("UPDATE clients SET last_connected_at = ?1 WHERE app_name = 'old'")
            .bind(chrono::Utc::now().timestamp() - 200 * DAY)
            .execute(&state.pool)
            .await
            .unwrap();

        let retention = Retention {
            messages_days: 0,
            raw_rx_days: 0,
            contacts_days: 0,
            clients_days: 180,
        };
        assert_eq!(state.maintain(&retention).await.unwrap().clients, 1);
        assert_eq!(state.client_cursor(5000, "old").await.unwrap(), None);
        assert_eq!(state.client_cursor(5000, "recent").await.unwrap(), Some(1));
    }
}
