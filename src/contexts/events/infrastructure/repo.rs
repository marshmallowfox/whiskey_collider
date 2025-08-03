use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize, Serializer};
use sqlx::prelude::FromRow;
use sqlx::types::JsonValue;
use sqlx::{Pool, Postgres, query_as, query_scalar};

#[derive(Clone)]
pub struct EventsRepo {
    postgres: Pool<Postgres>,
}

#[derive(Serialize, Deserialize)]
pub struct EventTypeRow {
    pub id: i64,
    pub name: String,
}

#[derive(Serialize)]
pub struct EventStat {
    pub page_count: i64,
    pub user_id: i64,
    pub page: String,
}

#[derive(Debug, FromRow, Serialize)]
pub struct Event {
    #[serde(serialize_with = "i64_to_string")]
    pub id: i64,
    #[serde(serialize_with = "i64_to_string")]
    pub user_id: i64,
    #[serde(serialize_with = "i64_to_string")]
    pub type_id: i64,
    pub timestamp: DateTime<Utc>,
    pub metadata: JsonValue,
}
fn i64_to_string<S>(x: &i64, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    s.serialize_str(&x.to_string())
}

impl EventsRepo {
    pub fn create(postgres: Pool<Postgres>) -> EventsRepo {
        EventsRepo { postgres }
    }

    pub async fn get_types(&self) -> Result<Vec<EventTypeRow>, anyhow::Error> {
        let rows = query_as!(EventTypeRow, r#"SELECT id, name FROM event_types"#)
            .fetch_all(&self.postgres.to_owned())
            .await?;

        Ok(rows)
    }

    pub async fn get_users_id(&self) -> Result<Vec<i64>, anyhow::Error> {
        let ids: Vec<i64> = query_scalar!(r#"SELECT id FROM users"#)
            .fetch_all(&self.postgres)
            .await?;

        Ok(ids)
    }

    pub async fn count_events(&self) -> Result<i64, anyhow::Error> {
        let count: Option<i64> = query_scalar!(r#"SELECT COUNT(id) FROM events"#)
            .fetch_one(&self.postgres)
            .await?;

        Ok(count.unwrap_or(0))
    }

    pub async fn paginate_events(
        &self,
        page: usize,
        limit: usize,
    ) -> Result<Vec<Event>, anyhow::Error> {
        let offset = (page - 1) * limit;

        let events = query_as!(
            Event,
            r#"SELECT
                id,
                user_id,
                type_id,
                timestamp,
                metadata
            FROM events 
            ORDER BY timestamp DESC
            OFFSET $1 LIMIT $2"#,
            offset as i64,
            limit as i64
        )
        .fetch_all(&self.postgres)
        .await?;

        Ok(events)
    }

    pub async fn get_thousand_user_events(
        &self,
        user_id: i64,
    ) -> Result<Vec<Event>, anyhow::Error> {
        let events = sqlx::query_as!(
            Event,
            r#"SELECT
                id,
                user_id,
                type_id,
                timestamp,
                metadata
               FROM events
               WHERE user_id = $1
               ORDER BY timestamp DESC
               LIMIT 1000"#,
            user_id
        )
        .fetch_all(&self.postgres)
        .await?;

        Ok(events)
    }

    pub async fn stats(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        type_id: i64,
    ) -> Result<Vec<EventStat>, anyhow::Error> {
        let stats = query_as!(
            EventStat,
            r#"SELECT
                COUNT(*) as "page_count!",
                user_id as "user_id!",
                metadata->>'page' as "page!"
            FROM events
            WHERE timestamp >= $1
            AND timestamp <= $2
            AND type_id = $3
            GROUP BY user_id, metadata->>'page'"#,
            from,
            to,
            type_id
        )
        .fetch_all(&self.postgres)
        .await?;

        Ok(stats)
    }
}
