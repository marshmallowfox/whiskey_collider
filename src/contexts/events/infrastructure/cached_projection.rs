use std::{collections::HashMap, sync::Arc};

use chrono::{DateTime, Utc};
use serde::{Serialize, Serializer};
use simd_json::{from_slice, to_vec};
use sqlx::types::JsonValue;
use tokio::sync::RwLock;

use crate::{
    common::cache::{CacheSetKey, LeveledCache},
    contexts::events::infrastructure::repo::{EventTypeRow, EventsRepo},
};

#[derive(Clone)]
pub struct EventsProj {
    cache: Arc<RwLock<LeveledCache>>,
    repo: EventsRepo,
}

#[derive(Serialize)]
pub struct Stat {
    total_events: i64,
    unique_users: i64,
    top_pages: HashMap<String, i64>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct EventWithType {
    #[serde(serialize_with = "i64_to_string")]
    pub id: i64,
    #[serde(serialize_with = "i64_to_string")]
    pub user_id: i64,
    pub event_type: String,
    pub timestamp: DateTime<Utc>,
    pub metadata: JsonValue,
}

fn i64_to_string<S>(x: &i64, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    s.serialize_str(&x.to_string())
}

#[derive(Serialize)]
pub struct PaginatedEvents {
    data: Vec<EventWithType>,
    query: Pagination,
}

#[derive(Serialize)]
pub struct Pagination {
    page: usize,
    limit: usize,
    total: i64,
}

impl EventsProj {
    pub fn create(cache: Arc<RwLock<LeveledCache>>, repo: EventsRepo) -> EventsProj {
        EventsProj { cache, repo }
    }

    pub async fn stats(&self, from: DateTime<Utc>, to: DateTime<Utc>, type_id: i64) -> Vec<u8> {
        let from_rfc = from.to_rfc3339();
        let to_rfc = to.to_rfc3339();

        let cache = format!("events_stat_{}_{}_{}", &from_rfc, &to_rfc, type_id);

        if let Some(bytes) = &self.cache.read().await.try_get(cache.clone()).await {
            return bytes.to_owned();
        }

        let stats = &self.repo.stats(from, to, type_id).await.unwrap();

        let mut users: Vec<i64> = vec![];
        let mut total: i64 = 0;
        let mut pages: HashMap<String, i64> = HashMap::new();

        for event in stats {
            if !users.contains(&event.user_id) {
                users.push(event.user_id)
            }
            total += event.page_count;
            *pages.entry(event.page.clone()).or_insert(0) += event.page_count;
        }

        let stat = Stat {
            total_events: total,
            unique_users: users.len() as i64,
            top_pages: pages,
        };

        let payload = to_vec(&stat).unwrap();

        let _ = &self
            .cache
            .write()
            .await
            .save(
                CacheSetKey::Pattern(
                    "events_stat_{}_{}_{}".to_string(),
                    vec![from_rfc, to_rfc, type_id.to_string()],
                ),
                payload.clone(),
                300,
            )
            .await;

        payload
    }

    pub async fn get_thousand_user_events(&self, user_id: i64) -> Vec<u8> {
        let cache = format!("user_events_{}", user_id);

        if let Some(bytes) = &self.cache.read().await.try_get(cache.clone()).await {
            return bytes.to_owned();
        }

        let events = self.repo.get_thousand_user_events(user_id).await.unwrap();

        let payload = to_vec(&events).unwrap();

        let _ = &self
            .cache
            .write()
            .await
            .save(CacheSetKey::Exact(cache), payload.clone(), 300)
            .await;

        payload
    }

    pub async fn paginate_events(&self, page: usize, limit: usize) -> Vec<u8> {
        let cache = format!("page_{}_{}", page, limit);

        if let Some(bytes) = &self.cache.read().await.try_get(cache.clone()).await {
            return bytes.to_owned();
        }

        let events = &self.repo.paginate_events(page, limit).await.unwrap();

        let mut typed_rows: Vec<EventWithType> = Vec::with_capacity(events.len());
        let types_map = self.get_types_id_name().await;

        for ev in events {
            let name = types_map
                .get(&ev.type_id)
                .unwrap_or_else(|| panic!("Event type not found for type_id {}", ev.type_id))
                .clone();

            typed_rows.push(EventWithType {
                id: ev.id,
                user_id: ev.user_id,
                event_type: name,
                timestamp: ev.timestamp,
                metadata: ev.metadata.clone(),
            });
        }

        let total = self.get_events_count().await;

        let result = PaginatedEvents {
            data: typed_rows,
            query: Pagination { page, limit, total },
        };

        let payload = to_vec(&result).unwrap();

        let _ = &self
            .cache
            .write()
            .await
            .save(
                CacheSetKey::Pattern(
                    "page_{}_{}".to_string(),
                    vec![page.to_string(), limit.to_string()],
                ),
                payload.clone(),
                300,
            )
            .await;

        payload
    }

    pub async fn get_events_count(&self) -> i64 {
        let cache = "total_events".to_owned();

        if let Some(bytes) = &self.cache.read().await.try_get(cache.clone()).await {
            let mut bytes = bytes.to_owned();
            let count: i64 = from_slice(bytes.as_mut()).unwrap();
            return count;
        }

        let value = self.repo.count_events().await.unwrap();

        let payload = to_vec(&value).unwrap();

        let _ = &self
            .cache
            .write()
            .await
            .save(CacheSetKey::Exact(cache.clone()), payload.clone(), 300)
            .await;

        value
    }

    pub async fn get_types(&self) -> Vec<u8> {
        let cache = "event_types".to_owned();

        if let Some(bytes) = &self.cache.read().await.try_get(cache.clone()).await {
            return bytes.to_owned();
        }

        let types = &self.repo.get_types().await.unwrap();

        let payload = to_vec(types).unwrap();

        let _ = &self
            .cache
            .write()
            .await
            .save(CacheSetKey::Exact(cache.clone()), payload.clone(), 300)
            .await;

        payload
    }

    pub async fn get_types_name_id(&self) -> HashMap<String, i64> {
        let types: Vec<EventTypeRow> = from_slice(self.get_types().await.as_mut()).unwrap();

        types
            .into_iter()
            .map(|type_row| (type_row.name, type_row.id))
            .collect::<HashMap<String, i64>>()
    }

    pub async fn get_types_id_name(&self) -> HashMap<i64, String> {
        let types: Vec<EventTypeRow> = from_slice(self.get_types().await.as_mut()).unwrap();

        types
            .into_iter()
            .map(|type_row| (type_row.id, type_row.name))
            .collect::<HashMap<i64, String>>()
    }

    pub async fn get_users_id(&self) -> Vec<u8> {
        let cache = "users_id".to_owned();

        if let Some(bytes) = &self.cache.read().await.try_get(cache.clone()).await {
            return bytes.to_owned();
        }

        let ids = &self.repo.get_users_id().await.unwrap();

        let payload = to_vec(ids).unwrap();

        let _ = &self
            .cache
            .write()
            .await
            .save(CacheSetKey::Exact(cache.clone()), payload.clone(), 300)
            .await;

        payload
    }
}
