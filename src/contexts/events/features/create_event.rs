use std::{any::Any, sync::Arc};

use actix_web::{HttpResponse, Responder, post, web};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::json;
use simd_json::{
    BorrowedValue,
    base::{ValueAsObject, ValueAsScalar},
    derived::ValueObjectAccess,
    to_borrowed_value, to_vec,
};
use sqlx::Row;
use sqlx::types::JsonValue;
use tokio::{sync::RwLock, try_join};

use crate::{
    common::{
        cache::LeveledCache,
        command_bus::{CommandBus, CommandValue},
        snowflake::next_id,
    },
    contexts::events::{
        features::functions_php::{get_type, is_user_exist},
        infrastructure::cached_projection::EventsProj,
    },
};

#[derive(Clone)]
struct CreateEventRequest {
    user_id: i64,
    event_type: String,
    timestamp: DateTime<Utc>,
    metadata: JsonValue,
}

#[derive(Serialize)]
pub struct HttpError {
    pub error: String,
}

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(create_event);
}

#[post("/event")]
pub async fn create_event(
    body: Bytes,
    proj: web::Data<EventsProj>,
    bus: web::Data<Arc<CommandBus>>,
    cache: web::Data<Arc<RwLock<LeveledCache>>>,
) -> impl Responder {
    let mut buf = body.to_vec();

    let raw_json: BorrowedValue = match to_borrowed_value(&mut buf) {
        Ok(value) => value,
        Err(_) => {
            return HttpResponse::BadRequest()
                .content_type("application/json")
                .body("Some");
        }
    };

    let request = match validate_request(&raw_json) {
        Ok(dto) => dto,
        Err(resp) => return resp,
    };

    let type_id = match try_join!(
        is_user_exist(&proj, request.user_id),
        get_type(&proj, request.event_type.as_str())
    ) {
        Ok((user_exist, type_id)) => {
            if !user_exist {
                return HttpResponse::BadRequest()
                    .content_type("application/json")
                    .body("{\"error\":\"User not exist\"}");
            }

            if type_id.is_none() {
                return HttpResponse::BadRequest()
                    .content_type("application/json")
                    .body("{\"error\":\"Type not exist\"}");
            }

            type_id.unwrap()
        }
        Err(e) => {
            return HttpResponse::InternalServerError()
                .content_type("application/json")
                .body(format!("{{\"error\":\"Database error: {}\"}}", e));
        }
    };

    let id = next_id();

    match insert_to_command_bus(
        id,
        type_id,
        request.clone(),
        bus.get_ref(),
        cache.get_ref(),
        proj.get_ref(),
    )
    .await
    {
        Ok(_) => {}
        Err(e) => {
            return HttpResponse::InternalServerError()
                .body(format!("Failed insert to bus: {}", e));
        }
    };

    HttpResponse::Ok()
        .content_type("application/json")
        .body(format!(
            "{{\"id\":\"{}\",\"user_id\":\"{}\",\"type_id\":\"{}\",\"timestamp\":\"{}\",\"metadata\":{}}}",
            id, &request.user_id, type_id, &request.timestamp, &request.metadata
        ))
}

fn validate_request<'a>(
    raw_json: &'a BorrowedValue<'a>,
) -> Result<CreateEventRequest, HttpResponse> {
    let user_id = raw_json
        .get("user_id")
        .and_then(|v| v.as_i64())
        .ok_or_else(|| {
            HttpResponse::BadRequest().json(HttpError {
                error: "`user_id` missing or not a i64".to_owned(),
            })
        })?;

    let event_type = raw_json
        .get("event_type")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            HttpResponse::BadRequest().json(HttpError {
                error: "`event_type` missing or empty".to_owned(),
            })
        })?;

    let timestamp = raw_json
        .get("timestamp")
        .and_then(|v| v.as_str())
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&Utc))
        .ok_or_else(|| {
            HttpResponse::BadRequest().json(HttpError {
                error: "`timestamp` missing or invalid".to_owned(),
            })
        })?;

    let page = raw_json
        .get("metadata")
        .and_then(BorrowedValue::as_object)
        .and_then(|m| m.get("page"))
        .and_then(BorrowedValue::as_str)
        .ok_or_else(|| {
            HttpResponse::BadRequest().json(HttpError {
                error: "`metadata.page` is required and must be a string".to_owned(),
            })
        })?;

    let metadata = json!({"page": page});

    Ok(CreateEventRequest {
        user_id,
        event_type: event_type.to_owned(),
        timestamp,
        metadata,
    })
}

async fn insert_to_command_bus(
    id: i64,
    type_id: i64,
    request: CreateEventRequest,
    bus: &CommandBus,
    cache: &Arc<RwLock<LeveledCache>>,
    proj: &EventsProj,
) -> Result<(), anyhow::Error> {
    let proj_clone = proj.clone();
    let cache_clone = cache.clone();

    bus.push(
        r#"
        WITH inserted AS (
            INSERT INTO events (id, user_id, type_id, timestamp, metadata)
            SELECT *
            FROM UNNEST(
                $1::bigint[],
                $2::bigint[],
                $3::bigint[],
                $4::timestamptz[],
                $5::jsonb[]
            ) AS t(
                id,
                user_id,
                type_id,
                timestamp,
                metadata
            )
            RETURNING user_id
        )
        SELECT COUNT(*) AS total_inserted, array_agg(DISTINCT user_id) AS unique_users
        FROM inserted;
        "#,
        vec![
            CommandValue::Int(id),
            CommandValue::Int(request.user_id),
            CommandValue::Int(type_id),
            CommandValue::Timestamp(request.timestamp),
            CommandValue::Json(request.metadata),
        ],
        Some(Box::new(move |row: &dyn Any| {
            if let Some(row) = row.downcast_ref::<sqlx::postgres::PgRow>() {
                let inserted: i64 = row.try_get("total_inserted").unwrap();
                let users: Vec<i64> = row.try_get("unique_users").unwrap();

                let proj_clone = proj_clone.clone();
                let cache_clone = cache_clone.clone();

                tokio::spawn(async move {
                    let count = proj_clone.get_events_count().await;

                    let mut write_cache = cache_clone.write().await;

                    let calculated = count + inserted;

                    let _ = write_cache
                        .save(
                            crate::common::cache::CacheSetKey::Exact("total_events".to_string()),
                            to_vec(&calculated).unwrap(),
                            100,
                        )
                        .await;

                    for user_id in users {
                        let _ = write_cache
                            .invalidate(crate::common::cache::CacheDeleteKey::Exact(format!(
                                "user_{}",
                                user_id
                            )))
                            .await;

                        let _ = write_cache
                            .invalidate(crate::common::cache::CacheDeleteKey::Pattern(
                                "events_stat_{}_{}_{}".to_string(),
                            ))
                            .await;
                    }
                });
            } else {
                println!("Unexpected row type");
            }
        })),
    )
    .await;
    Ok(())
}
