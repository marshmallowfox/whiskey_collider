use actix_web::{HttpResponse, Responder, get, web};
use chrono::{DateTime, Utc};
use serde::Deserialize;

use crate::contexts::events::{
    features::functions_php::get_type, infrastructure::cached_projection::EventsProj,
};

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(read_events_stat);
}

#[derive(Deserialize)]
struct StatsQuery {
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    e_type: String,
}

#[get("/stats")]
pub async fn read_events_stat(
    query: web::Query<StatsQuery>,
    proj: web::Data<EventsProj>,
) -> impl Responder {
    let type_id: i64 = match get_type(&proj, &query.e_type).await {
        Ok(type_id) => {
            if let Some(type_id) = type_id {
                type_id
            } else {
                return HttpResponse::BadRequest()
                    .content_type("application/json")
                    .body("{\"error\":\"Type not exist\"}");
            }
        }
        Err(e) => {
            return HttpResponse::InternalServerError()
                .content_type("application/json")
                .body(format!("{{\"error\":\"Database error: {}\"}}", e));
        }
    };

    let stats = proj.get_ref().stats(query.from, query.to, type_id).await;

    HttpResponse::Ok()
        .content_type("application/json")
        .body(stats)
}
