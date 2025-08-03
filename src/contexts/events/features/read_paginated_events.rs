use actix_web::{HttpResponse, Responder, get, web};
use serde::Deserialize;

use crate::contexts::events::infrastructure::cached_projection::EventsProj;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(read_paginated_events);
}

#[derive(Deserialize)]
struct Pagination {
    page: Option<usize>,
    limit: Option<usize>,
}

#[get("/events")]
pub async fn read_paginated_events(
    pagination: web::Query<Pagination>,
    proj: web::Data<EventsProj>,
) -> impl Responder {
    let page = pagination.page.unwrap_or(1);
    let limit = pagination.limit.unwrap_or(100);

    let data = proj.paginate_events(page, limit).await;

    HttpResponse::Ok()
        .content_type("application/json")
        .body(data)
}
