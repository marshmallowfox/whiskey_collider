use actix_web::{HttpResponse, Responder, get, web};
use serde::Deserialize;

use crate::contexts::events::{
    features::functions_php::is_user_exist, infrastructure::cached_projection::EventsProj,
};

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(read_last_user_events);
}

#[derive(Deserialize)]
pub struct UserPath {
    user_id: i64,
}

#[get("/users/{user_id}/events")]
pub async fn read_last_user_events(
    path: web::Path<UserPath>,
    proj: web::Data<EventsProj>,
) -> impl Responder {
    match is_user_exist(&proj, path.user_id).await {
        Ok(exists) => {
            if !exists {
                return HttpResponse::BadRequest()
                    .content_type("application/json")
                    .body("{\"error\":\"User not exist\"}");
            }
        }
        Err(e) => {
            return HttpResponse::InternalServerError()
                .content_type("application/json")
                .body(format!("{{\"error\":\"Database error: {}\"}}", e));
        }
    };

    let events = proj.get_thousand_user_events(path.user_id).await;

    HttpResponse::Ok()
        .content_type("application/json")
        .body(events)
}
