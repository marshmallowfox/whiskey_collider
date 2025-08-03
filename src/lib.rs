use actix_web::web;

pub mod common;
pub mod contexts;

pub fn init_routes(cfg: &mut web::ServiceConfig) {
    cfg.configure(contexts::events::features::configure);
}
