use actix_web::web;

pub mod create_event;
pub mod functions_php;
pub mod read_events_stat;
pub mod read_last_user_events;
pub mod read_paginated_events;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.configure(create_event::configure);
    cfg.configure(read_events_stat::configure);
    cfg.configure(read_last_user_events::configure);
    cfg.configure(read_paginated_events::configure);
}
