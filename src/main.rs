use chrono::Utc;
use std::{env, sync::Arc, time::Duration};
use tokio::{sync::RwLock, try_join};

use actix_web::{App, HttpServer, web};

use sqlx::{
    Executor, Pool, Postgres,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use w_collider::{
    common::{
        cache::LeveledCache,
        command_bus::CommandBus,
        env::Env,
        output::{send_group, send_message},
        seeder::seed,
    },
    contexts::events::infrastructure::{cached_projection::EventsProj, repo::EventsRepo},
    init_routes,
};

use moka::future::Cache;

use redis::{Client, aio::MultiplexedConnection};

mod common;
mod contexts;

#[actix_web::main]
async fn main() -> anyhow::Result<(), anyhow::Error> {
    let app_env = load_env();

    send_group("Loading databases".to_owned());

    let (pg_pool, redis_connection) =
        try_join!(load_postgres_pool(&app_env), load_redis_multiplex(&app_env)).unwrap();

    send_message("Successful".to_owned());

    let args: Vec<String> = env::args().collect();
    if args.len() > 1 && args[1] == "seeder" {
        run_seeder(pg_pool, app_env).await;
    } else {
        run_server(pg_pool, redis_connection, app_env).await;
    }

    Ok(())
}

async fn run_seeder(pg_pool: Pool<Postgres>, env: Env) -> anyhow::Result<(), anyhow::Error> {
    send_group("Seed database".to_owned());

    let start = Utc::now();

    seed(pg_pool, env).await;

    let duration = Utc::now().signed_duration_since(start).num_seconds();
    send_message(format!("─ Total Duration {} seconds", duration));

    send_message("Successful".to_owned());
    Ok(())
}

async fn run_server(
    pg_pool: Pool<Postgres>,
    redis_connection: MultiplexedConnection,
    env: Env,
) -> anyhow::Result<(), anyhow::Error> {
    send_group("Creating leveled cache".to_owned());

    let lru: Cache<String, Vec<u8>> = Cache::builder()
        .weigher(|_, v: &Vec<u8>| v.len().try_into().unwrap_or(u32::MAX))
        .max_capacity(env.app_cache * 1024 * 1024)
        .build();

    let cache = Arc::new(RwLock::new(LeveledCache::create(
        redis_connection.clone(),
        lru,
    )));

    send_message("Successful".to_owned());

    send_group("Creating command bus thread".to_owned());

    let bus = Arc::new(CommandBus::init(Duration::from_secs(1), pg_pool.clone()));

    send_message("Successful".to_owned());

    send_group("Creating repository and projection".to_owned());

    let repo = EventsRepo::create(pg_pool.clone());
    let proj = EventsProj::create(cache.clone(), repo.clone());

    send_message("Successful".to_owned());

    send_group("Server will be started".to_owned());
    send_message(format!("IP 0.0.0.0:{}", env.port));

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(repo.clone()))
            .app_data(web::Data::new(proj.clone()))
            .app_data(web::Data::new(pg_pool.clone()))
            .app_data(web::Data::new(bus.clone()))
            .app_data(web::Data::new(redis_connection.clone()))
            .app_data(web::Data::new(cache.clone()))
            .configure(init_routes)
    })
    .bind(("0.0.0.0", env.port))?
    .run()
    .await?;

    Ok(())
}

fn load_env() -> Env {
    send_group("Loading environment file".to_owned());
    let env = Env::load();
    send_message("Successful".to_owned());

    env
}

async fn load_postgres_pool(env: &Env) -> std::result::Result<Pool<Postgres>, anyhow::Error> {
    let pg_options = PgConnectOptions::new()
        .host(env.postgres_host.as_str())
        .port(env.postgres_port)
        .username(env.postgres_user.as_str())
        .password(env.postgres_password.as_str())
        .database(env.postgres_database.as_str())
        .statement_cache_capacity(env.postgres_capacity);

    match PgPoolOptions::new()
        .min_connections(env.postgres_min_connections)
        .max_connections(env.postgres_max_connections)
        .max_lifetime(Duration::from_secs(3600))
        .after_connect(|conn, _meta| {
            Box::pin(async move {
                conn.execute("SET search_path TO public").await?;
                Ok(())
            })
        })
        .connect_with(pg_options)
        .await
    {
        Ok(pool) => Ok(pool),
        Err(error) => {
            send_message(format!("Error: {}", error.to_string()));
            panic!();
        }
    }
}

async fn load_redis_multiplex(
    env: &Env,
) -> std::result::Result<MultiplexedConnection, anyhow::Error> {
    let client = Client::open(format!("redis://{}:{}/", env.redis_host, env.redis_port)).unwrap();

    match client.get_multiplexed_tokio_connection().await {
        Ok(connection) => Ok(connection),
        Err(error) => {
            send_message(format!("Redis error: {}", error.to_string()));
            panic!();
        }
    }
}
