use dotenvy::dotenv;
use http::Uri;
use std::env;

pub struct Env {
    pub port: u16,
    pub postgres_min_connections: u32,
    pub postgres_max_connections: u32,
    pub postgres_url: String,
    pub postgres_host: String,
    pub postgres_port: u16,
    pub postgres_password: String,
    pub postgres_user: String,
    pub postgres_database: String,
    pub postgres_capacity: usize,
    pub redis_host: String,
    pub redis_port: u16,
    pub app_cache: u64,
}

impl Env {
    pub fn load() -> Env {
        dotenv().ok();

        let postgres_url: String = env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://root:secret@127.0.0.1:5433/app".into());

        let uri: Uri = postgres_url
            .parse()
            .expect("Invalid DATABASE_URL, expected postgres://user:pass@host:port/db");

        let auth = uri
            .authority()
            .expect("DATABASE_URL missing authority")
            .as_str();

        let (creds, host_port) = auth.split_once('@').unwrap_or(("", auth));

        let (postgres_user, postgres_password) =
            creds.split_once(':').unwrap_or(("root", "secret"));

        let (postgres_host, port_str) = host_port.split_once(':').unwrap_or(("127.0.0.1", "5433"));

        let postgres_port = port_str.parse().unwrap_or(5433);

        let postgres_database = uri.path().trim_start_matches('/').to_string();

        Env {
            port: env::var("APP_PORT")
                .ok()
                .and_then(|v| v.parse::<u16>().ok())
                .unwrap_or(80u16),
            postgres_min_connections: env::var("POSTGRES_CONNECTIONS_MIN")
                .ok()
                .and_then(|v| v.parse::<u32>().ok())
                .unwrap_or(20u32),
            postgres_max_connections: env::var("POSTGRES_CONNECTIONS_MAX")
                .ok()
                .and_then(|v| v.parse::<u32>().ok())
                .unwrap_or(200u32),
            postgres_url,
            postgres_host: postgres_host.to_owned(),
            postgres_port,
            postgres_password: postgres_password.to_owned(),
            postgres_user: postgres_user.to_owned(),
            postgres_database,
            postgres_capacity: env::var("POSTGRES_CAPACITY")
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(256usize),
            redis_host: env::var("REDIS_HOST")
                .ok()
                .unwrap_or("127.0.0.1".to_owned()),
            redis_port: env::var("REDIS_PORT")
                .ok()
                .and_then(|v| v.parse::<u16>().ok())
                .unwrap_or(6379u16),
            app_cache: env::var("APP_CACHE")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(50u64),
        }
    }
}
