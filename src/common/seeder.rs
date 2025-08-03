use std::thread::current;

use chrono::{DateTime, Duration, NaiveDateTime, SecondsFormat, Utc};
use itoa::Buffer;
use rand::rngs::StdRng;
use rand::{RngCore, SeedableRng};
use sqlx::{Executor, Pool, Postgres, query};
use tokio::io::AsyncWriteExt;
use tokio::try_join;
use tokio::{io::BufWriter, process::Command};

use crate::common::output::send_group;
use crate::common::{env::Env, output::send_message, snowflake::next_id};

pub async fn seed(pool: Pool<Postgres>, env: Env) {
    let start = Utc::now();

    let (users_opt, types_opt) =
        try_join!(create_users(&pool), create_types(&pool)).expect("Slucilos govno");

    let users_id = users_opt.expect("Slucilos strannoe govno (users_id is None)");
    let types_id = types_opt.expect("Slucilos strannoe govno (types_id is None)");

    send_message("Users and types created".to_owned());

    delete_indexes(&pool).await;

    send_message("Indexes deleted".to_owned());

    create_events(&pool, &env, users_id, types_id).await;

    send_message("Events created".to_owned());

    let duration = Utc::now().signed_duration_since(start).num_seconds();
    send_message(format!("─ Duration of seed {} seconds", duration));

    create_indexes(&pool).await;

    send_message("Indexes restored".to_owned());
}

pub async fn create_users(pool: &Pool<Postgres>) -> Result<Option<Vec<i64>>, anyhow::Error> {
    let names_array = vec!["Izya", "Kot", "Nikolayi", "Whiskey", "Michael"];
    let names_len = names_array.len();

    let mut ids: Vec<i64> = Vec::with_capacity(100);
    let mut names: Vec<String> = Vec::with_capacity(100);
    (1..=100).for_each(|i| {
        let id = next_id();
        let name = names_array[i % names_len];

        &ids.push(id);
        &names.push(name.to_owned());
    });

    query(
        r#"
        INSERT INTO users (id, name)
        SELECT * FROM UNNEST($1::bigint[], $2::text[])
        "#,
    )
    .bind(ids.clone())
    .bind(names.clone())
    .execute(pool)
    .await?;

    Ok(Some(ids))
}

pub async fn create_types(pool: &Pool<Postgres>) -> Result<Option<Vec<i64>>, anyhow::Error> {
    let types_array = vec![
        "user.registered",
        "user.login",
        "user.logout",
        "user.updated",
        "order.created",
        "order.paid",
        "order.shipped",
        "order.delivered",
        "payment.processed",
        "payment.failed",
        "payment.refunded",
        "product.viewed",
        "product.added_to_cart",
        "product.removed_from_cart",
        "email.sent",
        "email.opened",
        "email.clicked",
        "notification.sent",
        "notification.read",
        "api.request",
        "api.response",
        "api.error",
        "user.password_reset_requested",
        "user.password_changed",
        "user.two_factor_enabled",
        "user.two_factor_disabled",
        "user.deleted",
        "user.suspended",
        "user.reactivated",
        "user.subscription_started",
        "user.subscription_cancelled",
        "user.subscription_renewed",
        "user.invited",
        "user.invite_accepted",
        "user.feedback_submitted",
        "user.avatar_uploaded",
        "user.preferences_updated",
        "user.email_verified",
        "user.login_failed",
        "user.profile_viewed",
        "user.notification_preferences_updated",
        "user.newsletter_subscribed",
        "order.cancelled",
        "order.return_requested",
        "order.return_approved",
        "order.return_rejected",
        "order.review_submitted",
        "order.invoice_generated",
        "payment.pending",
        "payment.disputed",
        "payment.settled",
        "cart.viewed",
        "cart.updated",
        "cart.cleared",
        "checkout.started",
        "checkout.completed",
        "product.review_submitted",
        "product.wishlisted",
        "product.unwishlisted",
        "product.compared",
        "product.shared",
        "product.restock_requested",
        "product.stock_low",
        "email.bounced",
        "email.unsubscribed",
        "notification.dismissed",
        "notification.failed",
        "session.started",
        "session.expired",
        "session.terminated",
        "admin.login",
        "admin.logout",
        "admin.updated_user",
        "admin.deleted_user",
        "admin.generated_report",
        "admin.settings_updated",
        "file.uploaded",
        "file.deleted",
        "file.downloaded",
        "file.previewed",
        "support.ticket_created",
        "support.ticket_closed",
        "support.ticket_reopened",
        "support.message_sent",
        "support.rating_submitted",
        "search.performed",
        "search.filtered",
        "search.sorted",
        "settings.updated",
        "language.changed",
        "timezone.changed",
        "api.token_generated",
        "api.token_revoked",
        "api.rate_limited",
        "cron.job_started",
        "cron.job_finished",
        "cron.job_failed",
        "webhook.received",
        "webhook.verified",
        "webhook.failed",
    ];
    let types_len = types_array.len();

    let mut ids: Vec<i64> = Vec::with_capacity(types_len);
    let mut names: Vec<String> = Vec::with_capacity(types_len);
    (1..=types_len).for_each(|i| {
        let id = next_id();
        let name = types_array[i % types_len];

        &ids.push(id);
        &names.push(name.to_owned());
    });

    query(
        r#"
        INSERT INTO event_types (id, name)
        SELECT * FROM UNNEST($1::bigint[], $2::text[])
        "#,
    )
    .bind(ids.clone())
    .bind(names.clone())
    .execute(pool)
    .await?;

    Ok(Some(ids))
}

pub async fn create_events(
    pool: &Pool<Postgres>,
    env: &Env,
    users_id: Vec<i64>,
    types_id: Vec<i64>,
) -> Result<(), anyhow::Error> {
    let pages: Vec<&str> = vec![
        "{\"page\":\"/registration\"}",
        "{\"page\":\"/login\"}",
        "{\"page\":\"/logout\"}",
        "{\"page\":\"/profile/edit\"}",
        "{\"page\":\"/order/create\"}",
        "{\"page\":\"/order/confirm\"}",
        "{\"page\":\"/order/shipped\"}",
        "{\"page\":\"/order/tracking\"}",
        "{\"page\":\"/payment/complete\"}",
        "{\"page\":\"/payment/failed\"}",
        "{\"page\":\"/payment/refund\"}",
        "{\"page\":\"/product/view\"}",
        "{\"page\":\"/cart/add\"}",
        "{\"page\":\"/cart/remove\"}",
        "{\"page\":\"/emails/sent\"}",
        "{\"page\":\"/emails/opened\"}",
        "{\"page\":\"/emails/click\"}",
        "{\"page\":\"/notifications/sent\"}",
        "{\"page\":\"/notifications/read\"}",
        "{\"page\":\"/api/request\"}",
        "{\"page\":\"/api/response\"}",
        "{\"page\":\"/api/error\"}",
        "{\"page\":\"/password/reset\"}",
        "{\"page\":\"/password/change\"}",
        "{\"page\":\"/security/2fa\"}",
        "{\"page\":\"/security/2fa/disable\"}",
        "{\"page\":\"/account/delete\"}",
        "{\"page\":\"/account/suspend\"}",
        "{\"page\":\"/account/reactivate\"}",
        "{\"page\":\"/subscription/start\"}",
        "{\"page\":\"/subscription/cancel\"}",
        "{\"page\":\"/subscription/renew\"}",
        "{\"page\":\"/invite/send\"}",
        "{\"page\":\"/invite/accept\"}",
        "{\"page\":\"/feedback\"}",
        "{\"page\":\"/profile/avatar\"}",
        "{\"page\":\"/profile/preferences\"}",
        "{\"page\":\"/email/verify\"}",
        "{\"page\":\"/login/failed\"}",
        "{\"page\":\"/profile/view\"}",
        "{\"page\":\"/profile/notifications\"}",
        "{\"page\":\"/newsletter/subscribe\"}",
        "{\"page\":\"/order/cancel\"}",
        "{\"page\":\"/order/return\"}",
        "{\"page\":\"/order/return/approved\"}",
        "{\"page\":\"/order/return/rejected\"}",
        "{\"page\":\"/order/review\"}",
        "{\"page\":\"/order/invoice\"}",
        "{\"page\":\"/payment/pending\"}",
        "{\"page\":\"/payment/dispute\"}",
        "{\"page\":\"/payment/settled\"}",
        "{\"page\":\"/cart/view\"}",
        "{\"page\":\"/cart/update\"}",
        "{\"page\":\"/cart/clear\"}",
        "{\"page\":\"/checkout/start\"}",
        "{\"page\":\"/checkout/complete\"}",
        "{\"page\":\"/product/review\"}",
        "{\"page\":\"/wishlist/add\"}",
        "{\"page\":\"/wishlist/remove\"}",
        "{\"page\":\"/product/compare\"}",
        "{\"page\":\"/product/share\"}",
        "{\"page\":\"/product/restock\"}",
        "{\"page\":\"/product/stock\"}",
        "{\"page\":\"/emails/bounced\"}",
        "{\"page\":\"/emails/unsubscribe\"}",
        "{\"page\":\"/notifications/dismiss\"}",
        "{\"page\":\"/notifications/failure\"}",
        "{\"page\":\"/session/start\"}",
        "{\"page\":\"/session/expired\"}",
        "{\"page\":\"/session/end\"}",
        "{\"page\":\"/admin/login\"}",
        "{\"page\":\"/admin/logout\"}",
        "{\"page\":\"/admin/user/edit\"}",
        "{\"page\":\"/admin/user/delete\"}",
        "{\"page\":\"/admin/reports\"}",
        "{\"page\":\"/admin/settings\"}",
        "{\"page\":\"/files/upload\"}",
        "{\"page\":\"/files/delete\"}",
        "{\"page\":\"/files/download\"}",
        "{\"page\":\"/files/preview\"}",
        "{\"page\":\"/support/create\"}",
        "{\"page\":\"/support/close\"}",
        "{\"page\":\"/support/reopen\"}",
        "{\"page\":\"/support/message\"}",
        "{\"page\":\"/support/rating\"}",
        "{\"page\":\"/search\"}",
        "{\"page\":\"/search/filter\"}",
        "{\"page\":\"/search/sort\"}",
        "{\"page\":\"/settings\"}",
        "{\"page\":\"/settings/language\"}",
        "{\"page\":\"/settings/timezone\"}",
        "{\"page\":\"/api/token\"}",
        "{\"page\":\"/api/token/revoke\"}",
        "{\"page\":\"/api/rate-limit\"}",
        "{\"page\":\"/cron/start\"}",
        "{\"page\":\"/cron/end\"}",
        "{\"page\":\"/cron/failure\"}",
        "{\"page\":\"/webhooks/incoming\"}",
        "{\"page\":\"/webhooks/verified\"}",
        "{\"page\":\"/webhooks/failure\"}",
    ];

    let byte_users_id: Vec<Vec<u8>> = users_id
        .iter()
        .map(|id| id.to_string().into_bytes())
        .collect();

    let byte_types_id: Vec<Vec<u8>> = types_id
        .iter()
        .map(|a| a.to_string().into_bytes())
        .collect();

    let byte_pages: Vec<Vec<u8>> = pages
        .iter()
        .map(|raw| {
            let escaped = raw.replace('"', "\"\"");
            let field = format!("\"{}\"", escaped);
            field.into_bytes()
        })
        .collect();

    let database = env.postgres_database.clone();
    let host = env.postgres_host.clone();
    let port = env.postgres_port.clone().to_string();
    let user = env.postgres_user.clone();
    let password = env.postgres_password.clone();

    let mut child = Command::new("pg_bulkload")
        .arg("-d")
        .arg(database)
        .arg("-h")
        .arg(host)
        .arg("-p")
        .arg(port)
        .arg("-U")
        .arg(user)
        .env("PGPASSWORD", password)
        .arg("events.ctl")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit())
        .spawn()
        .expect("failed to spawn pg_bulkload");

    let mut writer = BufWriter::with_capacity(
        16 * 1024 * 1024,
        child.stdin.take().expect("stdin not piped"),
    );

    let mut chunk = Vec::<u8>::with_capacity(12 * 1024 * 1024);
    let chunk_mb: usize = 8 * 1024 * 1024;

    let mut key = 0;
    let users_len = byte_users_id.len();
    let types_len = byte_types_id.len();
    let metadata_len = byte_pages.len();

    let now = Utc::now();
    let one_year_ago = now - Duration::days(365);
    let start_ts = one_year_ago.timestamp();
    let end_ts = now.timestamp();

    let mut rng = StdRng::from_os_rng();

    let mut id_buf = Buffer::new();

    for _ in 1..=10_000_000 {
        let mut line = [0u8; 128];
        let mut pos = 0;

        let user_idx = key % users_len;
        let type_idx = key % types_len;
        let metadata_idx = key % metadata_len;
        let user_bytes = &byte_users_id[user_idx];
        let type_bytes = &byte_types_id[type_idx];
        let metadata_bytes = &byte_pages[metadata_idx];

        let ts_bytes = rand_timestamp(&mut rng, start_ts, end_ts)
            .to_rfc3339_opts(SecondsFormat::Secs, true)
            .into_bytes();

        key += 1;

        let id = next_id();

        let bytes_id = id_buf.format(id).as_bytes();

        macro_rules! push_slice {
            ($src:expr) => {{
                let s: &[u8] = $src;
                let len = s.len();
                line[pos..pos + len].copy_from_slice(s);
                pos += len;
            }};
        }

        push_slice!(bytes_id);
        line[pos] = b',';
        pos += 1;

        push_slice!(&user_bytes);
        line[pos] = b',';
        pos += 1;

        push_slice!(&type_bytes);
        line[pos] = b',';
        pos += 1;

        push_slice!(&ts_bytes);
        line[pos] = b',';
        pos += 1;

        push_slice!(&metadata_bytes);
        line[pos] = b'\n';
        pos += 1;

        chunk.extend_from_slice(&line[..pos]);

        if chunk.len() >= chunk_mb {
            writer.write_all(&chunk).await.expect("Failed to Write");
            chunk.clear();
        }
    }

    if !chunk.is_empty() {
        writer.write_all(&chunk).await.expect("Failed to Write");
    }
    writer.flush().await.expect("Failed to flush");
    drop(writer);

    let status = child
        .wait()
        .await
        .expect("pg_bulkload process failed to run");
    if !status.success() {
        send_group(format!("pg_bulkload exit with {:?}", status.code()));
    }

    Ok(())
}

pub async fn create_indexes(pool: &Pool<Postgres>) {
    match pool
        .execute(
            r#"
    CREATE INDEX IF NOT EXISTS idx_events_count
        ON events USING btree (id);
    
    CREATE INDEX IF NOT EXISTS idx_events_user_timestamp
        ON events USING btree (user_id, timestamp DESC);
    
    CREATE INDEX IF NOT EXISTS idx_events_timestamp_desc
        ON events USING btree (timestamp DESC);
    
    CREATE INDEX IF NOT EXISTS idx_events_type_timestamp
        ON events USING btree (type_id, timestamp DESC);
    
    CREATE INDEX IF NOT EXISTS idx_events_stats
        ON events USING btree (user_id, (metadata->>'page'), type_id);
    
    CREATE INDEX IF NOT EXISTS idx_events_covering
        ON events USING btree (user_id, type_id, timestamp DESC)
        INCLUDE (id, metadata);
        "#,
        )
        .await
    {
        Ok(_) => {}
        Err(e) => {
            panic!("{}", e)
        }
    };
}

pub async fn delete_indexes(pool: &Pool<Postgres>) {
    match pool
        .execute(
            r#"
DROP INDEX IF EXISTS idx_events_count;

DROP INDEX IF EXISTS idx_events_user_timestamp;

DROP INDEX IF EXISTS idx_events_timestamp_desc;

DROP INDEX IF EXISTS idx_events_type_timestamp;

DROP INDEX IF EXISTS idx_events_stats;

DROP INDEX IF EXISTS idx_events_covering;
    "#,
        )
        .await
    {
        Ok(_) => {}
        Err(e) => {
            panic!("{}", e)
        }
    };
}

fn rand_timestamp(rng: &mut impl RngCore, start_ts: i64, end_ts: i64) -> DateTime<Utc> {
    let range = (end_ts - start_ts + 1) as u32;
    let sec = (rng.next_u32() % range) as i64 + start_ts;
    DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(sec, 0), Utc)
}
