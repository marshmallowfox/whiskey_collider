use std::{any::Any, collections::HashMap, sync::Arc, time::Duration};

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{Executor, Pool, Postgres, postgres::PgArguments, query::Query};
use tokio::sync::{Notify, RwLock};

use crate::common::output::send_group;
use sqlx::types::JsonValue;

#[derive(Clone, Debug)]
pub enum CommandValue {
    Int(i64),
    Str(String),
    Json(Value),
    Timestamp(DateTime<Utc>),
}

pub struct CommandBus {
    queries: Arc<RwLock<HashMap<String, Vec<Vec<CommandValue>>>>>,
    callbacks: Arc<RwLock<HashMap<String, Box<dyn Fn(&dyn Any) + Send + Sync>>>>,
}

impl CommandBus {
    pub fn init(duration: Duration, postgres: Pool<Postgres>) -> CommandBus {
        let queries: Arc<RwLock<HashMap<String, Vec<Vec<CommandValue>>>>> =
            Arc::new(RwLock::new(HashMap::new()));
        let callbacks: Arc<RwLock<HashMap<String, Box<dyn Fn(&dyn Any) + Send + Sync>>>> =
            Arc::new(RwLock::new(HashMap::new()));
        let notifier = Arc::new(Notify::new());

        let queries_clone = Arc::clone(&queries);
        let callbacks_clone = Arc::clone(&callbacks);
        let postgres_clone = postgres.clone();
        let notifier_clone = Arc::clone(&notifier);

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(duration);

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                    }
                    _ = notifier_clone.notified() => {
                    }
                }

                let mut queries = queries_clone.write().await;
                if queries.is_empty() {
                    continue;
                }

                for (query, param_sets) in queries.drain() {
                    for chunk in param_sets.chunks(2000) {
                        let chunk = chunk.to_vec();

                        if query.to_lowercase().contains("unnest") {
                            let q = bind_unnest(sqlx::query(&query), &chunk);
                            match q.fetch_one(&postgres_clone).await {
                                Ok(row) => {
                                    let read_callback = callbacks_clone.read().await;
                                    if let Some(function) = read_callback.get(&query) {
                                        function(&row);
                                    }
                                }
                                Err(e) => send_group(e.to_string()),
                            }
                        } else {
                            for _ in &chunk {
                                match postgres_clone.fetch_one(query.as_str()).await {
                                    Ok(row) => {
                                        let read_callback = callbacks_clone.read().await;
                                        if let Some(function) = read_callback.get(&query) {
                                            function(&row);
                                        }
                                    }
                                    Err(e) => send_group(e.to_string()),
                                }
                            }
                        }
                    }
                }

                let mut callbacks_map = callbacks_clone.write().await;
                callbacks_map.clear();
            }
        });

        CommandBus { queries, callbacks }
    }

    pub async fn push(
        &self,
        query: &str,
        params: Vec<CommandValue>,
        callback: Option<Box<dyn Fn(&dyn Any) + Send + Sync>>,
    ) {
        let mut queries = self.queries.write().await;
        queries.entry(query.to_string()).or_default().push(params);

        if callback.is_some() {
            let mut callbacks = self.callbacks.write().await;
            callbacks.insert(query.to_string(), callback.unwrap());
        }
    }
}

fn bind_unnest<'q>(
    mut q: Query<'q, Postgres, PgArguments>,
    rows: &[Vec<CommandValue>],
) -> Query<'q, Postgres, PgArguments> {
    if rows.is_empty() {
        return q;
    }

    let cols = rows[0].len();
    for col in 0..cols {
        match &rows[0][col] {
            CommandValue::Int(_) => {
                let arr: Vec<i64> = rows
                    .iter()
                    .map(|r| match &r[col] {
                        CommandValue::Int(v) => *v,
                        _ => panic!("Param type mismatch at col {}", col),
                    })
                    .collect();
                q = q.bind(arr);
            }
            CommandValue::Str(_) => {
                let arr: Vec<String> = rows
                    .iter()
                    .map(|r| match &r[col] {
                        CommandValue::Str(v) => v.clone(),
                        _ => panic!("Param type mismatch at col {}", col),
                    })
                    .collect();
                q = q.bind(arr);
            }
            CommandValue::Json(_) => {
                let arr: Vec<JsonValue> = rows
                    .iter()
                    .map(|r| match &r[col] {
                        CommandValue::Json(v) => v.clone(),
                        _ => panic!("Param type mismatch at col {}", col),
                    })
                    .collect();
                q = q.bind(arr);
            }
            CommandValue::Timestamp(_) => {
                let arr: Vec<DateTime<Utc>> = rows
                    .iter()
                    .map(|r| match &r[col] {
                        CommandValue::Timestamp(v) => *v,
                        _ => panic!("Param type mismatch at col {}", col),
                    })
                    .collect();
                q = q.bind(arr);
            }
        }
    }

    q
}
