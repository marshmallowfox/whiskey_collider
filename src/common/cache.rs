use std::collections::HashMap;

use anyhow::Error;
use futures::future::try_join_all;
use moka::future::Cache;
use redis::{AsyncTypedCommands, aio::MultiplexedConnection};
use tokio::try_join;

pub enum CacheSetKey {
    Exact(String),
    Pattern(String, Vec<String>),
}

pub enum CacheDeleteKey {
    Exact(String),
    Pattern(String),
}

pub struct LeveledCache {
    redis: MultiplexedConnection,
    lru: Cache<String, Vec<u8>>,
    saved_pattern: HashMap<String, Vec<String>>,
}

impl LeveledCache {
    pub fn create(redis: MultiplexedConnection, lru: Cache<String, Vec<u8>>) -> LeveledCache {
        LeveledCache {
            redis,
            lru,
            saved_pattern: HashMap::new(),
        }
    }

    pub async fn try_get(&self, key: String) -> Option<Vec<u8>> {
        if let Some(bytes) = &self.lru.get(&key).await {
            return Some(bytes.to_owned());
        }

        let conn = &self.redis.clone();

        let redis_value = conn.to_owned().get(&key).await.unwrap();
        if let Some(data) = redis_value {
            let bytes = data.into_bytes();

            self.lru.insert(key.clone(), bytes.clone()).await;
            return Some(bytes);
        }

        None
    }

    pub async fn save(
        &mut self,
        key: CacheSetKey,
        value: Vec<u8>,
        seconds: u64,
    ) -> Result<(), Error> {
        match key {
            CacheSetKey::Exact(key) => {
                let conn = &self.redis.clone();

                let key_for_redis = key.clone();
                let value_for_redis = value.clone();
                let redis_fut = async move {
                    conn.to_owned()
                        .set_ex(key_for_redis, value_for_redis, seconds)
                        .await
                        .map(|_| ())
                        .map_err(Error::from)
                };

                let key_for_lru = key.clone();
                let lru_fut = async move {
                    self.lru.insert(key_for_lru, value).await;
                    Ok::<(), Error>(())
                };

                let _ = try_join!(redis_fut, lru_fut);

                Ok(())
            }
            CacheSetKey::Pattern(pattern, values) => {
                let contain = self.saved_pattern.contains_key(&pattern);

                if !contain {
                    &self
                        .saved_pattern
                        .insert(pattern.clone(), Vec::<String>::new());
                }

                let key = fill_placeholders(pattern.clone(), &values);

                &self
                    .saved_pattern
                    .get_mut(&pattern)
                    .unwrap()
                    .push(key.clone());

                let conn = &self.redis.clone();

                let key_for_redis = key.clone();
                let value_for_redis = value.clone();
                let redis_fut = async move {
                    conn.to_owned()
                        .set_ex(key_for_redis, value_for_redis, seconds)
                        .await
                        .map(|_| ())
                        .map_err(Error::from)
                };

                let key_for_lru = key.clone();
                let lru_fut = async move {
                    self.lru.insert(key_for_lru, value).await;
                    Ok::<(), Error>(())
                };

                let _ = try_join!(redis_fut, lru_fut);

                Ok(())
            }
        }
    }

    pub async fn invalidate(&self, key: CacheDeleteKey) -> Result<(), Error> {
        let conn = &self.redis.clone();

        match key {
            CacheDeleteKey::Exact(key) => {
                let key_for_redis = key.clone();
                let redis_fut = async move {
                    conn.to_owned()
                        .del(key_for_redis)
                        .await
                        .map(|_| ())
                        .map_err(Error::from)
                };

                let key_for_lru = key.clone();
                let lru_fut = async move {
                    self.lru.invalidate(&key_for_lru).await;
                    Ok::<(), Error>(())
                };

                let _ = try_join!(redis_fut, lru_fut);

                Ok(())
            }
            CacheDeleteKey::Pattern(pattern) => {
                let cache_keys: Vec<String> = self
                    .saved_pattern
                    .get(&pattern)
                    .unwrap_or(&Vec::<String>::new())
                    .to_owned();

                let futures = cache_keys.iter().map(|cache_key| {
                    let mut redis_conn = conn.clone();
                    let key_for_redis = cache_key.clone();
                    let lru = self.lru.clone();

                    async move {
                        let redis_fut = async {
                            redis_conn
                                .del(key_for_redis.clone())
                                .await
                                .map(|_| ())
                                .map_err(Error::from)
                        };

                        let lru_fut = async {
                            lru.invalidate(&key_for_redis).await;
                            Ok::<(), Error>(())
                        };

                        futures::try_join!(redis_fut, lru_fut).map(|_| ())
                    }
                });

                let _ = try_join_all(futures);

                Ok(())
            }
        }
    }
}

fn fill_placeholders(mut template: String, values: &Vec<String>) -> String {
    for val in values {
        template = template.replacen("{}", val, 1);
    }
    template
}
