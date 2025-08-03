use std::collections::HashMap;

use serde_json::from_slice;

use crate::contexts::events::infrastructure::cached_projection::EventsProj;

pub async fn get_type(proj: &EventsProj, event_type: &str) -> Result<Option<i64>, anyhow::Error> {
    let types: HashMap<String, i64> = proj.get_types_name_id().await;

    Ok(match types.get(event_type) {
        Some(&id) => Some(id),
        None => None,
    })
}

pub async fn is_user_exist(proj: &EventsProj, user_id: i64) -> Result<bool, anyhow::Error> {
    let users: Vec<i64> = from_slice(proj.get_users_id().await.as_mut())?;

    let is_exist = users.contains(&user_id);

    Ok(is_exist.to_owned())
}
