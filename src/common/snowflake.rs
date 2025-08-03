use snowflake::SnowflakeIdGenerator;
use std::cell::RefCell;
use std::sync::atomic::{AtomicU32, Ordering};

static NEXT_THREAD_ID: AtomicU32 = AtomicU32::new(0);

thread_local! {
    static THIS_THREAD_ID: u32 = NEXT_THREAD_ID.fetch_add(1, Ordering::Relaxed);
}

thread_local! {
    pub static SNOWFLAKE_GENERATOR: RefCell<SnowflakeIdGenerator> =
        RefCell::new(SnowflakeIdGenerator::new(1, THIS_THREAD_ID.with(|id| *id as i32)));
}

pub fn next_id() -> i64 {
    SNOWFLAKE_GENERATOR.with(|g| g.borrow_mut().lazy_generate())
}
