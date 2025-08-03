use once_cell::sync::Lazy;
use std::{
    collections::HashMap,
    io::{self, Write},
    sync::Mutex,
};

static GROUPS: Lazy<Mutex<HashMap<String, Vec<String>>>> = Lazy::new(|| Mutex::new(HashMap::new()));
static CURRENT_GROUP: Lazy<Mutex<Option<String>>> = Lazy::new(|| Mutex::new(None));

pub fn send_group(group_name: String) {
    let mut groups = GROUPS.lock().unwrap();
    groups.entry(group_name.clone()).or_default();
    *CURRENT_GROUP.lock().unwrap() = Some(group_name.clone());
    println!("\x1B[94m• {}\x1B[0m", group_name);
}

pub fn send_message(message: String) {
    let mut groups = GROUPS.lock().unwrap();
    let group = CURRENT_GROUP
        .lock()
        .unwrap()
        .as_ref()
        .expect("WHERE IS GROUP FAGGOT")
        .clone();
    let msgs = groups.get_mut(&group).unwrap();

    let gray = "\x1B[90m";
    let reset = "\x1B[0m";

    if !msgs.is_empty() {
        print!("\x1B[1A\r\x1B[K");

        let prev = msgs.last().unwrap();
        let prefix = if need_prefix(prev) { "├─" } else { "│ " };

        println!("{}{}{}{}", gray, prefix, prev, reset);
    }

    let prefix = if need_prefix(&message) {
        "└─"
    } else {
        "│ "
    };
    println!("{}{}{}{}", gray, prefix, message, reset);
    io::stdout().flush().unwrap();

    msgs.push(message);
}

pub fn need_prefix(string: &str) -> bool {
    !matches!(string.chars().next(), Some('├') | Some('└'))
}
