use std::time::Duration;

pub fn decrypt() -> bool {
    true
}

pub fn dir_perm() -> String {
    "0700".to_string()
}

pub fn file_perm() -> String {
    "0600".to_string()
}

pub fn wait_time_seconds() -> i32 {
    20
}

pub fn max_messages() -> i32 {
    10
}

pub fn visibility_timeout() -> i32 {
    30
}

pub fn on_change_timeout() -> Duration {
    Duration::from_secs(30)
}

pub fn on_change_debounce() -> Duration {
    Duration::from_secs(2)
}

pub fn setup_event_bus_name() -> String {
    "default".to_string()
}

pub fn setup_visibility_timeout() -> i32 {
    30
}

pub fn setup_message_retention_seconds() -> i32 {
    345_600
}

pub fn dlq_max_receive_count() -> i32 {
    5
}

pub fn log_level() -> String {
    "info".to_string()
}

pub fn log_format() -> String {
    "text".to_string()
}
