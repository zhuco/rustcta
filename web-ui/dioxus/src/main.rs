#![allow(dead_code)]

mod api;
mod api_keys;
mod app;
mod console_pages;
mod dashboard_panels;
mod exchange_latency;
mod i18n;
mod overview;
mod storage;
mod strategies;
mod types;
mod ui;
mod utils;

fn main() {
    dioxus::launch(app::App);
}
