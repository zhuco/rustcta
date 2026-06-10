#![allow(dead_code)]

mod api;
mod api_keys;
mod app;
mod cross_arb;
mod dashboard_panels;
mod exchange_latency;
mod funding_arb;
mod i18n;
mod overview;
mod spot_arb;
mod storage;
mod types;
mod ui;
mod utils;
mod workspace;

fn main() {
    dioxus::launch(app::App);
}
