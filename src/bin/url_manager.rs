use std::io::ErrorKind;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use axum::extract::{Path as AxumPath, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, put};
use axum::{Json, Router};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use clap::Parser;
use serde::{Deserialize, Serialize};
use tokio::fs;
use tokio::sync::RwLock;
use url::Url;
use uuid::Uuid;

const MAX_URLS: usize = 100;
const PLATFORMS: &[&str] = &[
    "平博",
    "IM",
    "雷火",
    "小艾",
    "沙巴",
    "DB",
    "雷竞技",
    "newbb",
];

#[derive(Parser, Debug)]
#[command(name = "url-manager", version, about = "简单网址管理程序")]
struct Args {
    #[arg(long, default_value = "0.0.0.0")]
    host: String,
    #[arg(long, default_value_t = 8090)]
    port: u16,
    #[arg(long, default_value = "config/url_manager.json")]
    data: PathBuf,
    #[arg(long, default_value = "admin")]
    username: String,
    #[arg(long, default_value = "abc20155")]
    password: String,
}

#[derive(Clone)]
struct AppState {
    data_path: Arc<PathBuf>,
    urls: Arc<RwLock<Vec<ManagedUrl>>>,
    username: Arc<str>,
    password: Arc<str>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ManagedUrl {
    #[serde(default = "new_id")]
    id: String,
    name: String,
    platforms: String,
    url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct UrlStore {
    items: Vec<ManagedUrl>,
}

#[derive(Debug, Deserialize)]
struct UrlPayload {
    name: String,
    platforms: String,
    url: String,
}

#[derive(Debug)]
struct UrlDraft {
    name: String,
    platforms: String,
    url: String,
}

#[derive(Debug, Serialize)]
struct UrlListResponse {
    items: Vec<ManagedUrl>,
    total: usize,
    limit: usize,
    platforms: &'static [&'static str],
}

#[derive(Debug, Serialize)]
struct DeleteResponse {
    ok: bool,
}

#[derive(Debug, Serialize)]
struct ApiError {
    error: String,
}

fn new_id() -> String {
    Uuid::new_v4().to_string()
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    env_logger::init();

    let args = Args::parse();
    let mut urls = load_urls(&args.data).await?;
    if urls.len() > MAX_URLS {
        anyhow::bail!(
            "网址数据超过上限: {} 条，当前程序上限为 {} 条",
            urls.len(),
            MAX_URLS
        );
    }

    for item in &mut urls {
        item.platforms = normalize_platforms(&item.platforms)
            .map_err(|err| anyhow!("网址「{}」的平台不在固定平台列表内: {err}", item.name))?;
    }
    save_urls(&args.data, &urls).await?;

    let state = AppState {
        data_path: Arc::new(args.data),
        urls: Arc::new(RwLock::new(urls)),
        username: Arc::from(args.username),
        password: Arc::from(args.password),
    };

    let app = Router::new()
        .route("/", get(index))
        .route("/admin", get(admin))
        .route("/url-manager", get(index))
        .route("/url-manager/", get(index))
        .route("/url-manager/admin", get(admin))
        .route("/healthz", get(healthz))
        .route("/url-manager/healthz", get(healthz))
        .route("/api/urls", get(list_urls).post(create_url))
        .route("/api/urls/:id", put(update_url).delete(delete_url))
        .route("/url-manager/api/urls", get(list_urls).post(create_url))
        .route(
            "/url-manager/api/urls/:id",
            put(update_url).delete(delete_url),
        )
        .with_state(state);

    let addr: SocketAddr = format!("{}:{}", args.host, args.port)
        .parse()
        .with_context(|| "host/port 解析失败")?;

    log::info!("网址管理程序启动成功: http://{}", addr);
    log::info!("默认账号: admin，默认密码: abc20155");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

async fn index() -> Response {
    Html(INDEX_HTML).into_response()
}

async fn admin() -> Response {
    Html(ADMIN_HTML).into_response()
}

async fn healthz() -> &'static str {
    "ok"
}

async fn list_urls(State(state): State<AppState>) -> Response {
    let items = state.urls.read().await.clone();
    Json(UrlListResponse {
        total: items.len(),
        items,
        limit: MAX_URLS,
        platforms: PLATFORMS,
    })
    .into_response()
}

async fn create_url(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<UrlPayload>,
) -> Response {
    if let Err(response) = require_auth(&state, &headers) {
        return response;
    }

    let draft = match normalize_payload(payload) {
        Ok(draft) => draft,
        Err(message) => return json_error(StatusCode::BAD_REQUEST, message),
    };

    let mut urls = state.urls.write().await;
    if urls.len() >= MAX_URLS {
        return json_error(
            StatusCode::CONFLICT,
            format!("最多只能保存 {} 条网址", MAX_URLS),
        );
    }

    let item = ManagedUrl {
        id: new_id(),
        name: draft.name,
        platforms: draft.platforms,
        url: draft.url,
    };
    urls.push(item.clone());

    if let Err(err) = save_urls(&state.data_path, &urls).await {
        urls.pop();
        return json_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("保存失败: {err}"),
        );
    }

    (StatusCode::CREATED, Json(item)).into_response()
}

async fn update_url(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(id): AxumPath<String>,
    Json(payload): Json<UrlPayload>,
) -> Response {
    if let Err(response) = require_auth(&state, &headers) {
        return response;
    }

    let draft = match normalize_payload(payload) {
        Ok(draft) => draft,
        Err(message) => return json_error(StatusCode::BAD_REQUEST, message),
    };

    let mut urls = state.urls.write().await;
    let Some(position) = urls.iter().position(|item| item.id == id) else {
        return json_error(StatusCode::NOT_FOUND, "未找到该网址");
    };

    let previous = urls[position].clone();
    urls[position].name = draft.name;
    urls[position].platforms = draft.platforms;
    urls[position].url = draft.url;
    let item = urls[position].clone();

    if let Err(err) = save_urls(&state.data_path, &urls).await {
        urls[position] = previous;
        return json_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("保存失败: {err}"),
        );
    }

    Json(item).into_response()
}

async fn delete_url(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(id): AxumPath<String>,
) -> Response {
    if let Err(response) = require_auth(&state, &headers) {
        return response;
    }

    let mut urls = state.urls.write().await;
    let Some(position) = urls.iter().position(|item| item.id == id) else {
        return json_error(StatusCode::NOT_FOUND, "未找到该网址");
    };

    let removed = urls.remove(position);
    if let Err(err) = save_urls(&state.data_path, &urls).await {
        urls.insert(position, removed);
        return json_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("保存失败: {err}"),
        );
    }

    Json(DeleteResponse { ok: true }).into_response()
}

fn require_auth(state: &AppState, headers: &HeaderMap) -> Result<(), Response> {
    let Some(value) = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
    else {
        return Err(unauthorized());
    };

    let Some(encoded) = value.strip_prefix("Basic ") else {
        return Err(unauthorized());
    };

    let decoded = STANDARD.decode(encoded).map_err(|_| unauthorized())?;
    let credentials = String::from_utf8(decoded).map_err(|_| unauthorized())?;
    let expected = format!("{}:{}", state.username, state.password);

    if credentials == expected {
        Ok(())
    } else {
        Err(unauthorized())
    }
}

fn unauthorized() -> Response {
    json_error(StatusCode::UNAUTHORIZED, "管理密码错误或未填写")
}

fn normalize_payload(payload: UrlPayload) -> Result<UrlDraft, String> {
    let name = payload.name.trim().to_string();
    let platforms = normalize_platforms(&payload.platforms)?;
    let url = payload.url.trim().to_string();

    if name.is_empty() {
        return Err("名称不能为空".to_string());
    }
    if url.is_empty() {
        return Err("网址不能为空".to_string());
    }
    if name.chars().count() > 120 {
        return Err("名称不能超过 120 个字符".to_string());
    }
    if url.chars().count() > 2048 {
        return Err("网址不能超过 2048 个字符".to_string());
    }

    let parsed = Url::parse(&url).map_err(|_| "网址格式不正确".to_string())?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return Err("网址只支持 http 或 https".to_string());
    }

    Ok(UrlDraft {
        name,
        platforms,
        url,
    })
}

fn normalize_platforms(input: &str) -> Result<String, String> {
    let mut selected = Vec::new();
    let mut unknown = Vec::new();

    for token in input
        .split(|c: char| {
            c.is_whitespace() || matches!(c, ',' | '，' | '、' | '/' | ';' | '；' | '|')
        })
        .map(str::trim)
        .filter(|token| !token.is_empty())
    {
        if PLATFORMS.contains(&token) {
            if !selected.contains(&token) {
                selected.push(token);
            }
        } else {
            unknown.push(token.to_string());
        }
    }

    if !unknown.is_empty() {
        return Err(format!(
            "平台只能选择固定项，未知平台: {}",
            unknown.join("、")
        ));
    }

    let normalized = PLATFORMS
        .iter()
        .filter(|platform| selected.iter().any(|selected| selected == *platform))
        .copied()
        .collect::<Vec<_>>()
        .join(" ");

    if normalized.is_empty() {
        return Err("至少选择一个支持平台".to_string());
    }

    Ok(normalized)
}

fn json_error(status: StatusCode, message: impl Into<String>) -> Response {
    (
        status,
        Json(ApiError {
            error: message.into(),
        }),
    )
        .into_response()
}

async fn load_urls(path: &Path) -> Result<Vec<ManagedUrl>> {
    let content = match fs::read_to_string(path).await {
        Ok(content) => content,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => {
            return Err(err).with_context(|| format!("读取数据文件失败: {}", path.display()))
        }
    };

    if content.trim().is_empty() {
        return Ok(Vec::new());
    }

    match serde_json::from_str::<UrlStore>(&content) {
        Ok(store) => Ok(store.items),
        Err(store_err) => serde_json::from_str::<Vec<ManagedUrl>>(&content).with_context(|| {
            format!(
                "解析数据文件失败: {}，既不是 {{\"items\": [...]}} 也不是数组格式；原始错误: {store_err}",
                path.display()
            )
        }),
    }
}

async fn save_urls(path: &Path, urls: &[ManagedUrl]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .await
            .with_context(|| format!("创建数据目录失败: {}", parent.display()))?;
    }

    let content = serde_json::to_string_pretty(&UrlStore {
        items: urls.to_vec(),
    })?;
    let tmp_path = path.with_extension("json.tmp");
    fs::write(&tmp_path, format!("{content}\n"))
        .await
        .with_context(|| format!("写入临时数据文件失败: {}", tmp_path.display()))?;
    fs::rename(&tmp_path, path)
        .await
        .with_context(|| format!("替换数据文件失败: {}", path.display()))?;

    Ok(())
}

const INDEX_HTML: &str = r##"<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>电竞网址导航 - 电竞平台网址入口大全</title>
  <meta name="description" content="电竞网址导航收录常用电竞平台入口，支持按平博、IM、雷火、小艾、沙巴、DB、雷竞技、newbb 等平台快速筛选访问。" />
  <meta name="keywords" content="电竞网址导航,电竞平台,电竞网址,平博,IM,雷火,小艾,沙巴,DB,雷竞技,newbb" />
  <meta name="robots" content="index,follow" />
  <meta name="theme-color" content="#10201f" />
  <link rel="canonical" href="https://url.clawdbotweb.site/" />
  <meta property="og:type" content="website" />
  <meta property="og:site_name" content="电竞网址导航" />
  <meta property="og:title" content="电竞网址导航 - 电竞平台网址入口大全" />
  <meta property="og:description" content="按平台快速筛选常用电竞网址入口，覆盖平博、IM、雷火、小艾、沙巴、DB、雷竞技、newbb 等平台。" />
  <meta property="og:url" content="https://url.clawdbotweb.site/" />
  <meta name="twitter:card" content="summary" />
  <script type="application/ld+json">
    {
      "@context": "https://schema.org",
      "@type": "WebSite",
      "name": "电竞网址导航",
      "url": "https://url.clawdbotweb.site/",
      "description": "电竞网址导航收录常用电竞平台入口，支持按平台快速筛选访问。",
      "inLanguage": "zh-CN"
    }
  </script>
  <style>
    :root {
      color-scheme: light;
      --page: #f3f5f8;
      --ink: #101828;
      --muted: #667085;
      --line: #d8e0ea;
      --panel: #ffffff;
      --brand: #12312f;
      --accent: #0f766e;
      --shadow: 0 18px 44px rgba(16, 24, 40, 0.12);
      --soft-shadow: 0 1px 2px rgba(16, 24, 40, 0.08);
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }

    * { box-sizing: border-box; }

    body {
      margin: 0;
      min-height: 100vh;
      background:
        linear-gradient(180deg, #10201f 0, #10201f 248px, transparent 248px),
        linear-gradient(135deg, rgba(15, 118, 110, 0.18), transparent 42%),
        var(--page);
      color: var(--ink);
      font-size: 15px;
      line-height: 1.5;
    }

    .shell {
      width: min(1360px, calc(100vw - 32px));
      margin: 0 auto;
      padding: 24px 0 40px;
    }

    .topbar {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 18px;
      color: #fff;
      margin-bottom: 22px;
    }

    .brand {
      display: flex;
      align-items: center;
      gap: 14px;
      min-width: 0;
    }

    .brand-mark {
      display: grid;
      place-items: center;
      width: 54px;
      height: 54px;
      border-radius: 8px;
      background: linear-gradient(135deg, #0f766e, #b7791f);
      box-shadow: 0 18px 34px rgba(0, 0, 0, 0.22);
      color: #fff;
      font-weight: 850;
    }

    h1 {
      margin: 0;
      font-size: clamp(28px, 4vw, 42px);
      line-height: 1.08;
      letter-spacing: 0;
    }

    .subtitle {
      margin-top: 6px;
      color: rgba(255, 255, 255, 0.72);
    }

    .admin-link {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 40px;
      border: 1px solid rgba(255, 255, 255, 0.18);
      border-radius: 7px;
      padding: 0 14px;
      color: #fff;
      text-decoration: none;
      font-weight: 760;
      background: rgba(255, 255, 255, 0.1);
    }

    .filter-panel {
      border: 1px solid rgba(255, 255, 255, 0.18);
      border-radius: 8px;
      background: rgba(255, 255, 255, 0.96);
      box-shadow: var(--shadow);
      padding: 14px;
      margin-bottom: 18px;
    }

    .filters {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
    }

    button {
      border: 1px solid var(--line);
      border-radius: 7px;
      min-height: 40px;
      padding: 0 13px;
      background: #fff;
      color: var(--ink);
      cursor: pointer;
      font: inherit;
      font-weight: 800;
      letter-spacing: 0;
    }

    button.active {
      border-color: transparent;
      background: var(--accent);
      color: #fff;
      box-shadow: 0 10px 20px rgba(15, 118, 110, 0.22);
    }

    .platform-dot {
      display: inline-grid;
      place-items: center;
      width: 26px;
      height: 26px;
      border-radius: 7px;
      margin-right: 7px;
      color: #fff;
      font-size: 12px;
      font-weight: 850;
      vertical-align: middle;
      background: var(--platform-color, #475467);
    }

    .summary {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      color: var(--muted);
      margin: 0 2px 14px;
      font-weight: 760;
    }

    .grid {
      display: grid;
      grid-template-columns: repeat(5, minmax(0, 1fr));
      gap: 14px;
    }

    .site-card {
      display: grid;
      gap: 14px;
      min-height: 168px;
      border: 1px solid rgba(216, 224, 234, 0.9);
      border-radius: 8px;
      background: var(--panel);
      color: inherit;
      text-decoration: none;
      padding: 16px;
      box-shadow: var(--soft-shadow);
      transition: transform 0.14s ease, box-shadow 0.14s ease, border-color 0.14s ease;
    }

    .site-card:hover {
      border-color: #a8bacb;
      box-shadow: var(--shadow);
      transform: translateY(-2px);
    }

    .card-top {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
    }

    .site-name {
      margin: 0;
      font-size: 20px;
      font-weight: 850;
      letter-spacing: 0;
      overflow-wrap: anywhere;
    }

    .domain {
      color: var(--muted);
      font-size: 13px;
      overflow-wrap: anywhere;
    }

    .chips {
      display: flex;
      flex-wrap: wrap;
      gap: 7px;
      align-content: start;
    }

    .chip {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      min-height: 28px;
      border-radius: 7px;
      padding: 4px 9px;
      color: var(--platform-color, #475467);
      background: var(--platform-bg, #eef2f6);
      font-size: 12px;
      font-weight: 820;
    }

    .empty {
      border: 1px dashed var(--line);
      border-radius: 8px;
      background: #fff;
      color: var(--muted);
      padding: 38px;
      text-align: center;
      grid-column: 1 / -1;
    }

    @media (max-width: 1180px) {
      .grid { grid-template-columns: repeat(4, minmax(0, 1fr)); }
    }

    @media (max-width: 920px) {
      .grid { grid-template-columns: repeat(3, minmax(0, 1fr)); }
    }

    @media (max-width: 680px) {
      .shell { width: min(100vw - 22px, 1360px); padding-top: 16px; }
      .topbar { align-items: flex-start; flex-direction: column; }
      .grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .site-card { min-height: 154px; }
    }

    @media (max-width: 430px) {
      .grid { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <div class="shell">
    <header class="topbar">
      <div class="brand">
        <div class="brand-mark">URL</div>
        <div>
          <h1>电竞网址导航</h1>
          <div class="subtitle">按平台快速筛选常用电竞入口</div>
        </div>
      </div>
      <a class="admin-link" id="admin-link" href="/admin">后台管理</a>
    </header>

    <section class="filter-panel">
      <div class="filters" id="filters"></div>
    </section>

    <div class="summary">
      <span id="summary">加载中</span>
      <span>url.clawdbotweb.site</span>
    </div>

    <main class="grid" id="grid"></main>
  </div>

  <script>
    const PLATFORM_META = {
      '平博': { icon: '平', color: '#0f766e', bg: '#d8f3ee' },
      'IM': { icon: 'IM', color: '#3b5bdb', bg: '#e7edff' },
      '雷火': { icon: '雷', color: '#c2410c', bg: '#ffedd5' },
      '小艾': { icon: '艾', color: '#be185d', bg: '#fce7f3' },
      '沙巴': { icon: '沙', color: '#a16207', bg: '#fef3c7' },
      'DB': { icon: 'DB', color: '#0369a1', bg: '#e0f2fe' },
      '雷竞技': { icon: '竞', color: '#6d28d9', bg: '#ede9fe' },
      'newbb': { icon: 'NB', color: '#047857', bg: '#d1fae5' },
    };
    const state = { items: [], platforms: Object.keys(PLATFORM_META), active: '全部' };
    const grid = document.getElementById('grid');
    const filters = document.getElementById('filters');
    const summary = document.getElementById('summary');
    const adminLink = document.getElementById('admin-link');

    function apiBase() {
      return window.location.pathname.startsWith('/url-manager') ? '/url-manager/' : '/';
    }

    function apiUrl(path) {
      return new URL(`${apiBase()}${path}`, window.location.origin).toString();
    }

    function adminUrl() {
      return window.location.pathname.startsWith('/url-manager') ? '/url-manager/admin' : '/admin';
    }

    function platformList(value) {
      return String(value || '').split(/\s+/).map((item) => item.trim()).filter(Boolean);
    }

    function domainOf(url) {
      try { return new URL(url).hostname; } catch (_err) { return url; }
    }

    function platformStyle(platform) {
      const meta = PLATFORM_META[platform] || { color: '#475467', bg: '#eef2f6' };
      return `--platform-color:${meta.color};--platform-bg:${meta.bg}`;
    }

    function platformIcon(platform) {
      const meta = PLATFORM_META[platform] || { icon: platform.slice(0, 2) };
      return meta.icon;
    }

    function renderFilters() {
      const counts = new Map([['全部', state.items.length]]);
      state.platforms.forEach((platform) => counts.set(platform, 0));
      state.items.forEach((item) => {
        platformList(item.platforms).forEach((platform) => {
          counts.set(platform, (counts.get(platform) || 0) + 1);
        });
      });

      filters.replaceChildren(...['全部', ...state.platforms].map((platform) => {
        const button = document.createElement('button');
        button.type = 'button';
        button.className = state.active === platform ? 'active' : '';
        if (platform !== '全部') {
          const dot = document.createElement('span');
          dot.className = 'platform-dot';
          dot.style = platformStyle(platform);
          dot.textContent = platformIcon(platform);
          button.appendChild(dot);
        }
        button.append(document.createTextNode(`${platform} ${counts.get(platform) || 0}`));
        button.addEventListener('click', () => {
          state.active = platform;
          render();
        });
        return button;
      }));
    }

    function visibleItems() {
      if (state.active === '全部') {
        return state.items;
      }
      return state.items.filter((item) => platformList(item.platforms).includes(state.active));
    }

    function renderCards(items) {
      if (items.length === 0) {
        grid.innerHTML = '<div class="empty">当前平台暂无网址</div>';
        return;
      }

      grid.replaceChildren(...items.map((item) => {
        const card = document.createElement('a');
        card.className = 'site-card';
        card.href = item.url;

        const top = document.createElement('div');
        top.className = 'card-top';
        const name = document.createElement('h2');
        name.className = 'site-name';
        name.textContent = item.name;
        top.appendChild(name);

        const domain = document.createElement('div');
        domain.className = 'domain';
        domain.textContent = domainOf(item.url);

        const chips = document.createElement('div');
        chips.className = 'chips';
        platformList(item.platforms).forEach((platform) => {
          const chip = document.createElement('span');
          chip.className = 'chip';
          chip.style = platformStyle(platform);
          chip.innerHTML = `<span class="platform-dot" style="${platformStyle(platform)}">${platformIcon(platform)}</span>${platform}`;
          chips.appendChild(chip);
        });

        card.append(top, domain, chips);
        return card;
      }));
    }

    function render() {
      const items = visibleItems();
      renderFilters();
      summary.textContent = `${state.active} · ${items.length} / ${state.items.length} 个网址`;
      renderCards(items);
    }

    async function load() {
      adminLink.href = adminUrl();
      const response = await fetch(apiUrl('api/urls'));
      const data = await response.json();
      state.items = data.items || [];
      state.platforms = data.platforms || state.platforms;
      render();
    }

    load().catch((err) => {
      summary.textContent = `加载失败: ${err.message}`;
      grid.innerHTML = '<div class="empty">数据加载失败</div>';
    });
  </script>
</body>
</html>
"##;

const ADMIN_HTML: &str = r##"<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>后台管理</title>
  <meta name="robots" content="noindex,nofollow,noarchive" />
  <style>
    :root {
      color-scheme: light;
      --page: #f3f5f8;
      --ink: #101828;
      --muted: #667085;
      --line: #d8e0ea;
      --panel: #ffffff;
      --accent: #0f766e;
      --accent-dark: #0b5f59;
      --danger: #b42318;
      --danger-bg: #fff0ed;
      --shadow: 0 16px 40px rgba(16, 24, 40, 0.1);
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }

    * { box-sizing: border-box; }

    body {
      margin: 0;
      min-height: 100vh;
      background:
        linear-gradient(180deg, #121f2d 0, #121f2d 178px, transparent 178px),
        var(--page);
      color: var(--ink);
      font-size: 15px;
      line-height: 1.5;
    }

    .shell {
      width: min(1280px, calc(100vw - 32px));
      margin: 0 auto;
      padding: 22px 0 38px;
    }

    .topbar {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      margin-bottom: 18px;
      color: #fff;
    }

    h1 {
      margin: 0;
      font-size: 30px;
      letter-spacing: 0;
    }

    .nav-link {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 40px;
      border: 1px solid rgba(255, 255, 255, 0.18);
      border-radius: 7px;
      padding: 0 14px;
      color: #fff;
      text-decoration: none;
      font-weight: 760;
      background: rgba(255, 255, 255, 0.1);
    }

    .panel {
      border: 1px solid rgba(216, 224, 234, 0.92);
      border-radius: 8px;
      background: var(--panel);
      box-shadow: var(--shadow);
      overflow: hidden;
    }

    .edit-panel {
      overflow: visible;
      position: relative;
      z-index: 5;
    }

    .panel-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      padding: 14px 16px;
      border-bottom: 1px solid var(--line);
      background: #f8fafc;
    }

    .status {
      color: var(--muted);
      min-height: 22px;
      text-align: right;
    }

    .status.ok { color: #087443; }
    .status.error { color: var(--danger); }

    form {
      display: grid;
      grid-template-columns: minmax(150px, 1fr) minmax(260px, 1.5fr) minmax(220px, 1fr) minmax(150px, 0.8fr) auto auto;
      gap: 12px;
      align-items: end;
      padding: 16px;
    }

    label,
    .field {
      display: grid;
      gap: 7px;
      color: var(--muted);
      font-size: 12px;
      font-weight: 820;
    }

    input {
      width: 100%;
      min-width: 0;
      height: 42px;
      border: 1px solid #b8c4d2;
      border-radius: 7px;
      padding: 9px 11px;
      outline: none;
      font: inherit;
    }

    input:focus {
      border-color: var(--accent);
      box-shadow: 0 0 0 4px rgba(15, 118, 110, 0.14);
    }

    button {
      min-height: 40px;
      border: 1px solid transparent;
      border-radius: 7px;
      padding: 0 13px;
      cursor: pointer;
      font: inherit;
      font-weight: 780;
      letter-spacing: 0;
      white-space: nowrap;
    }

    button:disabled { opacity: 0.55; cursor: not-allowed; }

    .primary {
      background: var(--accent);
      color: #fff;
      box-shadow: 0 10px 20px rgba(15, 118, 110, 0.2);
    }

    .primary:hover { background: var(--accent-dark); }

    .secondary {
      border-color: #b8c4d2;
      background: #fff;
      color: var(--ink);
    }

    .danger {
      border-color: #ffd2cc;
      background: var(--danger-bg);
      color: var(--danger);
    }

    details.multi {
      position: relative;
      width: 100%;
    }

    details.multi summary {
      display: flex;
      align-items: center;
      min-height: 42px;
      border: 1px solid #b8c4d2;
      border-radius: 7px;
      background: #fff;
      padding: 0 11px;
      color: var(--ink);
      cursor: pointer;
      list-style: none;
    }

    details.multi summary::-webkit-details-marker { display: none; }

    .multi-menu {
      position: absolute;
      z-index: 50;
      top: calc(100% + 6px);
      left: 0;
      width: min(320px, 92vw);
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #fff;
      box-shadow: var(--shadow);
      padding: 8px;
    }

    .check-row {
      display: flex;
      align-items: center;
      gap: 9px;
      min-height: 36px;
      border-radius: 7px;
      padding: 6px 8px;
      color: var(--ink);
      font-size: 14px;
      font-weight: 760;
    }

    .check-row:hover { background: #f3f6f9; }
    .check-row input { width: 16px; height: 16px; }

    .toolbar {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      margin: 16px 0 12px;
    }

    .search {
      max-width: 420px;
      box-shadow: 0 1px 2px rgba(16, 24, 40, 0.08);
    }

    .table-wrap { overflow-x: auto; }

    table {
      width: 100%;
      min-width: 900px;
      border-collapse: collapse;
      table-layout: fixed;
    }

    th, td {
      border-bottom: 1px solid var(--line);
      padding: 13px 14px;
      text-align: left;
      vertical-align: middle;
    }

    th {
      background: #f8fafc;
      color: var(--muted);
      font-size: 12px;
      font-weight: 850;
    }

    .name-col { width: 18%; font-weight: 850; }
    .platform-col { width: 28%; }
    .url-col { width: 36%; overflow-wrap: anywhere; color: var(--muted); }
    .action-col { width: 18%; }

    .chips {
      display: flex;
      flex-wrap: wrap;
      gap: 7px;
    }

    .chip {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      min-height: 28px;
      border-radius: 7px;
      padding: 4px 9px;
      color: var(--platform-color, #475467);
      background: var(--platform-bg, #eef2f6);
      font-size: 12px;
      font-weight: 820;
    }

    .platform-dot {
      display: inline-grid;
      place-items: center;
      width: 22px;
      height: 22px;
      border-radius: 6px;
      color: #fff;
      background: var(--platform-color, #475467);
      font-size: 11px;
      font-weight: 850;
    }

    .actions {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      justify-content: flex-end;
    }

    .actions button { min-height: 34px; padding: 0 10px; }

    .empty {
      color: var(--muted);
      text-align: center;
      padding: 34px;
    }

    @media (max-width: 960px) {
      form { grid-template-columns: 1fr 1fr; }
    }

    @media (max-width: 620px) {
      .shell { width: min(100vw - 22px, 1280px); }
      .topbar, .panel-head, .toolbar { align-items: flex-start; flex-direction: column; }
      form { grid-template-columns: 1fr; }
      button { width: 100%; }
      .status { text-align: left; }
    }
  </style>
</head>
<body>
  <div class="shell">
    <header class="topbar">
      <div>
        <h1>后台管理</h1>
        <div>新增、编辑、删除需要管理密码</div>
      </div>
      <a class="nav-link" id="home-link" href="/">返回首页</a>
    </header>

    <section class="panel edit-panel">
      <div class="panel-head">
        <strong>网址资料</strong>
        <div class="status" id="status"></div>
      </div>
      <form id="url-form" autocomplete="off">
        <label>
          名称
          <input id="name" maxlength="120" required />
        </label>
        <label>
          网址
          <input id="url" type="url" maxlength="2048" placeholder="https://" required />
        </label>
        <div class="field">
          <span>支持平台</span>
          <details class="multi" id="platform-menu">
            <summary id="platform-summary">选择平台</summary>
            <div class="multi-menu" id="platform-options"></div>
          </details>
        </div>
        <label>
          管理密码
          <input id="password" type="password" autocomplete="current-password" />
        </label>
        <button class="primary" id="submit-btn" type="submit">新增</button>
        <button class="secondary" id="cancel-btn" type="button" hidden>取消</button>
      </form>
    </section>

    <section class="toolbar">
      <input class="search" id="search" type="search" placeholder="搜索名称、平台或网址" />
      <div id="summary">加载中</div>
    </section>

    <section class="panel table-wrap">
      <table>
        <thead>
          <tr>
            <th class="name-col">名称</th>
            <th class="platform-col">支持平台</th>
            <th class="url-col">网址</th>
            <th class="action-col">操作</th>
          </tr>
        </thead>
        <tbody id="url-list">
          <tr><td class="empty" colspan="4">加载中</td></tr>
        </tbody>
      </table>
    </section>
  </div>

  <script>
    const PLATFORM_META = {
      '平博': { icon: '平', color: '#0f766e', bg: '#d8f3ee' },
      'IM': { icon: 'IM', color: '#3b5bdb', bg: '#e7edff' },
      '雷火': { icon: '雷', color: '#c2410c', bg: '#ffedd5' },
      '小艾': { icon: '艾', color: '#be185d', bg: '#fce7f3' },
      '沙巴': { icon: '沙', color: '#a16207', bg: '#fef3c7' },
      'DB': { icon: 'DB', color: '#0369a1', bg: '#e0f2fe' },
      '雷竞技': { icon: '竞', color: '#6d28d9', bg: '#ede9fe' },
      'newbb': { icon: 'NB', color: '#047857', bg: '#d1fae5' },
    };

    const state = { items: [], platforms: Object.keys(PLATFORM_META), editingId: null };
    const el = {
      form: document.getElementById('url-form'),
      name: document.getElementById('name'),
      url: document.getElementById('url'),
      password: document.getElementById('password'),
      submit: document.getElementById('submit-btn'),
      cancel: document.getElementById('cancel-btn'),
      status: document.getElementById('status'),
      list: document.getElementById('url-list'),
      search: document.getElementById('search'),
      summary: document.getElementById('summary'),
      platformMenu: document.getElementById('platform-menu'),
      platformSummary: document.getElementById('platform-summary'),
      platformOptions: document.getElementById('platform-options'),
      homeLink: document.getElementById('home-link'),
    };

    function apiBase() {
      return window.location.pathname.startsWith('/url-manager') ? '/url-manager/' : '/';
    }

    function apiUrl(path) {
      return new URL(`${apiBase()}${path}`, window.location.origin).toString();
    }

    function homeUrl() {
      return window.location.pathname.startsWith('/url-manager') ? '/url-manager/' : '/';
    }

    async function api(path, options = {}) {
      const response = await fetch(path, {
        ...options,
        credentials: 'same-origin',
        headers: {
          'Content-Type': 'application/json',
          ...(options.headers || {}),
        },
      });
      const text = await response.text();
      const data = text ? JSON.parse(text) : null;
      if (!response.ok) {
        throw new Error(data?.error || `请求失败: ${response.status}`);
      }
      return data;
    }

    function authHeaders() {
      if (!el.password.value) {
        throw new Error('请输入管理密码');
      }
      return { Authorization: `Basic ${btoa(`admin:${el.password.value}`)}` };
    }

    function setStatus(message, type = '') {
      el.status.textContent = message;
      el.status.className = `status ${type}`.trim();
    }

    function platformList(value) {
      return String(value || '').split(/\s+/).map((item) => item.trim()).filter(Boolean);
    }

    function selectedPlatforms() {
      return [...el.platformOptions.querySelectorAll('input[type="checkbox"]:checked')]
        .map((input) => input.value);
    }

    function setSelectedPlatforms(platforms) {
      const selected = new Set(platforms);
      el.platformOptions.querySelectorAll('input[type="checkbox"]').forEach((input) => {
        input.checked = selected.has(input.value);
      });
      refreshPlatformSummary();
    }

    function platformStyle(platform) {
      const meta = PLATFORM_META[platform] || { color: '#475467', bg: '#eef2f6' };
      return `--platform-color:${meta.color};--platform-bg:${meta.bg}`;
    }

    function platformIcon(platform) {
      const meta = PLATFORM_META[platform] || { icon: platform.slice(0, 2) };
      return meta.icon;
    }

    function refreshPlatformSummary() {
      const values = selectedPlatforms();
      el.platformSummary.textContent = values.length ? values.join(' / ') : '选择平台';
    }

    function renderPlatformOptions() {
      el.platformOptions.replaceChildren(...state.platforms.map((platform) => {
        const row = document.createElement('label');
        row.className = 'check-row';
        const input = document.createElement('input');
        input.type = 'checkbox';
        input.value = platform;
        input.addEventListener('change', refreshPlatformSummary);
        const dot = document.createElement('span');
        dot.className = 'platform-dot';
        dot.style = platformStyle(platform);
        dot.textContent = platformIcon(platform);
        row.append(input, dot, document.createTextNode(platform));
        return row;
      }));
      refreshPlatformSummary();
    }

    function chips(platforms) {
      const wrap = document.createElement('div');
      wrap.className = 'chips';
      platformList(platforms).forEach((platform) => {
        const chip = document.createElement('span');
        chip.className = 'chip';
        chip.style = platformStyle(platform);
        chip.innerHTML = `<span class="platform-dot" style="${platformStyle(platform)}">${platformIcon(platform)}</span>${platform}`;
        wrap.appendChild(chip);
      });
      return wrap;
    }

    function visibleItems() {
      const query = el.search.value.trim().toLowerCase();
      if (!query) {
        return state.items;
      }
      return state.items.filter((item) => [item.name, item.platforms, item.url].join(' ').toLowerCase().includes(query));
    }

    function render() {
      const items = visibleItems();
      el.summary.textContent = `${items.length} / ${state.items.length} 条`;

      if (items.length === 0) {
        el.list.innerHTML = '<tr><td class="empty" colspan="4">暂无数据</td></tr>';
        return;
      }

      el.list.replaceChildren(...items.map((item) => {
        const row = document.createElement('tr');
        const name = document.createElement('td');
        name.className = 'name-col';
        name.textContent = item.name;

        const platforms = document.createElement('td');
        platforms.className = 'platform-col';
        platforms.appendChild(chips(item.platforms));

        const url = document.createElement('td');
        url.className = 'url-col';
        url.textContent = item.url;

        const actions = document.createElement('td');
        actions.className = 'action-col';
        const actionWrap = document.createElement('div');
        actionWrap.className = 'actions';

        const edit = document.createElement('button');
        edit.className = 'secondary';
        edit.type = 'button';
        edit.textContent = '编辑';
        edit.addEventListener('click', () => startEdit(item));

        const remove = document.createElement('button');
        remove.className = 'danger';
        remove.type = 'button';
        remove.textContent = '删除';
        remove.addEventListener('click', () => removeItem(item));

        actionWrap.append(edit, remove);
        actions.appendChild(actionWrap);
        row.append(name, platforms, url, actions);
        return row;
      }));
    }

    function startEdit(item) {
      state.editingId = item.id;
      el.name.value = item.name;
      el.url.value = item.url;
      setSelectedPlatforms(platformList(item.platforms));
      el.submit.textContent = '保存';
      el.cancel.hidden = false;
      el.name.focus();
      setStatus(`正在编辑: ${item.name}`);
    }

    function resetForm() {
      const password = el.password.value;
      state.editingId = null;
      el.form.reset();
      el.password.value = password;
      setSelectedPlatforms([]);
      el.submit.textContent = '新增';
      el.cancel.hidden = true;
    }

    async function submitForm(event) {
      event.preventDefault();
      el.submit.disabled = true;
      const platforms = selectedPlatforms();
      if (platforms.length === 0) {
        el.submit.disabled = false;
        setStatus('至少选择一个支持平台', 'error');
        return;
      }

      const payload = {
        name: el.name.value,
        platforms: platforms.join(' '),
        url: el.url.value,
      };

      try {
        if (state.editingId) {
          const updated = await api(apiUrl(`api/urls/${encodeURIComponent(state.editingId)}`), {
            method: 'PUT',
            headers: authHeaders(),
            body: JSON.stringify(payload),
          });
          state.items = state.items.map((item) => item.id === updated.id ? updated : item);
          setStatus('已保存', 'ok');
        } else {
          const created = await api(apiUrl('api/urls'), {
            method: 'POST',
            headers: authHeaders(),
            body: JSON.stringify(payload),
          });
          state.items = [...state.items, created];
          setStatus('已新增', 'ok');
        }
        resetForm();
        render();
      } catch (err) {
        setStatus(err.message, 'error');
      } finally {
        el.submit.disabled = false;
      }
    }

    async function removeItem(item) {
      if (!el.password.value) {
        el.password.focus();
        setStatus('请输入管理密码', 'error');
        return;
      }
      if (!confirm(`删除「${item.name}」？`)) {
        return;
      }
      try {
        await api(apiUrl(`api/urls/${encodeURIComponent(item.id)}`), {
          method: 'DELETE',
          headers: authHeaders(),
        });
        state.items = state.items.filter((candidate) => candidate.id !== item.id);
        if (state.editingId === item.id) {
          resetForm();
        }
        render();
        setStatus('已删除', 'ok');
      } catch (err) {
        setStatus(err.message, 'error');
      }
    }

    async function load() {
      el.homeLink.href = homeUrl();
      const data = await api(apiUrl('api/urls'));
      state.items = data.items || [];
      state.platforms = data.platforms || state.platforms;
      renderPlatformOptions();
      render();
      setStatus('已加载', 'ok');
    }

    el.form.addEventListener('submit', submitForm);
    el.cancel.addEventListener('click', () => {
      resetForm();
      setStatus('');
    });
    el.search.addEventListener('input', render);
    document.addEventListener('click', (event) => {
      if (!el.platformMenu.contains(event.target)) {
        el.platformMenu.open = false;
      }
    });

    load().catch((err) => {
      setStatus(err.message, 'error');
      el.list.innerHTML = '<tr><td class="empty" colspan="4">加载失败</td></tr>';
    });
  </script>
</body>
</html>
"##;
