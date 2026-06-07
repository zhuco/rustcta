use base64::{engine::general_purpose, Engine as _};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::error::{Error, UrlError};
use tokio_tungstenite::tungstenite::handshake::client::Response;
use tokio_tungstenite::{client_async_tls_with_config, MaybeTlsStream, WebSocketStream};
use url::Url;

#[derive(Debug, Clone, PartialEq, Eq)]
struct HttpProxy {
    host: String,
    port: u16,
    authorization: Option<String>,
    source: &'static str,
}

pub async fn connect_async<R>(
    request: R,
) -> Result<(WebSocketStream<MaybeTlsStream<TcpStream>>, Response), Error>
where
    R: IntoClientRequest + Unpin,
{
    let request = request.into_client_request()?;
    let Some(proxy) = http_proxy_from_env(request.uri().host()) else {
        return tokio_tungstenite::connect_async(request).await;
    };

    let host = request
        .uri()
        .host()
        .ok_or(Error::Url(UrlError::NoHostName))?
        .to_string();
    let port = request
        .uri()
        .port_u16()
        .or_else(|| match request.uri().scheme_str() {
            Some("wss") => Some(443),
            Some("ws") => Some(80),
            _ => None,
        })
        .ok_or(Error::Url(UrlError::UnsupportedUrlScheme))?;

    log::debug!(
        "websocket connecting through HTTP proxy source={} proxy={}:{} target={}:{}",
        proxy.source,
        proxy.host,
        proxy.port,
        host,
        port
    );

    let stream = connect_http_tunnel(&proxy, &host, port).await?;
    client_async_tls_with_config(request, stream, None, None).await
}

async fn connect_http_tunnel(
    proxy: &HttpProxy,
    target_host: &str,
    target_port: u16,
) -> Result<TcpStream, Error> {
    let mut stream = TcpStream::connect((proxy.host.as_str(), proxy.port))
        .await
        .map_err(Error::Io)?;
    let authority = format!("{target_host}:{target_port}");
    let mut request = format!(
        "CONNECT {authority} HTTP/1.1\r\nHost: {authority}\r\nProxy-Connection: Keep-Alive\r\n"
    );
    if let Some(authorization) = &proxy.authorization {
        request.push_str("Proxy-Authorization: Basic ");
        request.push_str(authorization);
        request.push_str("\r\n");
    }
    request.push_str("\r\n");
    stream
        .write_all(request.as_bytes())
        .await
        .map_err(Error::Io)?;

    let mut reader = BufReader::new(stream);
    let mut status_line = String::new();
    let bytes = reader
        .read_line(&mut status_line)
        .await
        .map_err(Error::Io)?;
    if bytes == 0 || !status_line.contains(" 200 ") {
        return Err(Error::Url(UrlError::UnableToConnect(format!(
            "proxy CONNECT {authority} failed: {}",
            status_line.trim()
        ))));
    }

    loop {
        let mut line = String::new();
        let bytes = reader.read_line(&mut line).await.map_err(Error::Io)?;
        if bytes == 0 || line == "\r\n" || line == "\n" {
            break;
        }
    }

    Ok(reader.into_inner())
}

fn http_proxy_from_env(target_host: Option<&str>) -> Option<HttpProxy> {
    for source in [
        "RUSTCTA_WS_PROXY",
        "HTTPS_PROXY",
        "https_proxy",
        "HTTP_PROXY",
        "http_proxy",
        "ALL_PROXY",
        "all_proxy",
    ] {
        let Ok(value) = std::env::var(source) else {
            continue;
        };
        let value = value.trim();
        if value.is_empty() || no_proxy_matches(target_host, source) {
            continue;
        }
        match parse_http_proxy(value, source) {
            Some(proxy) => return Some(proxy),
            None => log::debug!(
                "ignoring unsupported websocket proxy source={} value={}",
                source,
                value
            ),
        }
    }
    None
}

fn parse_http_proxy(value: &str, source: &'static str) -> Option<HttpProxy> {
    let url_text = if value.contains("://") {
        value.to_string()
    } else {
        format!("http://{value}")
    };
    let url = Url::parse(&url_text).ok()?;
    if !matches!(url.scheme(), "http" | "https") {
        return None;
    }
    let host = url.host_str()?.to_string();
    let port = url.port().or_else(|| match url.scheme() {
        "http" => Some(80),
        "https" => Some(443),
        _ => None,
    })?;
    let authorization = if url.username().is_empty() {
        None
    } else {
        let password = url.password().unwrap_or("");
        Some(general_purpose::STANDARD.encode(format!("{}:{password}", url.username())))
    };
    Some(HttpProxy {
        host,
        port,
        authorization,
        source,
    })
}

fn no_proxy_matches(target_host: Option<&str>, source: &str) -> bool {
    if source == "RUSTCTA_WS_PROXY" {
        return false;
    }
    let Some(target_host) = target_host else {
        return false;
    };
    let no_proxy = std::env::var("NO_PROXY")
        .or_else(|_| std::env::var("no_proxy"))
        .unwrap_or_default();
    no_proxy.split(',').any(|entry| {
        let entry = entry.trim();
        if entry.is_empty() {
            false
        } else if entry == "*" {
            true
        } else if let Some(domain) = entry.strip_prefix('.') {
            target_host.ends_with(domain)
        } else {
            target_host == entry
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_http_proxy_should_accept_clash_http_port() {
        let proxy = parse_http_proxy("http://127.0.0.1:7897", "HTTPS_PROXY").unwrap();
        assert_eq!(proxy.host, "127.0.0.1");
        assert_eq!(proxy.port, 7897);
        assert_eq!(proxy.authorization, None);
    }

    #[test]
    fn parse_http_proxy_should_ignore_socks_proxy() {
        assert_eq!(
            parse_http_proxy("socks://127.0.0.1:7897", "ALL_PROXY"),
            None
        );
    }
}
