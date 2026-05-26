#!/usr/bin/env bash
set -euo pipefail

CADDY_API="${CADDY_API:-http://127.0.0.1:2019}"
UPSTREAM="${URL_MANAGER_UPSTREAM:-127.0.0.1:8090}"
HOSTNAME="${URL_MANAGER_DOMAIN:-url.clawdbotweb.site}"
TMP_CURRENT="$(mktemp)"
TMP_NEXT="$(mktemp)"

cleanup() {
  rm -f "${TMP_CURRENT}" "${TMP_NEXT}"
}
trap cleanup EXIT

curl -fsS --max-time 5 "${CADDY_API}/config/" > "${TMP_CURRENT}"

if jq -e --arg hostname "${HOSTNAME}" --arg upstream "${UPSTREAM}" '
  def has_domain_route:
    any(.apps.http.servers.srv0.routes[]?;
      any((.match // [])[]?.host[]?; . == $hostname)
      and any(.handle[0].routes[]?.handle[]?.upstreams[]?.dial?; . == $upstream)
    );

  def host_has_path_route($host):
    any(.apps.http.servers.srv0.routes[]?;
      any((.match // [])[]?.host[]?; . == $host)
      and any(.handle[0].routes[]?;
        any((.match // [])[]?.path[]?; . == "/url-manager" or . == "/url-manager/*")
        and any(.handle[]?.upstreams[]?.dial?; . == $upstream)
      )
    );

  has_domain_route
  and host_has_path_route("ddd.clawdbotweb.site")
  and host_has_path_route("clawdbotweb.site")
' "${TMP_CURRENT}" >/dev/null; then
  echo "Caddy route already configured for ${HOSTNAME}"
  exit 0
fi

jq --arg hostname "${HOSTNAME}" --arg upstream "${UPSTREAM}" '
  def url_manager_site($hostname; $upstream): {
    "handle": [
      {
        "handler": "subroute",
        "routes": [
          {
            "handle": [
              {
                "handler": "reverse_proxy",
                "upstreams": [
                  {"dial": $upstream}
                ]
              }
            ]
          }
        ]
      }
    ],
    "match": [
      {"host": [$hostname]}
    ],
    "terminal": true
  };

  def has_host($hostname):
    any(.apps.http.servers.srv0.routes[]?; any((.match // [])[]?.host[]?; . == $hostname));

  def has_path_route:
    any(.handle[0].routes[]?; any((.match // [])[]?.path[]?; . == "/url-manager" or . == "/url-manager/*"));

  def path_route($upstream): {
    "handle": [
      {
        "handler": "reverse_proxy",
        "upstreams": [
          {"dial": $upstream}
        ]
      }
    ],
    "match": [
      {"path": ["/url-manager", "/url-manager/*"]}
    ]
  };

  if has_host($hostname) then
    .
  else
    .apps.http.servers.srv0.routes = ([url_manager_site($hostname; $upstream)] + .apps.http.servers.srv0.routes)
  end
  |
  .apps.http.servers.srv0.routes |= map(
    if (.handle[0].handler? == "subroute") and any((.match // [])[]?.host[]?; . == "ddd.clawdbotweb.site" or . == "clawdbotweb.site" or . == "www.clawdbotweb.site") then
      if has_path_route then . else .handle[0].routes = ([path_route($upstream)] + .handle[0].routes) end
    else
      .
    end
  )
' "${TMP_CURRENT}" > "${TMP_NEXT}"

if cmp -s "${TMP_CURRENT}" "${TMP_NEXT}"; then
  echo "Caddy route already configured for ${HOSTNAME}"
  exit 0
fi

curl -fsS --max-time 10 -X POST \
  -H 'Content-Type: application/json' \
  --data-binary "@${TMP_NEXT}" \
  "${CADDY_API}/load"

echo "Caddy route configured for ${HOSTNAME} -> ${UPSTREAM}"
