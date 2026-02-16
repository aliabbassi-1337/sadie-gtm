/**
 * Cloudflare Worker — outbound fetch proxy for the deeplink service.
 *
 * Builds clean browser-like headers from scratch to avoid WAF detection.
 * Only the target URL, method, cookies, and body are forwarded from the proxy.
 */

export default {
  async fetch(request, env) {
    // Auth check
    const authKey = request.headers.get("X-Proxy-Key");
    if (authKey !== env.PROXY_KEY) {
      return new Response("Unauthorized", { status: 401 });
    }

    const targetUrl = request.headers.get("X-Target-URL");
    if (!targetUrl) {
      return new Response("Missing X-Target-URL header", { status: 400 });
    }

    const url = new URL(targetUrl);

    // Build clean browser-like headers from scratch
    const headers = new Headers({
      host: url.host,
      "user-agent":
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
      accept:
        "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
      "accept-language": "en-US,en;q=0.9",
      "accept-encoding": "gzip, deflate, br",
      "sec-fetch-dest": "document",
      "sec-fetch-mode": "navigate",
      "sec-fetch-site": "none",
      "sec-fetch-user": "?1",
      "sec-ch-ua":
        '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
      "sec-ch-ua-mobile": "?0",
      "sec-ch-ua-platform": '"macOS"',
      "upgrade-insecure-requests": "1",
      "cache-control": "max-age=0",
      referer: url.origin + "/",
    });

    // Forward cookies if provided (upstream session cookies)
    const cookies = request.headers.get("X-Target-Cookie");
    if (cookies) {
      headers.set("cookie", cookies);
    }

    // Forward content-type for POST requests
    const contentType = request.headers.get("content-type");
    if (contentType) {
      headers.set("content-type", contentType);
    }

    // Forward extra headers for AJAX requests (JSON blob)
    const extra = request.headers.get("X-Target-Headers");
    if (extra) {
      try {
        const parsed = JSON.parse(extra);
        for (const [k, v] of Object.entries(parsed)) {
          headers.set(k, v);
        }
      } catch (_) {}
    }

    try {
      const resp = await fetch(targetUrl, {
        method: request.method,
        headers: headers,
        body:
          request.method !== "GET" && request.method !== "HEAD"
            ? request.body
            : undefined,
        redirect: "manual",
      });

      return new Response(resp.body, {
        status: resp.status,
        headers: resp.headers,
      });
    } catch (err) {
      return new Response(`Upstream error: ${err.message}`, { status: 502 });
    }
  },
};
