/**
 * Cloudflare Worker proxy for web scraping + API requests with IP rotation.
 *
 * Deployed to Cloudflare's edge network (300+ cities).
 * Outbound requests originate from the nearest Cloudflare data center IP.
 *
 * Features:
 * - Auth via X-Auth-Key header
 * - User-Agent rotation
 * - KV caching (optional, reduces redundant fetches, preserves content type)
 * - Returns X-Worker-Colo header showing which data center handled the request
 * - Supports JSON APIs (RDAP, crt.sh) via X-Forward-Accept header
 *
 * Cost: $5/mo for 10M requests (vs Brightdata $8-12/GB residential).
 * Limitation: Datacenter IPs, not residential. Fine for WHOIS, hotel sites, gov APIs.
 */

const USER_AGENTS = [
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
];

function randomUA() {
  return USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    // Health check
    if (url.pathname === '/health') {
      return new Response(JSON.stringify({
        status: 'ok',
        colo: request.cf?.colo || 'unknown',
      }), { headers: { 'Content-Type': 'application/json' } });
    }

    // Extract target URL
    const targetUrl = url.searchParams.get('url');
    if (!targetUrl) {
      return new Response('Missing ?url= parameter', { status: 400 });
    }

    // Auth check
    const authKey = request.headers.get('X-Auth-Key');
    if (env.AUTH_SECRET && authKey !== env.AUTH_SECRET) {
      return new Response('Unauthorized', { status: 401 });
    }

    // Check KV cache if available
    const cacheKey = `page:${targetUrl}`;
    if (env.SCRAPE_CACHE) {
      const cached = await env.SCRAPE_CACHE.get(cacheKey);
      if (cached) {
        // Cache stores JSON envelope with body + contentType
        try {
          const envelope = JSON.parse(cached);
          return new Response(envelope.body, {
            headers: {
              'Content-Type': envelope.contentType || 'text/html; charset=utf-8',
              'X-Cache': 'HIT',
              'X-Worker-Colo': request.cf?.colo || 'unknown',
            },
          });
        } catch {
          // Legacy plain-text cache entry
          return new Response(cached, {
            headers: {
              'Content-Type': 'text/html; charset=utf-8',
              'X-Cache': 'HIT',
              'X-Worker-Colo': request.cf?.colo || 'unknown',
            },
          });
        }
      }
    }

    // Build outbound request headers
    const headers = new Headers();
    headers.set('User-Agent', randomUA());

    // Use forwarded Accept header if provided, otherwise default to HTML
    const forwardAccept = request.headers.get('X-Forward-Accept');
    if (forwardAccept) {
      headers.set('Accept', forwardAccept);
    } else {
      headers.set('Accept', 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8');
    }

    headers.set('Accept-Language', 'en-US,en;q=0.5');
    headers.set('Accept-Encoding', 'gzip');

    // Forward custom cookies if provided
    const cookies = request.headers.get('X-Forward-Cookies');
    if (cookies) {
      headers.set('Cookie', cookies);
    }

    try {
      const response = await fetch(targetUrl, {
        method: 'GET',
        headers,
        redirect: 'follow',
      });

      const body = await response.text();
      const contentType = response.headers.get('Content-Type') || 'text/html';

      // Cache successful responses in KV (1 hour TTL)
      if (response.ok && env.SCRAPE_CACHE && body.length > 100) {
        const envelope = JSON.stringify({ body, contentType });
        await env.SCRAPE_CACHE.put(cacheKey, envelope, { expirationTtl: 3600 });
      }

      return new Response(body, {
        status: response.status,
        headers: {
          'Content-Type': contentType,
          'X-Cache': 'MISS',
          'X-Worker-Colo': request.cf?.colo || 'unknown',
          'X-Target-Status': String(response.status),
        },
      });
    } catch (err) {
      return new Response(JSON.stringify({ error: err.message }), {
        status: 502,
        headers: { 'Content-Type': 'application/json' },
      });
    }
  },
};
