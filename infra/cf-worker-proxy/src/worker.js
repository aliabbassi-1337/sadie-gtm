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
 * - POST /batch — fetch up to 500 URLs in parallel from the edge (huge speedup for AU clients)
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

/**
 * Fetch a single URL with appropriate headers.
 * Returns { url, status, body, binary, error? }
 */
async function fetchOne(req) {
  const { url, range, accept } = req;
  try {
    const headers = new Headers();
    headers.set('User-Agent', randomUA());
    headers.set('Accept', accept || 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8');
    headers.set('Accept-Language', 'en-US,en;q=0.5');
    headers.set('Accept-Encoding', 'gzip');
    if (range) {
      headers.set('Range', range);
    }

    const response = await fetch(url, {
      method: 'GET',
      headers,
      redirect: 'follow',
    });

    if (range) {
      // Binary WARC data — base64 encode
      const buf = await response.arrayBuffer();
      const bytes = new Uint8Array(buf);
      let binary = '';
      // Process in chunks to avoid stack overflow on large arrays
      const chunkSize = 8192;
      for (let i = 0; i < bytes.length; i += chunkSize) {
        binary += String.fromCharCode(...bytes.subarray(i, i + chunkSize));
      }
      return {
        url,
        status: response.status,
        body: btoa(binary),
        binary: true,
      };
    } else {
      const body = await response.text();
      return {
        url,
        status: response.status,
        body,
        binary: false,
      };
    }
  } catch (err) {
    return {
      url,
      status: 0,
      body: '',
      binary: false,
      error: err.message,
    };
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const colo = request.cf?.colo || 'unknown';

    // Health check
    if (url.pathname === '/health') {
      return new Response(JSON.stringify({
        status: 'ok',
        colo,
      }), { headers: { 'Content-Type': 'application/json' } });
    }

    // Auth check — support both X-Auth-Key and X-Proxy-Key headers
    const authKey = request.headers.get('X-Auth-Key') || request.headers.get('X-Proxy-Key');
    if (env.AUTH_SECRET && authKey !== env.AUTH_SECRET) {
      return new Response('Unauthorized', { status: 401 });
    }

    // ── POST /batch — fetch multiple URLs in parallel from the edge ──────
    if (url.pathname === '/batch' && request.method === 'POST') {
      const payload = await request.json();
      const requests = payload.requests || [];  // [{url, range?, accept?}, ...]

      if (requests.length === 0) {
        return new Response(JSON.stringify({ results: [], colo }), {
          headers: { 'Content-Type': 'application/json' },
        });
      }

      // Fetch all in parallel
      const results = await Promise.all(requests.map(r => fetchOne(r)));

      return new Response(JSON.stringify({ results, colo }), {
        headers: {
          'Content-Type': 'application/json',
          'X-Worker-Colo': colo,
        },
      });
    }

    // ── Single URL proxy (legacy GET endpoint) ───────────────────────────

    // Extract target URL — support both ?url= param and X-Target-URL header
    const targetUrl = url.searchParams.get('url') || request.headers.get('X-Target-URL');
    if (!targetUrl) {
      return new Response('Missing ?url= parameter or X-Target-URL header', { status: 400 });
    }

    // Check KV cache if available
    const cacheKey = `page:${targetUrl}`;
    if (env.SCRAPE_CACHE) {
      const cached = await env.SCRAPE_CACHE.get(cacheKey);
      if (cached) {
        try {
          const envelope = JSON.parse(cached);
          return new Response(envelope.body, {
            headers: {
              'Content-Type': envelope.contentType || 'text/html; charset=utf-8',
              'X-Cache': 'HIT',
              'X-Worker-Colo': colo,
            },
          });
        } catch {
          return new Response(cached, {
            headers: {
              'Content-Type': 'text/html; charset=utf-8',
              'X-Cache': 'HIT',
              'X-Worker-Colo': colo,
            },
          });
        }
      }
    }

    // Build outbound request headers
    const headers = new Headers();
    headers.set('User-Agent', randomUA());

    const forwardAccept = request.headers.get('X-Forward-Accept');
    if (forwardAccept) {
      headers.set('Accept', forwardAccept);
    } else {
      headers.set('Accept', 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8');
    }

    headers.set('Accept-Language', 'en-US,en;q=0.5');
    headers.set('Accept-Encoding', 'gzip');

    const cookies = request.headers.get('X-Forward-Cookies');
    if (cookies) {
      headers.set('Cookie', cookies);
    }

    const rangeHeader = request.headers.get('Range');
    if (rangeHeader) {
      headers.set('Range', rangeHeader);
    }

    try {
      const response = await fetch(targetUrl, {
        method: 'GET',
        headers,
        redirect: 'follow',
      });

      if (rangeHeader) {
        const body = await response.arrayBuffer();
        return new Response(body, {
          status: response.status,
          headers: {
            'Content-Type': response.headers.get('Content-Type') || 'application/octet-stream',
            'X-Cache': 'MISS',
            'X-Worker-Colo': colo,
            'X-Target-Status': String(response.status),
          },
        });
      }

      const body = await response.text();
      const contentType = response.headers.get('Content-Type') || 'text/html';

      if (response.ok && env.SCRAPE_CACHE && body.length > 100) {
        const envelope = JSON.stringify({ body, contentType });
        await env.SCRAPE_CACHE.put(cacheKey, envelope, { expirationTtl: 3600 });
      }

      return new Response(body, {
        status: response.status,
        headers: {
          'Content-Type': contentType,
          'X-Cache': 'MISS',
          'X-Worker-Colo': colo,
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
