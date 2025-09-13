/**
 * worker-checker (Support Worker)
 * - POST /check-batch -> { batch: [ { ip, port, country?, isp? }, ... ] }
 * Auth: Authorization: Bearer <SLAVE_TOKEN>
 * Writes to PROXY_CACHE & GEO_CACHE if available; otherwise posts back to MASTER_CALLBACK with MASTER_TOKEN.
 */

import { connect } from 'cloudflare:sockets';

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'POST,OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type,Authorization',
};

function jsonResponse(obj, status=200) { return new Response(JSON.stringify(obj, null,2), { status, headers: { 'Content-Type':'application/json', ...CORS }}); }
function textResponse(t,s=200){ return new Response(t,{status:s, headers:CORS}); }

export default {
  async fetch(request, env) {
    if (request.method === 'OPTIONS') return new Response(null, { headers: CORS });
    const url = new URL(request.url);
    const path = url.pathname;

    if (request.method === 'POST' && path === '/check-batch') {
      const auth = request.headers.get('authorization')||'';
      if (!env.SLAVE_TOKEN || auth !== `Bearer ${env.SLAVE_TOKEN}`) return textResponse('Unauthorized',401);

      let body;
      try { body = await request.json(); } catch(e){ return textResponse('Invalid JSON',400); }
      if (!body || !Array.isArray(body.batch)) return textResponse('Missing batch',400);

      const batch = body.batch;
      const timeoutMs = parseInt(env.HEALTH_CHECK_TIMEOUT || '5000', 10);

      const settled = await Promise.allSettled(batch.map(item => processOne(item, env, timeoutMs)));
      const results = settled.map(s => s.status==='fulfilled' ? s.value : { error: String(s.reason) });

      // If PROXY_CACHE available, write; else post back to master
      if (env.PROXY_CACHE) {
        for (const rec of results) {
          if (!rec || !rec.proxy) continue;
          try {
            const prevRaw = await env.PROXY_CACHE.get(rec.proxy);
            await env.PROXY_CACHE.put(rec.proxy, JSON.stringify(rec));
            try { await updateSummaryIncremental(env, prevRaw ? JSON.parse(prevRaw) : null, rec); } catch(e){}
          } catch(e){ console.warn('KV put failed', e); }
        }
        return jsonResponse({ ok:true, stored: results.length, results });
      } else {
        if (!env.MASTER_CALLBACK || !env.MASTER_TOKEN) return textResponse('No MASTER_CALLBACK configured',500);
        try {
          const resp = await fetch(env.MASTER_CALLBACK, {
            method: 'POST',
            headers: { 'Content-Type':'application/json', 'Authorization': `Bearer ${env.MASTER_TOKEN}` },
            body: JSON.stringify({ results })
          });
          if (!resp.ok) return textResponse('Callback failed: '+ await resp.text(), 502);
          return jsonResponse({ ok:true, posted: results.length });
        } catch(e) {
          return textResponse('Callback error: '+String(e), 500);
        }
      }
    }

    return textResponse('Support worker ready');
  }
};

/* process one proxy: geoip per ip:port then tcp connect */
async function processOne(item, env, timeoutMs) {
  let ip, port, meta={};
  if (typeof item === 'string') {
    [ip, port] = item.split(':').map(s=>s.trim());
  } else {
    ip = item.ip; port = item.port; meta.country = item.country; meta.isp = item.isp;
  }
  const key = `${ip}:${port}`;
  let geo = null;
  try {
    if (env.GEO_CACHE) {
      const cached = await env.GEO_CACHE.get(key) || await env.GEO_CACHE.get(ip);
      if (cached) geo = JSON.parse(cached);
    }
  } catch(e){ geo=null; }

  if (!geo) {
    try {
      const r = await fetch(`http://ip-api.com/json/${ip}?fields=status,country,countryCode,isp,org,as,city`, { cf: { cacheEverything: false } });
      if (r.ok) {
        const j = await r.json();
        if (j && j.status === 'success') {
          geo = { country: j.countryCode || (j.country? j.country.slice(0,2):null), isp: j.isp || j.org || null, asn: j.as || null, city: j.city || null };
          try { if (env.GEO_CACHE) await env.GEO_CACHE.put(key, JSON.stringify(geo), { expirationTtl: 86400 }); } catch(e){}
          try { if (env.GEO_CACHE) await env.GEO_CACHE.put(ip, JSON.stringify(geo), { expirationTtl: 86400 }); } catch(e){}
        }
      }
    } catch(e){}
  }

  let status='dead', latency=null;
  try {
    const start = Date.now();
    const s = await connect({ hostname: ip, port: parseInt(port,10) });
    try { s.close(); } catch(e) {}
    latency = Date.now() - start;
    status = 'alive';
  } catch(e) {
    status = 'dead';
  }

  return { proxy: key, status, latency, country: geo && geo.country ? geo.country.toUpperCase() : (meta.country||null), isp: geo && geo.isp ? geo.isp : (meta.isp||null), last_checked: Date.now() };
}

/* best-effort summary update */
async function updateSummaryIncremental(env, prev, curr) {
  try {
    let raw = null;
    try { raw = await env.PROXY_CACHE.get('_HEALTH_SUMMARY'); } catch(e){ raw=null; }
    let summary = raw ? JSON.parse(raw) : { total:0, alive:0, dead:0, countries:{} };
    const remove = (o) => {
      if (!o || !o.country) return;
      const cc = o.country.toUpperCase();
      summary.countries[cc] = summary.countries[cc] || { alive:0, dead:0 };
      if (o.status==='alive') summary.countries[cc].alive = Math.max(0,(summary.countries[cc].alive||0)-1); else summary.countries[cc].dead = Math.max(0,(summary.countries[cc].dead||0)-1);
      if (o.status==='alive') summary.alive = Math.max(0,(summary.alive||0)-1); else summary.dead = Math.max(0,(summary.dead||0)-1);
    };
    const add = (o) => {
      if (!o || !o.country) return;
      const cc = o.country.toUpperCase();
      summary.countries[cc] = summary.countries[cc] || { alive:0, dead:0 };
      if (o.status==='alive') summary.countries[cc].alive = (summary.countries[cc].alive||0)+1; else summary.countries[cc].dead = (summary.countries[cc].dead||0)+1;
      if (o.status==='alive') summary.alive = (summary.alive||0)+1; else summary.dead = (summary.dead||0)+1;
    };
    if (prev) remove(prev);
    add(curr);
    summary.total = summary.total || 0;
    await env.PROXY_CACHE.put('_HEALTH_SUMMARY', JSON.stringify(summary));
  } catch(e) { console.warn('summary update fail', e); }
}
