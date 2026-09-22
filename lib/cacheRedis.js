// lib/cacheRedis.js
// Backend de cache pentru rulare pe Vercel (sau oriunde nu exista disc
// persistent) — Upstash Redis prin API REST, cu acelasi rol ca lib/cache.js
// (SQLite): cautari (7 zile) si continut de subtitrari descarcate (90 zile).
//
// Spre deosebire de SQLite, Redis expira intrarile singur prin TTL nativ —
// nu mai e nevoie de cleanup()/vacuum periodic.

const SEARCH_TTL_SECONDS   = 7  * 24 * 60 * 60; // 7 zile
const SUBTITLE_TTL_SECONDS = 90 * 24 * 60 * 60; // 90 zile

const MAX_ERR_LEN = 200;

let redis = null;
let available = false;

// Clientul Upstash ataseaza comanda completa la mesajul de eroare. Pentru
// setSubtitle asta inseamna tot textul subtitrarii — zeci de KB revarsati in
// log la fiecare esec, care ingroapa toate celelalte linii. Taiem bucata aia
// si punem oricum o limita de lungime, ca sa nu depindem de un singur format
// de mesaj.
function briefError(err) {
    const full = (err && err.message) || String(err);
    const cut  = full.indexOf(', command was:');
    const msg  = cut === -1 ? full : full.slice(0, cut);
    return msg.length > MAX_ERR_LEN ? `${msg.slice(0, MAX_ERR_LEN)}…` : msg;
}

// Credentiale gresite vs. retea picata: prima e o problema de configurare care
// nu se repara singura si merita oprit cache-ul, a doua poate trece de la sine.
function isAuthError(err) {
    const msg = ((err && err.message) || '').toUpperCase();
    return msg.includes('WRONGPASS') || msg.includes('UNAUTHORIZED') || msg.includes('NOPERM');
}

function init() {
    try {
        const { Redis } = require('@upstash/redis');
        const url = process.env.UPSTASH_REDIS_REST_URL;
        const token = process.env.UPSTASH_REDIS_REST_TOKEN;
        if (!url || !token) {
            console.warn('[CACHE-REDIS] UPSTASH_REDIS_REST_URL/TOKEN lipsesc — cache indisponibil.');
            return;
        }
        redis = new Redis({ url, token });
        available = true;
        console.log('[CACHE-REDIS] Initializat (Upstash Redis).');
    } catch (err) {
        console.warn(`[CACHE-REDIS] Indisponibil (${briefError(err)}).`);
        available = false;
    }
}

// `new Redis(...)` nu contacteaza serverul, deci un token gresit trecea
// neobservat la pornire si se vedea abia ca eroare la FIECARE comanda, la
// nesfarsit, fara ca nimeni sa spuna care e cauza. Un singur ping la pornire
// raspunde din prima.
async function verify() {
    if (!available) return;
    try {
        await redis.ping();
        console.log('[CACHE-REDIS] Ping OK — credentiale valide, cache activ.');
    } catch (err) {
        if (isAuthError(err)) {
            available = false;
            console.error(`[CACHE-REDIS] CREDENTIALE INVALIDE (${briefError(err)}).`);
            console.error('[CACHE-REDIS] Cache DEZACTIVAT — addon-ul merge normal, dar fara cache.');
            console.error('[CACHE-REDIS] Ia UPSTASH_REDIS_REST_TOKEN din tab-ul REST API al bazei —');
            console.error('[CACHE-REDIS] NU parola din redis://... — si verifica sa fie din aceeasi baza ca URL-ul.');
        } else {
            // Nu dezactivam: un hiccup de retea la pornire nu inseamna ca
            // urmatoarele comenzi pica si ele.
            console.warn(`[CACHE-REDIS] Ping esuat (${briefError(err)}) — cache ramane pornit, poate fi trecator.`);
        }
    }
}

// === CAUTARI ===

async function getSearch(key) {
    if (!available) return null;
    try {
        return await redis.get(`search:${key}`);
    } catch (err) {
        console.error('[CACHE-REDIS] Eroare getSearch:', briefError(err));
        return null;
    }
}

async function setSearch(key, data) {
    if (!available) return;
    try {
        await redis.set(`search:${key}`, data, { ex: SEARCH_TTL_SECONDS });
    } catch (err) {
        console.error('[CACHE-REDIS] Eroare setSearch:', briefError(err));
    }
}

// === SUBTITRARI DESCARCATE ===

async function getSubtitle(key) {
    if (!available) return null;
    try {
        return await redis.get(`sub:${key}`);
    } catch (err) {
        console.error('[CACHE-REDIS] Eroare getSubtitle:', briefError(err));
        return null;
    }
}

async function setSubtitle(key, content) {
    if (!available) return;
    try {
        await redis.set(`sub:${key}`, content, { ex: SUBTITLE_TTL_SECONDS });
    } catch (err) {
        console.error('[CACHE-REDIS] Eroare setSubtitle:', briefError(err));
    }
}

// === ADMINISTRARE ===

async function stats() {
    if (!available) return { available: false };
    try {
        const dbsize = await redis.dbsize();
        return { available: true, backend: 'redis', keys: dbsize };
    } catch {
        return { available: true, backend: 'redis', error: true };
    }
}

async function clearAll() {
    if (!available) return 0;
    try {
        const dbsize = await redis.dbsize();
        await redis.flushdb();
        return dbsize;
    } catch (err) {
        console.error('[CACHE-REDIS] Eroare clearAll:', briefError(err));
        return 0;
    }
}

// TTL-ul Redis expira singur intrarile — nimic de facut aici. Pastram functia
// doar ca interfata sa ramana identica cu lib/cache.js (SQLite).
async function cleanup() {}

init();
verify();

module.exports = {
    getSearch, setSearch,
    getSubtitle, setSubtitle,
    stats, clearAll, cleanup,
    isAvailable: () => available
};
