require('dotenv').config();

const fs = require('fs');
const express = require('express');
const cors = require('cors');
const { getRouter } = require('stremio-addon-sdk');
const addonInterface = require('./addon');
const axios = require('axios');
const AdmZip = require('adm-zip');
const iconv = require('iconv-lite');
const jschardet = require('jschardet');
const { clearSearchCache } = require('./lib/regielive');
const cacheDb = require('./lib/cacheStore');
const BoundedCache = require('./lib/boundedCache');

// node-unrar-js isi incarca singur fisierul unrar.wasm de pe disc, printr-un
// mecanism intern (Emscripten) care construieste calea dinamic — nedetectabil
// de instrumentul de trace al Vercel-ului la build, deci fisierul .wasm nu
// ajunge in bundle-ul functiei serverless (ENOENT la runtime doar pe Vercel,
// niciodata local/Pi unde tot node_modules e pe disc). Citim noi insine bytes-ii
// cu un require.resolve() STATIC — pe care trace-ul chiar il recunoaste — si-i
// dam explicit librariei, ocolind complet mecanismul ei intern de incarcare.
// Functioneaza identic pe orice platforma, deci nu schimba nimic local/Pi.
let unrarWasmBinary = null;
try {
    const wasmPath = require.resolve('node-unrar-js/dist/js/unrar.wasm');
    const wasmBuffer = fs.readFileSync(wasmPath);
    unrarWasmBinary = wasmBuffer.buffer.slice(wasmBuffer.byteOffset, wasmBuffer.byteOffset + wasmBuffer.byteLength);
} catch (err) {
    console.warn(`[RAR] Nu am putut preincarca unrar.wasm (${err.message}) — folosesc mecanismul implicit al librariei.`);
}

// Pe Vercel nu exista un proces persistent intre cereri — coada globala de
// 1.5s intre descarcari (gandita pt. un server single-user, local/Pi) ar doar
// incetini inutil accesul concurent al mai multor utilizatori. Acelasi semnal
// decide si daca merita pornit cleanup-ul periodic mai jos — pe Vercel fiecare
// invocare e scurta si separata, deci un setInterval n-ar mai apuca sa faca
// nimic util.
//
// ATENTIE: coada de mai jos protejeaza doar rularea locala/Pi (un singur
// proces). Pe Vercel, descarcarile RegieLive au propria lor protectie
// distribuita separata (vezi regieliveDownloadLimiter mai jos) — spre
// deosebire de ce spunea comentariul vechi aici, limitatorul distribuit din
// lib/regielive.js acopera DOAR cautarea (api.regielive.ro/bazarr/search.php),
// nu si descarcarea efectiva a arhivei (subtitrari.regielive.ro/descarca-*),
// care pana acum nu avea absolut nicio protectie pe Vercel. Confirmat pe
// productie (Evil Dead Burn, 19 sept.): 6 din 12 descarcari RegieLive au picat
// cu "RATE LIMIT atins" cand au fost cerute rapid, una dupa alta.
//
// Detectam Vercel explicit prin variabila lui de sistem (setata automat, "1"),
// nu prin prezenta Redis-ului — pe Render vrem Redis activ pt. cache, dar tot
// procesul persistent de mai jos (coada locala), nu ramura gandita pt. Vercel.
const IS_SERVERLESS = !!process.env.VERCEL;

// --- Limitator distribuit pentru DESCARCAREA de pe RegieLive ---
// Nu avem o cifra exacta de la RegieLive pt. descarcari (spre deosebire de
// cautare, unde ni s-a dat explicit "8/min, burst 2/sec") — admin-ul lor a
// descris-o doar ca "limita dinamica, dupa reputatia IP-ului" plus un prag de
// CAPTCHA care blocheaza descarcarile ~24h daca il atingem. Fara o cifra
// exacta, alegem acelasi ritm folosit deja cu succes in productie de addon-ul
// sora (stremio-regielive, pe Render): maxim 1 descarcare la fiecare 1.5s,
// aplicat GLOBAL (nu per-utilizator) — pe Vercel, spre deosebire de un singur
// proces Render, mai multe invocari serverless concurente n-ar respecta
// deloc acest ritm fara o coordonare distribuita (Redis), de-asta un simplu
// interval in memorie (ca la coada locala de mai sus) nu ar functiona aici.
const REGIELIVE_DOWNLOAD_MIN_INTERVAL = '1500 ms';
const MAX_DOWNLOAD_WAIT_MS = 4000; // acelasi plafon ca la asteptarea de cautare din regielive.js
let regieliveDownloadLimiter = null;

if (IS_SERVERLESS) {
    try {
        const { Redis } = require('@upstash/redis');
        const { Ratelimit } = require('@upstash/ratelimit');
        const redis = new Redis({
            url: process.env.UPSTASH_REDIS_REST_URL,
            token: process.env.UPSTASH_REDIS_REST_TOKEN
        });
        regieliveDownloadLimiter = new Ratelimit({
            redis,
            limiter: Ratelimit.slidingWindow(1, REGIELIVE_DOWNLOAD_MIN_INTERVAL),
            prefix: 'rl:regielive:download'
        });
        console.log('[REGIELIVE] Limitator distribuit pentru descarcare activ.');
    } catch (err) {
        console.warn(`[REGIELIVE] Limitator de descarcare indisponibil (${err.message}) — descarcarile raman neprotejate.`);
    }
}

// Asteapta (marginit la MAX_DOWNLOAD_WAIT_MS) pana cand e liber un slot de
// descarcare RegieLive. Un singur apel Redis, nu polling — folosim direct
// "reset"-ul intors de Ratelimit ca sa stim exact cat sa asteptam, in loc sa
// verificam repetat. Daca limitatorul nu e disponibil (local/Pi fara Redis,
// sau o eroare de retea), continuam neconditionat — mai bine incercam si
// riscam un 429 (prins oricum de fetchWithRetry429 mai jos) decat sa blocam
// userul la nesfarsit pe o protectie care oricum nu functioneaza.
async function waitForRegieliveDownloadSlot() {
    if (!regieliveDownloadLimiter) return;
    try {
        const result = await regieliveDownloadLimiter.limit('downloads');
        if (result.success) return;
        const waitMs = Math.min(Math.max(result.reset - Date.now(), 0), MAX_DOWNLOAD_WAIT_MS);
        if (waitMs > 0) {
            console.log(`[REGIELIVE] Slot de descarcare ocupat, astept ${waitMs}ms.`);
            await new Promise(r => setTimeout(r, waitMs));
        }
    } catch (err) {
        console.warn(`[REGIELIVE] Eroare la limitatorul de descarcare (${err.message}), continui oricum.`);
    }
}

// Reincearca automat o descarcare care a picat cu 429 (rate-limit trecator) —
// confirmat pe productie ca aceste esecuri sunt adesea trecatoare (functioneaza
// la o reincercare manuala, la cateva secunde distanta). Doar 429 se reincearca;
// orice alta eroare (retea, 404, etc.) e aruncata imediat, neschimbata.
const DOWNLOAD_RETRY_DELAYS_MS = [1500, 3000];

async function fetchWithRetry429(axiosConfig, sourceLabel) {
    for (let attempt = 0; ; attempt++) {
        try {
            return await axios(axiosConfig);
        } catch (err) {
            const is429 = err.response?.status === 429;
            if (!is429 || attempt >= DOWNLOAD_RETRY_DELAYS_MS.length) throw err;
            const delay = DOWNLOAD_RETRY_DELAYS_MS[attempt];
            console.warn(`[${sourceLabel}] 429 la descarcare, reincerc peste ${delay}ms (incercarea ${attempt + 2}/${DOWNLOAD_RETRY_DELAYS_MS.length + 1}).`);
            await new Promise(r => setTimeout(r, delay));
        }
    }
}

const app = express();
app.use(cors());
app.use(express.static('public'));

function fixRomanianDiacritics(text) {
    return text
        .replace(/\u015F/g, '\u0219')
        .replace(/\u015E/g, '\u0218')
        .replace(/\u0163/g, '\u021B')
        .replace(/\u0162/g, '\u021A')
        .replace(/\u00E3/g, 'ă')
        .replace(/\u00E2/g, 'â')
        .replace(/\u00EE/g, 'î');
}

function srtToVtt(srtText) {
    let text = String(srtText).replace(/\r+/g, '').trim();
    // Unele .srt convertite dintr-un .ass/.ssa original pastreaza codurile de
    // formatare ASS (ex. "{\an8}", "{\pos(320,240)}") — WebVTT nu le recunoaste
    // deloc (doar "<" introduce marcaj in text), deci apar ca text vizibil,
    // literal, peste replica reala. Le eliminam complet inainte de conversie.
    text = text.replace(/\{\\[^}]*\}/g, '');
    // Eliminam linia de index SRT INCLUSIV linia noua de dupa — nu doar cifrele.
    // Varianta veche (\d+\s*$ fara sa consume \n) lasa in urma un rand gol
    // suplimentar intre fiecare cue (dublu \n\n\n in loc de \n\n unic), rezultand
    // cue-uri WebVTT malformate. Player-ele permisive (mpv/ExoPlayer) ignora
    // asta, dar playerul nativ iOS (AVPlayer) respinge fisierul ca invalid —
    // confirmat direct: exact acelasi bug era prezent si in addonul vechi.
    text = text.replace(/^\d+\n/gm, '');
    // Ora poate avea 1 SAU 2 cifre in SRT-urile scrise manual (ex. "0:45:47,000"
    // in loc de "00:45:47,000") — regex-ul vechi cerea strict 2 cifre la ora, deci
    // rata exact acest caz, lasand in urma o virgula si o ora pe o cifra, ambele
    // nevalide in WebVTT. Confirmat direct: AVPlayer (iOS) respinge tot fisierul
    // la primul asemenea timestamp gresit ("invalidTime"), desi playere permisive
    // (mpv/ExoPlayer) trec peste fara sa se planga. Acceptam 1-2 cifre la ora si
    // completam mereu la 2, indiferent de cate cifre avea originalul.
    text = text.replace(/(\d{1,2}):(\d{2}):(\d{2}),(\d{3})/g, (_, h, m, s, ms) => `${h.padStart(2, '0')}:${m}:${s}.${ms}`);
    // Unele fisiere sursa (confirmat pe unul real, descarcat de la RegieLive
    // pentru "Obsession") sar linia goala dintre ultimul cue si linia de index
    // urmatoare — de obicei chiar inaintea cue-ului promotional "Subtitrare
    // descarcata de pe www.RegieLive.ro" adaugat de site. Rezultatul, dupa ce
    // stripam linia de index, e o linie de timp lipita direct de textul
    // cue-ului anterior, fara separator — WebVTT malformat. AVPlayer (iOS)
    // respinge tot fisierul, playerele permisive nu se sesizeaza. Fortam o
    // linie goala inaintea ORICAREI linii de timp care nu are deja una,
    // indiferent de cauza — idempotent pe fisierele deja corecte.
    text = text.replace(/([^\n])\n(\d{2}:\d{2}:\d{2}\.\d{3} --> \d{2}:\d{2}:\d{2}\.\d{3})/g, '$1\n\n$2');
    return 'WEBVTT\n\n' + text.trim() + '\n';
}

function parseAssTime(assTime) {
    // ASS foloseste H:MM:SS.cc (ora pe 1 cifra, centizecimi pe 2 cifre) — SRT
    // vrea HH:MM:SS,mmm. Adaugam un 0 la capat ca sa transformam centizecimi in
    // milizecimi (cc -> ccc0), fara sa pierdem precizie.
    const m = String(assTime).trim().match(/^(\d+):(\d{2}):(\d{2})\.(\d{2})$/);
    if (!m) return null;
    const [, h, mi, s, cs] = m;
    return `${h.padStart(2, '0')}:${mi}:${s},${cs}0`;
}

// Convertor ASS/SSA -> SRT. Confirmat pe productie (One Piece S02E06, sursa
// titrari.ro): unele link-uri de descarcare NU sunt arhive, ci fisierul .ass
// original trimis direct, cu Content-Type application/octet-stream — codul
// de mai jos (detectArchiveType + fallback SRT) nu recunostea deloc acest
// caz si arunca UNKNOWN_FORMAT, deci userul primea "subtitle failed to load"
// in Stremio pentru acel rezultat. Extragem liniile "Dialogue:" din sectiunea
// [Events], respectand ordinea reala a campurilor din linia ei "Format:" (nu
// presupunem o ordine fixa), si recompunem un SRT valid — restul pipeline-ului
// (srtToVtt) stie deja sa converteasca SRT -> WebVTT si sa elimine codurile
// de formatare ASS ramase in text (ex. "{\an8}").
function assToSrt(assText) {
    const lines = String(assText).replace(/\r\n/g, '\n').split('\n');

    let inEvents = false;
    let startIdx = -1, endIdx = -1, textIdx = -1;
    const cues = [];

    for (const line of lines) {
        const trimmed = line.trim();
        if (/^\[.+\]$/.test(trimmed)) {
            inEvents = /^\[Events\]$/i.test(trimmed);
            continue;
        }
        if (!inEvents) continue;

        if (/^Format:/i.test(trimmed)) {
            const fields = trimmed.slice(trimmed.indexOf(':') + 1).split(',').map(f => f.trim().toLowerCase());
            startIdx = fields.indexOf('start');
            endIdx = fields.indexOf('end');
            textIdx = fields.indexOf('text');
            continue;
        }

        if (/^Dialogue:/i.test(trimmed) && textIdx >= 0) {
            const parts = trimmed.slice(trimmed.indexOf(':') + 1).split(',');
            if (parts.length <= textIdx) continue;

            // Textul e mereu ultimul camp din spec-ul ASS, dar poate contine el
            // insusi virgule — recompunem tot ce a ramas de la indexul lui incolo.
            const start = parseAssTime(parts[startIdx]);
            const end = parseAssTime(parts[endIdx]);
            if (!start || !end) continue;

            const cleanText = parts.slice(textIdx).join(',')
                .replace(/\\N|\\n/g, '\n')
                .replace(/\\h/g, ' ')
                .trim();
            if (!cleanText) continue;

            cues.push({ start, end, text: cleanText });
        }
    }

    // AVPlayer (iOS) e strict si la ordinea cue-urilor — unele .ass au liniile
    // de dialog nesortate cronologic (actori/straturi diferite intercalate).
    cues.sort((a, b) => a.start.localeCompare(b.start));

    return cues.map((c, i) => `${i + 1}\n${c.start} --> ${c.end}\n${c.text}\n`).join('\n');
}

// Detectie + convertor MicroDVD (.sub cu timestamp-uri pe CADRE, nu pe timp:
// "{508}{583}text", "|" in loc de linie noua) -> SRT. Confirmat pe productie
// (Titrari id=8614, Ghosts of Mars): descarcarea directa (fara arhiva) e chiar
// acest format, needetectat de niciun cod existent (nu are "-->" ca SRT, nici
// "[Script Info]"/"Dialogue:" ca ASS) — arunca UNKNOWN_FORMAT desi traducerea
// e completa si buna. Verificat direct: fisierul n-are un header de fps (nu
// exista o linie gen "{1}{1}25"), dar cadrul maxim (133620) imparte la 23.976
// fps la ~93 minute — foarte aproape de durata reala a filmului (98 min); la
// 25fps ar da ~89min si la 29.97fps ~74min, ambele clar gresite. 23.976fps e
// standardul de facto pt. rip-urile de film din era asta (comunitatea asta de
// subtitrari romanesti, inceput de 2000), asa ca il folosim ca implicit.
const MICRODVD_DEFAULT_FPS = 23.976;

function isMicroDvdText(text) {
    const firstLine = String(text).replace(/^﻿/, '').trimStart().split(/\r?\n/, 1)[0] || '';
    return /^\{\d+\}\{\d+\}/.test(firstLine);
}

function microDvdToSrt(microDvdText, fps = MICRODVD_DEFAULT_FPS) {
    const lines = String(microDvdText).replace(/\r\n/g, '\n').split('\n');
    const cues = [];

    const frameToSrtTime = (frame) => {
        let ms = Math.round((frame / fps) * 1000);
        const h = Math.floor(ms / 3600000); ms -= h * 3600000;
        const m = Math.floor(ms / 60000); ms -= m * 60000;
        const s = Math.floor(ms / 1000); ms -= s * 1000;
        return `${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}:${String(s).padStart(2, '0')},${String(ms).padStart(3, '0')}`;
    };

    for (const line of lines) {
        const m = line.match(/^\{(\d+)\}\{(\d+)\}(.*)$/);
        if (!m) continue;
        const [, startFrame, endFrame, rawText] = m;
        // Codurile de stil MicroDVD ("{y:i}" italic, "{c:$FFFFFF}" culoare, etc.)
        // apar ca bloc separat, distinct de perechea obligatorie {start}{end} de
        // mai sus — le eliminam la fel cum srtToVtt elimina deja codurile ASS
        // ramase intr-un .srt convertit, ca sa nu apara ca text vizibil literal.
        const cleanText = rawText.replace(/\{[a-zA-Z]:[^}]*\}/g, '').replace(/\|/g, '\n').trim();
        if (!cleanText) continue;
        cues.push({ start: frameToSrtTime(parseInt(startFrame, 10)), end: frameToSrtTime(parseInt(endFrame, 10)), text: cleanText });
    }

    cues.sort((a, b) => a.start.localeCompare(b.start));
    return cues.map((c, i) => `${i + 1}\n${c.start} --> ${c.end}\n${c.text}\n`).join('\n');
}

// Cache in memorie — layer rapid peste SQLite. Continutul deja e persistat
// pe disc (90 zile), deci acest L1 nu trebuie sa fie nemarginit — il tinem
// mic si dam evacuare LRU, altfel textul complet al fiecarei subtitrari
// descarcate vreodata ramanea in heap pentru totdeauna.
const memCache = new BoundedCache({ ttlMs: 24 * 60 * 60 * 1000, maxEntries: 200 });
const activeDownloads = new Map();
let globalDownloadQueue = Promise.resolve();

const RL_API_KEY = process.env.RL_API_KEY || 'API-BAZARR-YTZ-SL';
const ADMIN_KEY = process.env.ADMIN_KEY || 'rosubs-admin-2026';
const TITRARI_COOKIE = process.env.TITRARI_COOKIE || '';

app.use(getRouter(addonInterface));

app.get('/admin/clear-cache', async (req, res) => {
    if (req.query.key !== ADMIN_KEY) return res.status(403).send('Cheie invalida.');
    const memCleared = memCache.size;
    memCache.clear();
    activeDownloads.clear();
    const searchesCleared = clearSearchCache();
    const dbCleared = await cacheDb.clearAll();
    res.send(`Cache golit: ${memCleared} memorie + ${searchesCleared} cautari + ${dbCleared} intrari ${cacheDb.backend}.`);
});

app.get('/admin/cache-stats', async (req, res) => {
    if (req.query.key !== ADMIN_KEY) return res.status(403).send('Cheie invalida.');
    const s = await cacheDb.stats();
    res.json({
        memorie: memCache.size,
        [cacheDb.backend]: s
    });
});

function detectArchiveType(buffer) {
    if (buffer.length < 4) return 'unknown';
    if (buffer[0] === 0x50 && buffer[1] === 0x4B) return 'zip';
    if (buffer[0] === 0x52 && buffer[1] === 0x61 && buffer[2] === 0x72 && buffer[3] === 0x21) return 'rar';
    return 'unknown';
}

// Cate un nivel de recursie e suficient pentru cazul real intalnit (pachet
// "serie completa" = un rar/zip exterior ce contine cate un rar/zip per sezon)
// si opreste orice risc de bucla / arhiva-in-arhiva-in-arhiva construita
// malitios. Limita de marime evita sa decomprimam ceva neasteptat de mare
// doar pentru ca "pare" un pachet per-sezon legitim.
const MAX_NESTED_DEPTH = 1;
// Un pachet de subtitrari pt. un singur sezon nu ar trebui sa depaseasca asta.
// Verificam marimea declarata in header INAINTE de decomprimare (filtru rapid)
// SI marimea reala a bufferului dupa decomprimare (header-ul poate fi
// falsificat intr-o arhiva construita malitios — marime mica declarata,
// continut real mult mai mare, clasicul "decompression bomb").
const MAX_NESTED_ARCHIVE_SIZE = 20 * 1024 * 1024; // 20MB

// Un .srt/.sub, oricat de incarcat cu mai multe limbi sau segmente, nu ar trebui
// sa depaseasca asta. Aplicam limita si pe marimea DECLARATA (filtru rapid,
// inainte de decomprimare) si pe cea REALA dupa decomprimare — un header
// falsificat intr-o arhiva construita malitios poate declara o marime mica
// si decomprima la ceva mult mai mare (decompression bomb).
const MAX_SUBTITLE_FILE_SIZE = 10 * 1024 * 1024; // 10MB

// /download accepta un URL controlat de client. Fara verificare, oricine poate
// cere serverului sa descarce si sa parseze orice arhiva de pe orice domeniu
// (risc SSRF + amplificare pt. decompression-bomb pe un domeniu strain). Legam
// fiecare sursa suportata la domeniul ei real (verificat direct in cod, nu presupus).
const ALLOWED_DOWNLOAD_DOMAINS = {
    regielive:     ['regielive.ro'],
    titrari:       ['titrari.ro'],
    subtitrarinoi: ['subtitrari-noi.ro'],
    subsro:        ['subs.ro'],
};
const MAX_DOWNLOAD_SIZE = 100 * 1024 * 1024; // 100MB — generos pt. orice arhiva reala de subtitrari

function isAllowedDownloadHost(hostname, source) {
    const allowedDomains = ALLOWED_DOWNLOAD_DOMAINS[source];
    if (!allowedDomains) return false;
    const h = (hostname || '').toLowerCase();
    return allowedDomains.some(domain => h === domain || h.endsWith(`.${domain}`));
}

const DISC_KEYWORDS = ['remux', 'bluray', 'blu-ray', 'bdrip', 'brrip', 'bd', 'uhd', 'hddvd'];
const WEB_KEYWORDS  = ['web-dl', 'webdl', 'webrip', 'web', 'amzn', 'nf', 'hmax', 'dsnp'];
const HDTV_KEYWORDS = ['hdtv', 'pdtv', 'tvrip'];
const LOW_KEYWORDS  = ['dvdrip', 'dvdscr', 'hdrip', 'cam', 'hdcam', 'hd-ts', 'hdts', 'telesync', 'telecine', 'r5'];

function getFileSourceType(text) {
    const t = (text || '').toLowerCase();
    if (LOW_KEYWORDS.some(s => t.includes(s)))  return 'low';
    if (DISC_KEYWORDS.some(s => t.includes(s))) return 'disc';
    if (HDTV_KEYWORDS.some(s => t.includes(s))) return 'hdtv';
    if (WEB_KEYWORDS.some(s => t.includes(s)))  return 'web';
    return null;
}

// Confirmat pe productie (Ghosts of Mars, arhiva titrari id=96539): unele
// arhive de pe aceste site-uri (in principiu exclusiv romanesti) contin de
// fapt fisiere din mai multe limbi ("z srt23 ro-r ...", "z srt23 uk-hi ...",
// "z srt23 uk ..."), fara nicio diferenta de scor intre ele (aceeasi sursa,
// rezolutie, grup) — tie-break-ul pe marime alegea silentios varianta engleza
// (mai mare, din cauza descrierilor audio pt. hipoacuzici) in locul celei
// romane, aflata chiar alaturi in aceeasi arhiva. Verificam token cu token
// (nu substring, ca sa nu prindem "ro" din interiorul altor cuvinte precum
// numele unui grup de release) numele fiecarei intrari din arhiva.
const RO_LANG_TOKENS = new Set(['ro', 'rom', 'ron', 'romana', 'romina', 'rumana']);
const FOREIGN_LANG_TOKENS = new Set([
    'en', 'eng', 'uk', 'gb', 'us', 'usa',
    'fr', 'fra', 'fre', 'de', 'ger', 'deu', 'es', 'spa', 'it', 'ita',
    'nl', 'dut', 'nld', 'pt', 'por', 'bra', 'ru', 'rus', 'hu', 'hun',
    'bg', 'bul', 'gr', 'gre', 'ell', 'tr', 'tur', 'pl', 'pol',
    'cz', 'cze', 'ces', 'sk', 'slo', 'ar', 'ara', 'zh', 'chi', 'zho',
    'ja', 'jpn', 'ko', 'kor'
]);

function detectArchiveEntryLanguage(entryName) {
    const tokens = entryName.toLowerCase().split(/[^a-z0-9]+/).filter(Boolean);
    if (tokens.some(t => RO_LANG_TOKENS.has(t))) return 'ro';
    if (tokens.some(t => FOREIGN_LANG_TOKENS.has(t))) return 'foreign';
    return null;
}

function scoreArchiveEntry(entryName, videoFilename, knownSeason, knownEpisode) {
    if (!videoFilename && !(knownSeason && knownEpisode)) return 0;

    const entry = entryName.toLowerCase();
    const video = (videoFilename || '').toLowerCase();
    let score = 0;

    const videoSrc = getFileSourceType(video);
    const entrySrc = getFileSourceType(entry);
    if (videoSrc && entrySrc) {
        if (videoSrc === entrySrc) {
            score += 100;
        } else if ((videoSrc === 'disc' && entrySrc === 'web') ||
                   (videoSrc === 'web'  && entrySrc === 'disc')) {
            score -= 80;
        } else {
            score -= 40;
        }
    }

    for (const res of ['2160p', '1080p', '720p', '480p']) {
        if (video.includes(res) && entry.includes(res)) { score += 40; break; }
    }

    const groupMatch = video.match(/-([a-z0-9]{2,20})(?:\.[a-z0-9]{2,4})?$/i);
    if (groupMatch) {
        const group = groupMatch[1].toLowerCase();
        if (group.length >= 3 && entry.includes(group)) score += 80;
    }

    for (const codec of ['x265', 'hevc', 'x264', 'h264', 'av1']) {
        if (video.includes(codec) && entry.includes(codec)) { score += 20; break; }
    }

    // Preferam sezonul/episodul cunoscut din ID-ul Stremio (sigur) in loc de regex
    // pe numele fisierului video — care poate fi un placeholder opac de la o sursa
    // debrid (altfel toti candidatii dintr-o arhiva multi-episod scoreaza 0 si
    // alegerea devine esentialmente aleatorie dupa marime, extragand episod gresit).
    const seMatch = video.match(/s(\d{1,2})e(\d{1,2})/i);
    const seSeason  = knownSeason  ? String(knownSeason)  : (seMatch ? seMatch[1] : null);
    const seEpisode = knownEpisode ? String(knownEpisode) : (seMatch ? seMatch[2] : null);
    if (seSeason && seEpisode && entryMatchesEpisode(entry, seSeason, seEpisode)) {
        score += 120;
    }

    return score;
}

// Recunoaste episodul cerut in numele unei intrari din arhiva, in oricare din
// formatele intalnite real pe siteurile romanesti: "s01e05"/"s1e5", "1x05"/
// "01x05", sau "e05"/"ep05"/"episod(ul) 05" ca marcaj de sine statator. NU
// acceptam un numar simplu, fara niciun context ("05" izolat) — prea ambiguu
// (poate fi rezolutie, an, orice altceva).
function entryMatchesEpisode(entry, season, episode) {
    return new RegExp(`s0?${season}e0?${episode}(?!\\d)`, 'i').test(entry) ||
           new RegExp(`\\b0?${season}x0?${episode}(?!\\d)`, 'i').test(entry) ||
           new RegExp(`\\bep?(?:isod(?:e|ul)?)?[\\s._-]*0?${episode}(?!\\d)`, 'i').test(entry);
}

// Un pachet "serie completa" e adesea o arhiva exterioara ce contine cate o
// arhiva per sezon (ex: "Supernatural.S04.720p.BluRay.x264-Mixed Groups.rar").
// Daca stim sigur sezonul cerut (din ID-ul Stremio) si EXACT una dintre
// arhivele imbricate il mentioneaza, o putem identifica fara ambiguitate —
// altfel (zero sau mai multe potriviri) nu ghicim.
function findSeasonMatchedNestedArchive(nestedNames, knownSeason) {
    const season = String(knownSeason);
    const pattern = new RegExp(`\\bs0?${season}\\b|\\bseason[\\s._-]*0?${season}\\b|\\bsezonul[\\s._-]*0?${season}\\b`, 'i');
    const matches = nestedNames.filter(name => pattern.test(name));
    return matches.length === 1 ? matches[0] : null;
}

function pickBestSubtitleFile(candidates, videoFilename, knownSeason, knownEpisode) {
    if (candidates.length === 0) return null;

    // Excludem complet fisierele identificate CU CERTITUDINE ca fiind in alta
    // limba decat romana — dar doar daca ramane cel putin o alternativa (fie
    // marcata explicit "ro", fie fara niciun marcaj de limba deloc, cazul
    // marii majoritati a arhivelor de pe aceste site-uri). Daca TOATE
    // fisierele par straine (sau niciunul nu e clar), nu ghicim si lasam
    // comportamentul de scor obisnuit sa decida, neschimbat.
    const withLang = candidates.map(c => ({ ...c, _lang: detectArchiveEntryLanguage(c.name) }));
    const nonForeign = withLang.filter(c => c._lang !== 'foreign');
    if (nonForeign.length > 0 && nonForeign.length < candidates.length) {
        const excluded = withLang.filter(c => c._lang === 'foreign').map(c => c.name);
        console.log(`[ARHIVA] Exclud ${excluded.length} fisier(e) dintr-o alta limba: ${excluded.join(', ')}`);
        candidates = nonForeign;
    }

    if (candidates.length === 1) return candidates[0];

    const scored = candidates.map(c => ({
        entry: c,
        matchScore: scoreArchiveEntry(c.name, videoFilename, knownSeason, knownEpisode),
        matchesEpisode: (knownSeason && knownEpisode)
            ? entryMatchesEpisode(c.name.toLowerCase(), String(knownSeason), String(knownEpisode))
            : null,
        size: c.size || 0
    }));

    // Stim exact ce episod cautam, arhiva are mai multe fisiere, dar NICIUNUL nu
    // poate fi identificat pozitiv ca fiind episodul cerut (nicio conventie de
    // denumire recunoscuta) — nu ghicim dupa marime. Aceeasi conventie ca la
    // arhivele imbricate ambigue mai jos (findSeasonMatchedNestedArchive): o
    // alegere gresita, silentioasa, e mai rea decat un esec explicit, logat.
    if (knownSeason && knownEpisode && !scored.some(s => s.matchesEpisode)) {
        const wanted = `S${String(knownSeason).padStart(2, '0')}E${String(knownEpisode).padStart(2, '0')}`;
        console.error(`[ARHIVA] ${candidates.length} fisiere, dar niciunul nu poate fi identificat ca ${wanted} — refuz sa aleg dupa marime: ${candidates.map(c => c.name).join(', ')}`);
        return null;
    }

    scored.sort((a, b) => {
        if (b.matchScore !== a.matchScore) return b.matchScore - a.matchScore;
        return b.size - a.size;
    });

    console.log(`[ARHIVA] ${candidates.length} fisiere, clasament:`);
    scored.slice(0, 5).forEach((s, i) => {
        const marker = i === 0 ? '  <-- ALES' : '';
        console.log(`   [${s.matchScore}] ${s.entry.name} (${s.size}b)${marker}`);
    });

    return scored[0].entry;
}

async function extractFromZip(buffer, videoFilename, knownSeason, knownEpisode, depth = 0) {
    const zip = new AdmZip(buffer);
    const zipEntries = zip.getEntries();

    const candidates = zipEntries
        .filter(e => {
            const fn = e.entryName.toLowerCase();
            const base = fn.split('/').pop();
            // .ass/.ssa acceptate la fel ca .srt/.sub — confirmat pe productie
            // (titrari.ro, One Piece): unele zip-uri contin doar scriptul .ass
            // original, niciodata un .srt/.sub, si erau ignorate complet aici,
            // aruncand NO_SRT_IN_ZIP desi subtitrarea buna exista in arhiva.
            return !fn.includes('__macosx') && !base.startsWith('.') &&
                   (fn.endsWith('.srt') || fn.endsWith('.sub') || fn.endsWith('.ass') || fn.endsWith('.ssa')) &&
                   (e.header.size || 0) <= MAX_SUBTITLE_FILE_SIZE;
        })
        .map(e => ({ name: e.entryName, size: e.header.size || 0, _entry: e }));

    if (candidates.length === 0) {
        // O arhiva imbricata (probabil pachet multi-sezon) are prioritate fata de
        // orice .txt gasit la nivelul de sus — altfel un README/Citeste-ma.txt (sau
        // un fisier junk __MACOSX/._ceva.txt de pe Mac) era trimis silentios ca
        // "subtitrare" in loc sa incercam arhiva reala de alaturi.
        const nested = zipEntries.filter(e => /\.(rar|zip)$/i.test(e.entryName));
        if (nested.length > 0) {
            const matchedName = (knownSeason && depth < MAX_NESTED_DEPTH)
                ? findSeasonMatchedNestedArchive(nested.map(e => e.entryName), knownSeason)
                : null;
            const matched = matchedName ? nested.find(e => e.entryName === matchedName) : null;

            if (matched && (matched.header.size || 0) <= MAX_NESTED_ARCHIVE_SIZE) {
                console.log(`[ZIP] Arhiva contine ${nested.length} arhive imbricate — recurg in cea a sezonului cunoscut: "${matched.entryName}"`);
                const nestedBuffer = matched.getData();
                if (nestedBuffer.length <= MAX_NESTED_ARCHIVE_SIZE) {
                    const nestedType = detectArchiveType(nestedBuffer);
                    if (nestedType === 'zip') return await extractFromZip(nestedBuffer, videoFilename, knownSeason, knownEpisode, depth + 1);
                    if (nestedType === 'rar') return await extractFromRar(nestedBuffer, videoFilename, knownSeason, knownEpisode, depth + 1);
                } else {
                    console.error(`[ZIP] Arhiva imbricata "${matched.entryName}" a decomprimat la ${nestedBuffer.length} bytes — peste limita reala, o ignor (header posibil falsificat).`);
                }
            }

            console.error(`[ZIP] Arhiva contine ${nested.length} arhive imbricate (probabil pachet multi-sezon), nu extragem recursiv: ${nested.map(e => e.entryName).join(', ')}`);
            throw new Error('NESTED_ARCHIVE_UNSUPPORTED');
        }

        // Fallback .txt — doar daca nu exista nicio arhiva imbricata. Excludem
        // fisiere junk (__MACOSX, dotfiles) la fel ca la .srt/.sub mai sus, si
        // validam ca arata a subtitrare reala (contine "-->" sau incepe cu un
        // index numeric) inainte sa il acceptam — altfel un README ajungea sa
        // fie trimis ca WebVTT fara niciun cue real.
        const txt = zipEntries.find(e => {
            const fn = e.entryName.toLowerCase();
            const base = fn.split('/').pop();
            if (fn.includes('__macosx') || base.startsWith('.') || !fn.endsWith('.txt')) return false;
            const preview = e.getData().slice(0, 200).toString('utf8');
            return preview.includes('-->') || /^\d+\s*\r?\n/.test(preview);
        });
        if (txt) return { data: txt.getData(), isAss: false };

        throw new Error('NO_SRT_IN_ZIP');
    }

    const best = pickBestSubtitleFile(candidates, videoFilename, knownSeason, knownEpisode);
    if (!best) throw new Error('EPISODE_NOT_IDENTIFIED');
    const data = best._entry.getData();
    if (data.length > MAX_SUBTITLE_FILE_SIZE) throw new Error('SUBTITLE_TOO_LARGE');
    // Doar limita de sus era verificata — un fisier gol (placeholder ramas din
    // greseala la upload, sau o intrare corupta) trecea nedetectat si ajungea
    // trimis ca "WEBVTT" fara niciun cue, cu raspuns 200 normal.
    if (data.length === 0) throw new Error('SUBTITLE_EMPTY');
    const isAss = /\.(ass|ssa)$/i.test(best.name);
    // ".sub" e ambiguu (MicroDVD si SubViewer folosesc aceeasi extensie) — spre
    // deosebire de .ass/.ssa mai sus, aici verificam CONTINUTUL, nu extensia.
    // Fara asta, un MicroDVD extras dintr-o arhiva trecea nedetectat (nu
    // arunca nicio eroare, dar srtToVtt nu recunoaste "{508}{583}" ca linie de
    // timp) si ajungea trimis ca WebVTT gol, fara niciun cue — o subtitrare
    // "esuata silentios", mai rea decat eroarea explicita pe care o rezolvam.
    const isMicroDvd = !isAss && isMicroDvdText(data.toString('latin1', 0, 200));
    return { data, isAss, isMicroDvd };
}

async function extractFromRar(buffer, videoFilename, knownSeason, knownEpisode, depth = 0) {
    try {
        const { createExtractorFromData } = require('node-unrar-js');
        const extractor = await createExtractorFromData(
            unrarWasmBinary ? { data: buffer, wasmBinary: unrarWasmBinary } : { data: buffer }
        );
        const list = extractor.getFileList();
        const fileHeaders = [...list.fileHeaders];

        const candidates = fileHeaders
            .filter(h => {
                const fn = h.name.toLowerCase();
                return (fn.endsWith('.srt') || fn.endsWith('.sub') || fn.endsWith('.ass') || fn.endsWith('.ssa')) &&
                       (h.unpSize || h.packSize || 0) <= MAX_SUBTITLE_FILE_SIZE;
            })
            .map(h => ({ name: h.name, size: h.unpSize || h.packSize || 0 }));

        if (candidates.length === 0) {
            // Unele pachete "serie completa" sunt o arhiva ce contine alte arhive
            // imbricate (cate un .rar per sezon). Daca stim sigur sezonul cerut si
            // EXACT una dintre ele il mentioneaza, recursam o singura data in ea —
            // altfel (sezon necunoscut sau ambiguu) logam explicit si renuntam, ca
            // sa fie clar dintr-o privire in loguri de ce a esuat descarcarea asta.
            const nested = fileHeaders.filter(h => /\.(rar|zip)$/i.test(h.name));
            if (nested.length > 0) {
                const matchedName = (knownSeason && depth < MAX_NESTED_DEPTH)
                    ? findSeasonMatchedNestedArchive(nested.map(h => h.name), knownSeason)
                    : null;
                const matchedHeader = matchedName ? nested.find(h => h.name === matchedName) : null;
                const matchedSize = matchedHeader ? (matchedHeader.unpSize || matchedHeader.packSize || 0) : 0;

                if (matchedHeader && matchedSize <= MAX_NESTED_ARCHIVE_SIZE) {
                    console.log(`[RAR] Arhiva contine ${nested.length} arhive imbricate — recurg in cea a sezonului cunoscut: "${matchedHeader.name}"`);
                    const nestedExtracted = extractor.extract({ files: [matchedHeader.name] });
                    const nestedFiles = [...nestedExtracted.files];
                    if (nestedFiles.length > 0 && nestedFiles[0].extraction) {
                        const nestedBuffer = Buffer.from(nestedFiles[0].extraction);
                        if (nestedBuffer.length <= MAX_NESTED_ARCHIVE_SIZE) {
                            const nestedType = detectArchiveType(nestedBuffer);
                            if (nestedType === 'zip') return await extractFromZip(nestedBuffer, videoFilename, knownSeason, knownEpisode, depth + 1);
                            if (nestedType === 'rar') return await extractFromRar(nestedBuffer, videoFilename, knownSeason, knownEpisode, depth + 1);
                        } else {
                            console.error(`[RAR] Arhiva imbricata "${matchedHeader.name}" a decomprimat la ${nestedBuffer.length} bytes — peste limita reala, o ignor (header posibil falsificat).`);
                        }
                    }
                }

                console.error(`[RAR] Arhiva contine ${nested.length} arhive imbricate (probabil pachet multi-sezon), nu extragem recursiv: ${nested.map(h => h.name).join(', ')}`);
                throw new Error('NESTED_ARCHIVE_UNSUPPORTED');
            }
            throw new Error('NO_SRT_IN_RAR');
        }

        const best = pickBestSubtitleFile(candidates, videoFilename, knownSeason, knownEpisode);
        if (!best) throw new Error('EPISODE_NOT_IDENTIFIED');
        console.log(`[RAR] Extrag: "${best.name}"`);

        const extracted = extractor.extract({ files: [best.name] });
        const files = [...extracted.files];
        if (files.length === 0) throw new Error('RAR_EXTRACT_FAILED');

        const finalBuffer = Buffer.from(files[0].extraction);
        if (finalBuffer.length > MAX_SUBTITLE_FILE_SIZE) throw new Error('SUBTITLE_TOO_LARGE');
        if (finalBuffer.length === 0) throw new Error('SUBTITLE_EMPTY');
        const isAss = /\.(ass|ssa)$/i.test(best.name);
        // Acelasi motiv ca in extractFromZip: ".sub" e ambiguu, verificam continutul.
        const isMicroDvd = !isAss && isMicroDvdText(finalBuffer.toString('latin1', 0, 200));
        return { data: finalBuffer, isAss, isMicroDvd };
    } catch (err) {
        console.error('[RAR] Eroare extractie:', err.message);
        throw new Error('RAR_EXTRACT_FAILED');
    }
}

app.get(['/download', '/download.vtt'], async (req, res) => {
    const zipUrl = req.query.url;
    const source = req.query.source || 'regielive';
    const sessionCookie = req.query.cookie || '';
    const videoFilename = req.query.vf || '';
    const knownSeason  = req.query.season  ? parseInt(req.query.season, 10)  : null;
    const knownEpisode = req.query.episode ? parseInt(req.query.episode, 10) : null;

    if (!zipUrl) return res.status(400).send('URL lipsa');

    // Fara asta, orice client (nu doar Stremio) putea cere serverului sa
    // descarce si parseze orice URL, de pe orice domeniu — risc SSRF + vector
    // de amplificare pt. un atac de tip decompression-bomb pe un domeniu strain.
    let parsedUrl;
    try {
        parsedUrl = new URL(zipUrl);
    } catch {
        return res.status(400).send('URL invalid.');
    }
    if (parsedUrl.protocol !== 'http:' && parsedUrl.protocol !== 'https:') {
        return res.status(400).send('Protocol nepermis.');
    }
    if (!isAllowedDownloadHost(parsedUrl.hostname, source)) {
        console.error(`[SECURITATE] URL refuzat — domeniul "${parsedUrl.hostname}" nu e permis pentru sursa "${source}".`);
        return res.status(403).send('Domeniu nepermis pentru aceasta sursa.');
    }

    if (source === 'regielive' && zipUrl.includes('/descarca-') && zipUrl.endsWith('-0.zip')) {
        console.log(`[FILTRU] URL invalid RegieLive (id 0), refuz.`);
        return res.status(404).send('Subtitrare indisponibila.');
    }

    const cacheKey = `${zipUrl}::${videoFilename}`;

    const sendSubtitleResponse = (text, responseObj) => {
        const fixedText = fixRomanianDiacritics(text);
        const vttText = srtToVtt(fixedText);
        responseObj.setHeader('Content-Type', 'text/vtt; charset=utf-8');
        responseObj.setHeader('Content-Disposition', 'inline; filename="subtitle.vtt"');
        responseObj.setHeader('Access-Control-Allow-Origin', '*');
        return responseObj.send(vttText);
    };

    // 1. Cache in memorie (cel mai rapid)
    if (memCache.has(cacheKey)) {
        console.log(`[CACHE-MEM] Hit: ${videoFilename || zipUrl}`);
        return sendSubtitleResponse(memCache.get(cacheKey), res);
    }

    // 2. Cache persistent (SQLite local sau Redis pe Vercel) — supravietuieste repornirii
    const fromDb = await cacheDb.getSubtitle(cacheKey);
    if (fromDb) {
        console.log(`[CACHE-DB] Hit: ${videoFilename || zipUrl}`);
        memCache.set(cacheKey, fromDb);
        return sendSubtitleResponse(fromDb, res);
    }

    if (activeDownloads.has(cacheKey)) {
        try {
            return sendSubtitleResponse(await activeDownloads.get(cacheKey), res);
        } catch {
            return res.status(500).send('Eroare');
        }
    }

    const downloadTask = async () => {
        console.log(`\n[DESCARCARE][${source}] ${zipUrl}`);
        if (videoFilename) console.log(`[DESCARCARE] Pentru: ${videoFilename}`);

        const headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
            'Accept': 'application/octet-stream, */*',
        };

        if (source === 'regielive') {
            headers['RL-API']  = RL_API_KEY;
            headers['Cookie']  = sessionCookie;
            headers['Referer'] = 'https://subtitrari.regielive.ro';
            await waitForRegieliveDownloadSlot();
        } else if (source === 'titrari') {
            headers['Cookie']  = `PHPSESSID=${TITRARI_COOKIE}`;
            headers['Referer'] = 'https://www.titrari.ro';
            headers['Host']    = 'www.titrari.ro';
        } else if (source === 'subsro') {
            headers['X-Subs-Api-Key'] = process.env.SUBSRO_API_KEY || '';
        }

        const response = await fetchWithRetry429({
            method: 'get',
            url: zipUrl,
            responseType: 'arraybuffer',
            headers,
            maxRedirects: 5,
            maxContentLength: MAX_DOWNLOAD_SIZE,
            maxBodyLength: MAX_DOWNLOAD_SIZE,
            // Un redirect poate duce in afara domeniului validat mai sus — verificam
            // si fiecare hop, nu doar URL-ul initial.
            beforeRedirect: (options) => {
                if (!isAllowedDownloadHost(options.hostname, source)) {
                    throw new Error(`Redirect catre domeniu nepermis: ${options.hostname}`);
                }
            }
        }, source);

        const buffer = Buffer.from(response.data);
        const archiveType = detectArchiveType(buffer);
        console.log(`[ARHIVA] Tip detectat: ${archiveType} (${buffer.length} bytes)`);

        let rawData;
        let isRawAss = false;
        let isRawMicroDvd = false;

        if (archiveType === 'zip') {
            const extracted = await extractFromZip(buffer, videoFilename, knownSeason, knownEpisode);
            rawData = extracted.data;
            isRawAss = extracted.isAss;
            isRawMicroDvd = extracted.isMicroDvd;
        } else if (archiveType === 'rar') {
            const extracted = await extractFromRar(buffer, videoFilename, knownSeason, knownEpisode);
            rawData = extracted.data;
            isRawAss = extracted.isAss;
            isRawMicroDvd = extracted.isMicroDvd;
        } else {
            const preview = buffer.slice(0, 50).toString('utf8');
            if (preview.includes('-->') || /^\d+\s*\n/.test(preview)) {
                console.log(`[ARHIVA] SRT direct, il folosesc ca atare.`);
                rawData = buffer;
            } else {
                // Markerii ASS/SSA ("[Script Info]", "Dialogue:") sunt ASCII pur,
                // deci ii putem cauta corect indiferent de encoding-ul real al
                // fisierului (utf8/windows-1250/etc.) — decodarea corecta se face
                // oricum mai jos, inainte de conversia efectiva la SRT.
                const asciiPreview = buffer.toString('latin1');
                if (/\[Script Info\]/i.test(asciiPreview) && /\r?\nDialogue:\s*\d/i.test(asciiPreview)) {
                    console.log(`[ARHIVA] ASS/SSA direct (fara arhiva), il convertesc la SRT.`);
                    rawData = buffer;
                    isRawAss = true;
                } else if (isMicroDvdText(asciiPreview)) {
                    // Confirmat pe productie (Titrari id=8614, Ghosts of Mars): un
                    // .sub MicroDVD (timestamp pe cadre) servit direct, fara arhiva —
                    // vezi comentariul de la microDvdToSrt() pt. detaliile pe fps.
                    console.log(`[ARHIVA] MicroDVD direct (fara arhiva), il convertesc la SRT.`);
                    rawData = buffer;
                    isRawMicroDvd = true;
                } else {
                    const bodyStr = buffer.toString('utf8');
                    const titleMatch = bodyStr.match(/<title>([\s\S]*?)<\/title>/i);
                    console.error(`[X][${source}] Format necunoscut!`);
                    console.error(`    Content-Type: ${response.headers['content-type'] || ''}`);
                    console.error(`    <title>: ${titleMatch ? titleMatch[1].trim() : '(fara title)'}`);
                    console.error(`    Primele 200 chars: ${bodyStr.slice(0, 200)}`);
                    throw new Error('UNKNOWN_FORMAT');
                }
            }
        }

        const detected = jschardet.detect(rawData);
        let encoding = 'windows-1250';
        if (detected && detected.encoding) {
            const enc = detected.encoding.toLowerCase();
            if (enc.includes('utf') || enc === 'ascii') encoding = enc;
        }
        console.log(`[ENCODING] Detectat: ${detected?.encoding} → folosesc: ${encoding}`);

        const decoded = iconv.decode(rawData, encoding);
        if (isRawAss) return assToSrt(decoded);
        if (isRawMicroDvd) return microDvdToSrt(decoded);
        return decoded;
    };

    const queuedTask = IS_SERVERLESS
        ? downloadTask()
        : new Promise((resolve, reject) => {
            globalDownloadQueue = globalDownloadQueue.then(async () => {
                try {
                    await new Promise(r => setTimeout(r, 1500));
                    resolve(await downloadTask());
                } catch (e) {
                    reject(e);
                }
            }).catch(() => {});
        });

    activeDownloads.set(cacheKey, queuedTask);

    try {
        const subtitleText = await queuedTask;
        // Salvam in ambele layere de cache
        memCache.set(cacheKey, subtitleText);
        await cacheDb.setSubtitle(cacheKey, subtitleText);
        activeDownloads.delete(cacheKey);
        return sendSubtitleResponse(subtitleText, res);
    } catch (error) {
        activeDownloads.delete(cacheKey);
        if (error.response?.status === 429) {
            console.error(`[X][${source}] RATE LIMIT atins.`);
        } else {
            // Pana acum, orice alta eroare decat 429 disparea complet — niciun
            // mesaj, doar "500 Eroare interna" fara nicio pista. Logam explicit
            // codul HTTP (daca a raspuns sursa), codul de eroare de retea
            // (timeout, refuz de conexiune etc.) si mesajul, ca sa nu mai ghicim.
            console.error(`[X][${source}] Descarcare esuata pentru ${zipUrl}:`);
            console.error(`    Mesaj: ${error.message}`);
            console.error(`    Cod retea: ${error.code || '(niciunul)'}`);
            if (error.response) {
                console.error(`    Status HTTP raspuns: ${error.response.status}`);
            }
        }
        res.status(500).send('Eroare interna.');
    }
});

// Curatarea periodica are sens doar pe un proces persistent (local/Pi) — pe
// Vercel fiecare invocare e scurta si separata, iar backend-ul Redis oricum
// expira singur intrarile prin TTL nativ (cleanup() e no-op acolo).
if (!IS_SERVERLESS) {
    setInterval(() => cacheDb.cleanup(), 24 * 60 * 60 * 1000);
}

// Pe Vercel, server.js e doar cerut ca modul (Vercel gestioneaza singur
// invocarea HTTP prin app-ul exportat mai jos) — nu trebuie sa asculte pe un
// port. Local (`node server.js`, sau viitor pe Raspberry Pi), ramane neschimbat.
if (require.main === module) {
    const port = process.env.PORT || 7000;
    app.listen(port, async () => {
        console.log(`Mega Subtitle Addon ruleaza la http://127.0.0.1:${port}/manifest.json`);
        if (process.env.RENDER_EXTERNAL_URL) require('./keep-alive');
        const s = await cacheDb.stats();
        if (s.available && cacheDb.backend === 'sqlite') {
            console.log(`[CACHE-DB] ${s.searches} cautari, ${s.subtitles} subtitrari, ${s.sizeMB} MB`);
        } else if (s.available) {
            console.log(`[CACHE-DB] Backend ${cacheDb.backend}, ${s.keys ?? '?'} chei.`);
        }
    });
}

module.exports = app;
