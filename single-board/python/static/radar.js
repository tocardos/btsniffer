/*
 * radar.js  (multi-source)
 *
 * Owns all of the visualization logic. The server does nothing but relay
 * raw JSON from one or more ZMQ sources (each stamped with a "src" field by
 * app.py), so everything below -- angle assignment, RSSI-to-radius mapping,
 * master/slave pairing, per-source tracking, staleness/fade -- lives here
 * and can be retuned without touching the Python side.
 *
 * Multi-source comparison design:
 *   - angle   = LAP (hashed), SHARED by all sources -> readings of the same
 *               LAP from different sniffers land on the same spoke, so
 *               disagreement is visible as *distance between dots*.
 *   - radius  = RSSI (EMA), same mapping for every source.
 *   - color   = source  (btsniffer / ubertooth / auto palette; see below).
 *   - shape   = role    (circle = master, diamond = slave) -- role can't be
 *               color anymore since color encodes the source.
 *   - sweep   = one sweep line per source, in the source's color and
 *               phase-offset, so freshness is readable per sniffer.
 *   - sidebar = one card per LAP, one row per source, plus a comparison
 *               block: ΔRSSI, UAP match/mismatch, ΔCFO.
 *
 * Expected message shape (one JSON object per WebSocket text frame), same
 * as zmq_sender.cpp's zmq_send_lap_uap_pdu() on the sniffer side, plus the
 * "src" stamp added by app.py:
 *   {
 *     src,           // "btsniffer" | "ubertooth" | ... (relay-stamped)
 *     lap, uap, channel, pkt_flag,
 *     role,          // 0 = unknown, 1 = master, 2 = slave
 *     rssi_dbm, snr_db, cfo_hz, cfo_diff_hz, cfo_valid,
 *     pdu_len, pdu_hex, ac_errors
 *   }
 */

// ---- tunables --------------------------------------------------------
const RSSI_MIN_DBM = -100;   // maps to the outer edge of the scope
const RSSI_MAX_DBM = -30;    // maps to the center
// Role separation within a source's slot, and source separation within a
// LAP's spoke. SOURCE_SPREAD dominates so same-LAP dots from different
// sniffers stay visually grouped but never overlap.
const ROLE_ANGLE_OFFSET = 0.030;
const SOURCE_SPREAD = 0.13;   // rad between adjacent sources on one spoke
const STALE_MS = 8000;    // dot starts fading after this long without an update
const REMOVE_MS = 25000;   // dot (and its source entry) is dropped after this long
const SWEEP_PERIOD_MS = 6000;    // one full rotation of each radar sweep
const SWEEP_PHASE_STEP = 1.05;   // rad of phase offset between source sweeps
const RING_COUNT = 4;       // number of range rings

// Per-source color table. Keys must match the names given on the app.py
// command line (--zmq btsniffer=... --zmq ubertooth=...). Anything not
// listed here gets a color from SOURCE_COLOR_POOL in first-sighting order.
const SOURCE_STYLES = {
  btsniffer: { color: "#4fd6d0" },   // teal
  ubertooth: { color: "#e0a94f" },   // amber
};
const SOURCE_COLOR_POOL = ["#a06fe0", "#6fd06f", "#e06f6f", "#6fa8e0", "#d0d06f", "#e08fc0"];

// A packet's access-code correlation error count (ac_errors) is a decent
// proxy for "how marginal was this detection". High-ac_errors packets are
// where RSSI, clock-offset-derived role, and CFO validity are all least
// trustworthy -- they still keep the dot alive (ts bump) but don't get to
// move the displayed RSSI/role, so a handful of noisy detections can't
// make a static device's dot jump or flip roles.
const AC_ERRORS_TRUST_MAX = 1;
// RSSI is smoothed per (src, lap, role) with a simple EMA -- scoped to one
// physical transmitter as heard by one sniffer, unlike the sniffer's own
// per-*channel* history, which mixes in whatever else the sweep heard on
// that channel index. Never blend EMAs across sources: they disagree by
// definition and blending would hide exactly what we're trying to compare.
const RSSI_EMA_ALPHA = 0.3;

// ---- state -------------------------------------------------------------
// laps: Map<lapKey, {
//   lap, angle,
//   name: string | null,              // resolved device name (any source)
//   bySource: Map<srcKey, {
//     master: null | {uap, rssi, rssiEma, snr, cfo, ts},
//     slave:  null | {uap, rssi, rssiEma, snr, cfo, ts},
//     unknown: null | {uap, rssi, rssiEma, snr, ts},
//     cfoDiff: number
//   }>
// }>
const laps = new Map();

// Sources in first-sighting order; the index drives each source's angular
// slot on a spoke and its sweep phase. Kept globally (not per-LAP) so the
// color legend and slot order stay stable as LAPs appear and vanish.
const srcOrder = [];
const srcColorCache = new Map();

function srcIndexOf(src) {
  let i = srcOrder.indexOf(src);
  if (i === -1) {
    srcOrder.push(src);
    i = srcOrder.length - 1;
    if (!SOURCE_STYLES[src]) {
      srcColorCache.set(src, SOURCE_COLOR_POOL[i % SOURCE_COLOR_POOL.length]);
    }
  }
  return i;
}

function srcColor(src) {
  return (SOURCE_STYLES[src] && SOURCE_STYLES[src].color) || srcColorCache.get(src) || "#6b7680";
}

const canvas = document.getElementById("scope");
const ctx = canvas.getContext("2d");
const statusDot = document.getElementById("statusDot");
const statusText = document.getElementById("statusText");
const lapListEl = document.getElementById("lapList");

// ---- websocket -----------------------------------------------------------
let ws = null;
let reconnectDelay = 1000;

function connectWS() {
  const proto = location.protocol === "https:" ? "wss" : "ws";
  ws = new WebSocket(`${proto}://${location.host}/ws`);

  ws.onopen = () => {
    statusDot.classList.remove("offline");
    statusDot.classList.add("online");
    statusText.textContent = "live";
    reconnectDelay = 1000;
  };

  ws.onclose = () => {
    statusDot.classList.remove("online");
    statusDot.classList.add("offline");
    statusText.textContent = "reconnecting…";
    setTimeout(connectWS, reconnectDelay);
    reconnectDelay = Math.min(reconnectDelay * 1.6, 15000);
  };

  ws.onerror = () => ws.close();

  ws.onmessage = (evt) => {
    let msg;
    try {
      msg = JSON.parse(evt.data);
    } catch (e) {
      return;
    }
    handleMessage(msg);
  };
}

// ---- message handling ------------------------------------------------
function hashAngle(lapValue) {
  const s = String(lapValue);
  let h = 2166136261;
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 16777619);
  }
  // Fold into [0, 2*PI)
  return ((h >>> 0) % 100000 / 100000) * Math.PI * 2;
}

function getOrCreateLap(lapValue) {
  const key = String(lapValue);
  let entry = laps.get(key);
  if (!entry) {
    entry = {
      lap: lapValue,
      angle: hashAngle(lapValue),
      name: null,
      bySource: new Map(),
    };
    laps.set(key, entry);
  }
  return entry;
}

function getOrCreateSourceEntry(lapEntry, src) {
  let e = lapEntry.bySource.get(src);
  if (!e) {
    e = { master: null, slave: null, unknown: null, cfoDiff: 0 };
    lapEntry.bySource.set(src, e);
  }
  return e;
}

// Parses the "??:??:UU:LL:LL:LL" addresses used by the sniffer's
// discovery/name events back into a lap value, so a standalone "name"
// event (published as soon as HCI inquiry resolves it) can be matched
// against an already-tracked lap entry without waiting for another packet.
function lapFromAddr(addr) {
  if (typeof addr !== "string") return null;
  const parts = addr.split(":");
  if (parts.length !== 6) return null;
  const lap = parseInt(parts[3] + parts[4] + parts[5], 16);
  return Number.isNaN(lap) ? null : lap;
}

function handleMessage(msg) {
  // Every relayed message carries the "src" stamp added by app.py. Fall
  // back to a generic bucket so a mis-stamped message still renders
  // instead of silently vanishing.
  const src = (typeof msg.src === "string" && msg.src) ? msg.src : "default";
  srcIndexOf(src);

  // Standalone name-resolution event -- arrives independently of packet
  // flow, as soon as HCI inquiry succeeds. Names are per-device, not per-
  // sniffer, so we resolve them at LAP level; we also make sure this
  // source has an entry so it shows up in the legend/sidebar even before
  // its first packet.
  if (msg.event === "name" && typeof msg.name === "string" && msg.name) {
    const lap = lapFromAddr(msg.addr);
    if (lap !== null) {
      const lapEntry = getOrCreateLap(lap);
      lapEntry.name = msg.name;
      getOrCreateSourceEntry(lapEntry, src);
    }
    return;
  }

  if (msg.lap === undefined) return;

  // ac_errors is the access-code correlation error count for this
  // specific packet -- a decent proxy for how marginal the detection
  // was. High-ac_errors packets are exactly where RSSI, clock-offset-
  // derived role, and CFO are all least trustworthy. Skip using them to
  // move the display (no rssi/role/cfo write) so a run of noisy
  // detections can't make a static device's dot jump around or flip
  // between the master and slave buckets. The entry itself isn't
  // touched, so it keeps fading/staling based on its last *trusted*
  // update rather than being propped up by noise.
  const trusted = typeof msg.ac_errors !== "number" || msg.ac_errors <= AC_ERRORS_TRUST_MAX;
  if (!trusted) return;

  const lapEntry = getOrCreateLap(msg.lap);
  const entry = getOrCreateSourceEntry(lapEntry, src);

  // Pick whichever bucket this role already has a sample in (if any) so
  // the EMA is scoped to that specific (source, role)'s own recent
  // history, not blended across master/slave/unknown -- and never
  // blended across sources.
  const prevBucket = msg.role === 1 ? entry.master
                    : msg.role === 2 ? entry.slave
                    : entry.unknown;
  const rssiEma = (typeof msg.rssi_dbm === "number")
    ? (prevBucket && typeof prevBucket.rssiEma === "number"
        ? prevBucket.rssiEma * (1 - RSSI_EMA_ALPHA) + msg.rssi_dbm * RSSI_EMA_ALPHA
        : msg.rssi_dbm)
    : (prevBucket ? prevBucket.rssiEma : undefined);

  const sample = {
    uap: msg.uap,
    channel: msg.channel,
    rssi: msg.rssi_dbm,     // raw, this packet only -- kept for reference/debugging
    rssiEma,                // smoothed, per-(src, lap, role) -- what the UI draws
    snr: msg.snr_db,
    // The C side sends cfo_hz as a hardcoded 0.0 whenever this specific
    // packet didn't carry a valid FREQEST reading (cfo_valid=false) --
    // that's normal and common, not "no signal". Only take the new value
    // when it's actually valid; otherwise keep whatever the last valid
    // reading was, so the display doesn't get stomped back to 0 by every
    // cfo_valid=false packet in between.
    cfo: (msg.cfo_valid === undefined || msg.cfo_valid)
      ? msg.cfo_hz
      : (prevBucket ? prevBucket.cfo : msg.cfo_hz),
    ts: Date.now(),
  };

  if (msg.role === 1) {
    entry.master = sample;
  } else if (msg.role === 2) {
    entry.slave = sample;
  } else {
    entry.unknown = sample;
  }

  if (typeof msg.cfo_diff_hz === "number") {
    entry.cfoDiff = msg.cfo_diff_hz;
  }

  // Packet-level messages also carry the name once resolved (see
  // get_name_for_device() on the sniffer side).
  if (typeof msg.name === "string" && msg.name) {
    lapEntry.name = msg.name;
  }
}

function pruneStale() {
  const now = Date.now();
  for (const [lapKey, lapEntry] of laps) {
    for (const [src, entry] of lapEntry.bySource) {
      if (entry.master && now - entry.master.ts > REMOVE_MS) entry.master = null;
      if (entry.slave && now - entry.slave.ts > REMOVE_MS) entry.slave = null;
      if (entry.unknown && now - entry.unknown.ts > REMOVE_MS) entry.unknown = null;
      if (!entry.master && !entry.slave && !entry.unknown) lapEntry.bySource.delete(src);
    }
    if (lapEntry.bySource.size === 0) laps.delete(lapKey);
  }
}

// ---- geometry helpers --------------------------------------------------
function rssiToRadiusFrac(rssiDbm) {
  const clamped = Math.max(RSSI_MIN_DBM, Math.min(RSSI_MAX_DBM, rssiDbm));
  const t = (clamped - RSSI_MIN_DBM) / (RSSI_MAX_DBM - RSSI_MIN_DBM); // 0..1, 1 = strongest
  // Strong signal -> near center, weak -> near edge. Keep a small inner
  // deadzone (0.06) so max-strength dots don't sit exactly on the origin.
  return 0.94 - t * 0.88;
}

function polarToXY(cx, cy, maxR, angle, radiusFrac) {
  const r = maxR * radiusFrac;
  return {
    x: cx + r * Math.sin(angle),
    y: cy - r * Math.cos(angle),
  };
}

function ageOpacity(ts) {
  const age = Date.now() - ts;
  if (age <= STALE_MS) return 1.0;
  if (age >= REMOVE_MS) return 0.0;
  const t = (age - STALE_MS) / (REMOVE_MS - STALE_MS);
  return 1.0 - t * 0.88; // fades to a dim 0.12, not fully invisible until removed
}

// Angular slot for the i-th of n sources on one LAP spoke, centered on
// the spoke so the group stays symmetric around the LAP angle.
function slotOffset(i, n) {
  return (i - (n - 1) / 2) * SOURCE_SPREAD;
}

// ---- drawing -------------------------------------------------------------
function drawGrid(cx, cy, maxR) {
  ctx.save();
  ctx.strokeStyle = "#1c2b33";
  ctx.fillStyle = "#54646c";
  ctx.font = "11px monospace";
  ctx.lineWidth = 1;

  for (let i = 1; i <= RING_COUNT; i++) {
    const frac = i / RING_COUNT;
    ctx.beginPath();
    ctx.arc(cx, cy, maxR * frac, 0, Math.PI * 2);
    ctx.stroke();

    // Label each ring with the RSSI it represents (inverse of rssiToRadiusFrac)
    const t = (0.94 - frac) / 0.88;
    const dbm = Math.round(RSSI_MIN_DBM + t * (RSSI_MAX_DBM - RSSI_MIN_DBM));
    ctx.fillText(`${dbm} dBm`, cx + 4, cy - maxR * frac - 3);
  }

  const spokes = 12;
  for (let i = 0; i < spokes; i++) {
    const a = (i / spokes) * Math.PI * 2;
    const p = polarToXY(cx, cy, maxR, a, 1);
    ctx.beginPath();
    ctx.moveTo(cx, cy);
    ctx.lineTo(p.x, p.y);
    ctx.stroke();
  }
  ctx.restore();
}

// One sweep line per source, in the source's color and phase-offset by
// source index. Each sniffer's "freshness" is therefore readable on its
// own colored sweep rather than a single shared one.
function drawSweeps(cx, cy, maxR) {
  const t = Date.now();
  srcOrder.forEach((src, i) => {
    const angle = ((t % SWEEP_PERIOD_MS) / SWEEP_PERIOD_MS) * Math.PI * 2
                + i * SWEEP_PHASE_STEP;
    const color = srcColor(src);
    const rgb = hexToRgb(color);

    ctx.save();
    if (ctx.createConicGradient) {
      const grad = ctx.createConicGradient(angle - Math.PI / 2, cx, cy);
      grad.addColorStop(0.0, `rgba(${rgb}, 0.20)`);
      grad.addColorStop(0.06, `rgba(${rgb}, 0.0)`);
      grad.addColorStop(1.0, `rgba(${rgb}, 0.0)`);
      ctx.fillStyle = grad;
      ctx.beginPath();
      ctx.arc(cx, cy, maxR, 0, Math.PI * 2);
      ctx.fill();
    }

    const tip = polarToXY(cx, cy, maxR, angle, 1);
    ctx.strokeStyle = `rgba(${rgb}, 0.65)`;
    ctx.lineWidth = 1.5;
    ctx.beginPath();
    ctx.moveTo(cx, cy);
    ctx.lineTo(tip.x, tip.y);
    ctx.stroke();
    ctx.restore();
  });
}

function hexToRgb(hex) {
  const m = /^#?([0-9a-f]{6})$/i.exec(hex);
  if (!m) return "128,128,128";
  const v = parseInt(m[1], 16);
  return `${(v >> 16) & 255},${(v >> 8) & 255},${v & 255}`;
}

// Source colors are taken by source, so role is encoded by shape:
// circle = master, diamond = slave. (Unknown-role dots stay small gray
// circles, drawn separately below.)
function drawDot(pos, color, opacity, shape) {
  ctx.save();
  ctx.globalAlpha = opacity;
  ctx.fillStyle = color;
  ctx.shadowColor = color;
  ctx.shadowBlur = 10;
  ctx.beginPath();
  if (shape === "diamond") {
    const r = 6.5;
    ctx.moveTo(pos.x, pos.y - r);
    ctx.lineTo(pos.x + r, pos.y);
    ctx.lineTo(pos.x, pos.y + r);
    ctx.lineTo(pos.x - r, pos.y);
    ctx.closePath();
  } else {
    ctx.arc(pos.x, pos.y, 5, 0, Math.PI * 2);
  }
  ctx.fill();
  ctx.restore();
}

function drawLabel(text, pos) {
  ctx.save();
  ctx.shadowBlur = 0;
  ctx.fillStyle = "#d7e2e6";
  ctx.font = "10px monospace";
  ctx.fillText(text, pos.x + 8, pos.y - 8);
  ctx.restore();
}

// Master -> slave link WITHIN one source (connection + CFO-diff
// fingerprint), colored amber as before.
function drawLink(posA, posB, opacity, cfoDiffHz) {
  ctx.save();
  ctx.globalAlpha = opacity;
  ctx.strokeStyle = "#e0a94f";
  ctx.lineWidth = 1.4;
  ctx.setLineDash([5, 4]);
  ctx.beginPath();
  ctx.moveTo(posA.x, posA.y);
  ctx.lineTo(posB.x, posB.y);
  ctx.stroke();
  ctx.setLineDash([]);

  // Arrowhead at the slave end, pointing from master -> slave.
  const angle = Math.atan2(posB.y - posA.y, posB.x - posA.x);
  const headLen = 8;
  ctx.fillStyle = "#e0a94f";
  ctx.beginPath();
  ctx.moveTo(posB.x, posB.y);
  ctx.lineTo(
    posB.x - headLen * Math.cos(angle - Math.PI / 6),
    posB.y - headLen * Math.sin(angle - Math.PI / 6)
  );
  ctx.lineTo(
    posB.x - headLen * Math.cos(angle + Math.PI / 6),
    posB.y - headLen * Math.sin(angle + Math.PI / 6)
  );
  ctx.closePath();
  ctx.fill();

  const midX = (posA.x + posB.x) / 2;
  const midY = (posA.y + posB.y) / 2;
  ctx.fillStyle = "#e0a94f";
  ctx.font = "10px monospace";
  ctx.fillText(`Δcfo ${cfoDiffHz.toFixed(0)} Hz`, midX + 6, midY - 4);
  ctx.restore();
}

// Thin gray dashed line connecting the SAME ROLE across sources on one
// LAP spoke -- the direct visual "how much do the sniffers disagree"
// indicator. Radius mismatch = RSSI disagreement; anything not on the
// spoke line = angle-slot separation only (cosmetic).
function drawCompareLink(posA, posB, opacity) {
  ctx.save();
  ctx.globalAlpha = opacity * 0.55;
  ctx.strokeStyle = "#9aa7ad";
  ctx.lineWidth = 1;
  ctx.setLineDash([2, 4]);
  ctx.beginPath();
  ctx.moveTo(posA.x, posA.y);
  ctx.lineTo(posB.x, posB.y);
  ctx.stroke();
  ctx.restore();
}

// Source color legend + shape legend, top-left of the scope.
function drawLegend() {
  ctx.save();
  ctx.font = "11px monospace";
  let x = 12;
  const y = 20;

  // role shapes
  ctx.fillStyle = "#d7e2e6";
  ctx.beginPath(); ctx.arc(x + 4, y - 3.5, 4, 0, Math.PI * 2); ctx.fill();
  ctx.fillText("master", x + 12, y);
  x += 12 + ctx.measureText("master").width + 18;
  ctx.beginPath();
  ctx.moveTo(x + 4, y - 8); ctx.lineTo(x + 8, y - 3.5); ctx.lineTo(x + 4, y + 1); ctx.lineTo(x, y - 3.5);
  ctx.closePath(); ctx.fill();
  ctx.fillText("slave", x + 12, y);
  x += 12 + ctx.measureText("slave").width + 24;

  ctx.fillStyle = "#54646c";
  ctx.fillText("sources:", x, y);
  x += ctx.measureText("sources:").width + 10;

  for (const src of srcOrder) {
    ctx.fillStyle = srcColor(src);
    ctx.fillRect(x, y - 9, 10, 10);
    ctx.fillStyle = "#d7e2e6";
    ctx.fillText(src, x + 14, y);
    x += 14 + ctx.measureText(src).width + 18;
  }
  ctx.restore();
}

function lapLabel(lapValue, uap, name) {
  const lapHex = Number(lapValue).toString(16).toUpperCase().padStart(6, "0");
  const id = (uap === undefined || uap === null)
    ? lapHex
    : `${lapHex}:${Number(uap).toString(16).toUpperCase().padStart(2, "0")}`;
  return name ? `${name} (${id})` : id;
}

function hexUap(uap) {
  return (uap === undefined || uap === null)
    ? "—"
    : Number(uap).toString(16).toUpperCase().padStart(2, "0");
}

function render() {
  const w = canvas.width, h = canvas.height;
  ctx.clearRect(0, 0, w, h);

  const cx = w / 2, cy = h / 2;
  const maxR = Math.min(w, h) / 2 - 30;

  drawGrid(cx, cy, maxR);
  drawSweeps(cx, cy, maxR);
  drawLegend();

  for (const lapEntry of laps.values()) {
    // Sources present for this LAP, in global first-sighting order so
    // slots/colors are stable frame to frame.
    const presentSrcs = srcOrder.filter(s => lapEntry.bySource.has(s));
    const n = presentSrcs.length;
    const anyBucket = lapEntry.bySource.get(presentSrcs[0]) || {};
    const label = lapLabel(lapEntry.lap,
      (anyBucket.master || anyBucket.slave || anyBucket.unknown || {}).uap,
      lapEntry.name);

    // Per-role positions across sources, for the cross-source compare link.
    const masterPos = [], slavePos = [];
    let labelPos = null;

    presentSrcs.forEach((src, i) => {
      const entry = lapEntry.bySource.get(src);
      const baseAngle = lapEntry.angle + slotOffset(i, n);
      const color = srcColor(src);

      if (entry.master) {
        const frac = rssiToRadiusFrac(entry.master.rssiEma);
        const pos = polarToXY(cx, cy, maxR, baseAngle - ROLE_ANGLE_OFFSET, frac);
        const op = ageOpacity(entry.master.ts);
        masterPos.push({ pos, op });
        drawDot(pos, color, op, "circle");
        if (!labelPos && op > 0) labelPos = pos;
      }
      if (entry.slave) {
        const frac = rssiToRadiusFrac(entry.slave.rssiEma);
        const pos = polarToXY(cx, cy, maxR, baseAngle + ROLE_ANGLE_OFFSET, frac);
        const op = ageOpacity(entry.slave.ts);
        slavePos.push({ pos, op });
        drawDot(pos, color, op, "diamond");
        if (!labelPos && op > 0) labelPos = pos;
      }

      // Active-connection arrow: only when both sides are present and
      // fresh, within the SAME source (a btsniffer master never pairs
      // with an ubertooth slave).
      if (entry.master && entry.slave) {
        const mFrac = rssiToRadiusFrac(entry.master.rssiEma);
        const sFrac = rssiToRadiusFrac(entry.slave.rssiEma);
        const mPos = polarToXY(cx, cy, maxR, baseAngle - ROLE_ANGLE_OFFSET, mFrac);
        const sPos = polarToXY(cx, cy, maxR, baseAngle + ROLE_ANGLE_OFFSET, sFrac);
        drawLink(mPos, sPos,
          Math.min(ageOpacity(entry.master.ts), ageOpacity(entry.slave.ts)),
          entry.cfoDiff);
      }

      if (entry.unknown) {
        const frac = rssiToRadiusFrac(entry.unknown.rssiEma);
        const pos = polarToXY(cx, cy, maxR, baseAngle, frac);
        const op = ageOpacity(entry.unknown.ts);
        ctx.save();
        ctx.globalAlpha = op;
        ctx.fillStyle = "#6b7680";
        ctx.beginPath();
        ctx.arc(pos.x, pos.y, 3.5, 0, Math.PI * 2);
        ctx.fill();
        ctx.restore();
        if (!labelPos && op > 0) labelPos = pos;
      }
    });

    // Cross-source comparison links between same-role dots.
    for (const arr of [masterPos, slavePos]) {
      for (let i = 0; i + 1 < arr.length; i++) {
        drawCompareLink(arr[i].pos, arr[i + 1].pos, Math.min(arr[i].op, arr[i + 1].op));
      }
    }

    if (labelPos) drawLabel(label, labelPos);
  }

  pruneStale();
  requestAnimationFrame(render);
}

// ---- sidebar list --------------------------------------------------------
function renderList() {
  if (laps.size === 0) {
    lapListEl.innerHTML = '<div class="empty-hint">Waiting for packets…</div>';
    return;
  }

  const cards = [];
  for (const lapEntry of laps.values()) {
    const presentSrcs = srcOrder.filter(s => lapEntry.bySource.has(s));
    const anyBucket = lapEntry.bySource.get(presentSrcs[0]) || {};
    const label = lapLabel(lapEntry.lap,
      (anyBucket.master || anyBucket.slave || anyBucket.unknown || {}).uap,
      lapEntry.name);

    // One row per source, with the source's colored marker. Rows are
    // aligned so the rssi / uap / cfo columns can be scanned across
    // sources like a tiny table.
    const rows = [];
    const snaps = []; // strongest bucket per source, for the compare line
    for (const src of presentSrcs) {
      const entry = lapEntry.bySource.get(src);
      const color = srcColor(src);
      const m = entry.master, s = entry.slave;
      if (m) {
        rows.push(`<div class="row"><span><i class="dot" style="background:${color}"></i>${src} master</span>`
          + `<span class="val">${m.rssiEma.toFixed(1)} dBm <small>(raw ${m.rssi.toFixed(1)})</small></span></div>`);
        rows.push(`<div class="row"><span></span><span class="val">uap ${hexUap(m.uap)} · cfo ${m.cfo.toFixed(0)} Hz</span></div>`);
      }
      if (s) {
        rows.push(`<div class="row"><span><i class="dot" style="background:${color}"></i>${src} slave</span>`
          + `<span class="val">${s.rssiEma.toFixed(1)} dBm <small>(raw ${s.rssi.toFixed(1)})</small></span></div>`);
        rows.push(`<div class="row"><span></span><span class="val">uap ${hexUap(s.uap)} · cfo ${s.cfo.toFixed(0)} Hz</span></div>`);
      }
      if (!m && !s && entry.unknown) {
        rows.push(`<div class="row"><span><i class="dot" style="background:${color}"></i>${src}</span>`
          + `<span class="val">${entry.unknown.rssiEma.toFixed(1)} dBm <small>(raw ${entry.unknown.rssi.toFixed(1)})</small></span></div>`);
      }
      const best = m || s || entry.unknown;
      if (best) snaps.push({ src, rssi: best.rssiEma, cfo: best.cfo, uap: best.uap, hasCfo: !!(m || s) });
    }

    // Comparison block: first two sources side by side. (Extend here for
    // >2 sources if ever needed.)
    let compare = "";
    if (snaps.length >= 2) {
      const a = snaps[0], b = snaps[1];
      const dRssi = (a.rssi - b.rssi);
      const uapTxt = (a.uap === b.uap)
        ? `uap match (${hexUap(a.uap)})`
        : `uap MISMATCH ${hexUap(a.uap)} vs ${hexUap(b.uap)}`;
      let cfoTxt = "";
      if (a.hasCfo && b.hasCfo) {
        cfoTxt = ` · Δcfo ${Math.abs(a.cfo - b.cfo).toFixed(0)} Hz`;
      }
      compare = `<div class="compare">Δrssi ${a.src}−${b.src}: ${dRssi >= 0 ? "+" : ""}${dRssi.toFixed(1)} dB · ${uapTxt}${cfoTxt}</div>`;
    }

    cards.push(`<div class="lap-card"><div class="lap-id">${label}</div>${rows.join("")}${compare}</div>`);
  }
  lapListEl.innerHTML = cards.join("");
}

// ---- boot -----------------------------------------------------------------
connectWS();
requestAnimationFrame(render);
setInterval(renderList, 500);
