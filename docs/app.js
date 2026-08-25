// Drives the real reconstruction engine, compiled to wasm.
//
// This file fetches bytes and draws pixels. Every claim it puts on screen, the
// book, the spread, whether a window is trustworthy, comes back from the Rust
// in viewer/, which is the same code the recorder and the query layer run.

import init, { Viewer } from './pkg/tickvault_viewer.js';

const DEPTH = 25;               // levels a side to draw; enough to read at a glance
const DATA = './data';

const el = (id) => document.getElementById(id);
const plot = el('plot');
const ctx = plot.getContext('2d');

const state = {
  partitions: [],
  venue: null,
  viewer: null,
  messages: 0,
  loading: false,
};

const css = (name) => getComputedStyle(document.documentElement).getPropertyValue(name).trim();

function say(text, kind = '') {
  const b = el('banner');
  b.className = 'banner' + (kind ? ' ' + kind : '');
  b.textContent = text;
}

// Receipt instants are nanoseconds and arrive as decimal strings, because they
// are past 2^53 where a double stops representing every integer.
function stamp(ns) {
  const n = BigInt(ns);
  const ms = Number(n / 1000000n);
  const frac = (n % 1000000000n).toString().padStart(9, '0');
  const d = new Date(ms);
  const hh = String(d.getUTCHours()).padStart(2, '0');
  const mm = String(d.getUTCMinutes()).padStart(2, '0');
  const ss = String(d.getUTCSeconds()).padStart(2, '0');
  return `${hh}:${mm}:${ss}.${frac} UTC`;
}

const fmt = (v, dp = 2) =>
  v === null || v === undefined ? '-' : v.toLocaleString('en-US', {
    minimumFractionDigits: dp, maximumFractionDigits: dp,
  });

// ---------------------------------------------------------------- the chart

function draw(book) {
  const dpr = Math.min(window.devicePixelRatio || 1, 2);
  const cssW = plot.clientWidth || 1200;
  const cssH = Math.round(cssW * 0.42);
  plot.width = Math.round(cssW * dpr);
  plot.height = Math.round(cssH * dpr);
  plot.style.height = cssH + 'px';
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, cssW, cssH);

  const pad = { l: 62, r: 20, t: 18, b: 40 };
  const w = cssW - pad.l - pad.r;
  const h = cssH - pad.t - pad.b;

  const bids = book.bids || [];
  const asks = book.asks || [];
  if (!bids.length && !asks.length) {
    ctx.fillStyle = css('--faint');
    ctx.font = "15px 'Times New Roman', serif";
    ctx.textAlign = 'center';
    ctx.fillText('no book here yet', cssW / 2, cssH / 2);
    return;
  }

  // Cumulative depth outward from the middle. Both sides arrive best first, so
  // the running total grows as you move away from the spread.
  let run = 0;
  const bidPts = bids.map((l) => { run += l.q; return { p: l.p, c: run }; });
  run = 0;
  const askPts = asks.map((l) => { run += l.q; return { p: l.p, c: run }; });

  const prices = [...bidPts, ...askPts].map((d) => d.p);
  const maxQ = Math.max(1e-9, ...bidPts.map((d) => d.c), ...askPts.map((d) => d.c));
  let lo = Math.min(...prices);
  let hi = Math.max(...prices);
  if (hi - lo < 1e-9) { lo -= 1; hi += 1; }
  const padX = (hi - lo) * 0.04;
  lo -= padX; hi += padX;

  const X = (p) => pad.l + ((p - lo) / (hi - lo)) * w;
  const Y = (q) => pad.t + h - (q / maxQ) * h;

  // axes
  ctx.strokeStyle = css('--hair');
  ctx.lineWidth = 1;
  ctx.beginPath();
  ctx.moveTo(pad.l, pad.t); ctx.lineTo(pad.l, pad.t + h); ctx.lineTo(pad.l + w, pad.t + h);
  ctx.stroke();

  ctx.fillStyle = css('--faint');
  ctx.font = "11px 'Courier New', monospace";
  ctx.textAlign = 'right';
  for (let i = 0; i <= 4; i++) {
    const q = (maxQ * i) / 4;
    const y = Y(q);
    ctx.fillText(q.toFixed(q < 10 ? 2 : 0), pad.l - 8, y + 3);
    if (i > 0) {
      ctx.strokeStyle = '#e8e3d6';
      ctx.beginPath(); ctx.moveTo(pad.l, y); ctx.lineTo(pad.l + w, y); ctx.stroke();
    }
  }
  ctx.textAlign = 'center';
  for (let i = 0; i <= 4; i++) {
    const p = lo + ((hi - lo) * i) / 4;
    ctx.fillText(p.toFixed(1), X(p), pad.t + h + 16);
  }

  // The spread, shaded and labelled, because it is the first thing to notice
  // and on a liquid pair it is too narrow to see unaided.
  if (bids.length && asks.length) {
    const a = X(bids[0].p), b = X(asks[0].p);
    const width = Math.max(b - a, 2);
    ctx.fillStyle = 'rgba(36,64,94,.14)';
    ctx.fillRect(a, pad.t, width, h);
    ctx.strokeStyle = 'rgba(36,64,94,.35)';
    ctx.lineWidth = 1;
    ctx.beginPath();
    ctx.moveTo(a, pad.t); ctx.lineTo(a, pad.t + h);
    ctx.moveTo(a + width, pad.t); ctx.lineTo(a + width, pad.t + h);
    ctx.stroke();
    ctx.fillStyle = css('--ox');
    ctx.font = "11px 'Courier New', monospace";
    ctx.textAlign = 'center';
    ctx.fillText('spread', a + width / 2, pad.t + h + 30);
  }

  // A depth chart is not symmetric. Both sides arrive best first, so bids
  // descend in price while asks ascend, and the running total belongs to a
  // different side of each price on each half: left of it for bids, right of it
  // for asks. Drawing them the same way strands the bid wall away from its own
  // prices, which looks plausible and is wrong.
  const side = (pts, colour, dashed, toward) => {
    if (!pts.length) return;
    const asc = toward === 'left' ? [...pts].reverse() : pts;
    const edgeL = pad.l, edgeR = pad.l + w;

    ctx.save();
    ctx.beginPath();
    if (toward === 'left') {
      // value of step i covers (x[i-1], x[i]]
      ctx.moveTo(edgeL, Y(asc[0].c));
      for (let i = 0; i < asc.length; i++) {
        ctx.lineTo(X(asc[i].p), Y(asc[i].c));
        if (i + 1 < asc.length) ctx.lineTo(X(asc[i].p), Y(asc[i + 1].c));
      }
    } else {
      // value of step i covers [x[i], x[i+1])
      ctx.moveTo(X(asc[0].p), Y(asc[0].c));
      for (let i = 1; i < asc.length; i++) {
        ctx.lineTo(X(asc[i].p), Y(asc[i - 1].c));
        ctx.lineTo(X(asc[i].p), Y(asc[i].c));
      }
      ctx.lineTo(edgeR, Y(asc[asc.length - 1].c));
    }
    ctx.strokeStyle = colour;
    ctx.lineWidth = 1.6;
    ctx.setLineDash(dashed ? [5, 3] : []);
    ctx.stroke();

    // close down to the axis for the fill, without drawing that edge
    ctx.lineTo(toward === 'left' ? X(asc[asc.length - 1].p) : edgeR, pad.t + h);
    ctx.lineTo(toward === 'left' ? edgeL : X(asc[0].p), pad.t + h);
    ctx.closePath();
    ctx.setLineDash([]);
    ctx.globalAlpha = 0.13;
    ctx.fillStyle = colour;
    ctx.fill();
    ctx.restore();
  };

  side(bidPts, css('--ok'), false, 'left');
  side(askPts, css('--bad'), true, 'right');

  // mid line
  if (book.mid !== null && book.mid !== undefined) {
    const x = X(book.mid);
    ctx.save();
    ctx.strokeStyle = css('--ox');
    ctx.setLineDash([2, 3]);
    ctx.beginPath(); ctx.moveTo(x, pad.t); ctx.lineTo(x, pad.t + h); ctx.stroke();
    ctx.restore();
    ctx.fillStyle = css('--ox');
    ctx.font = "11px 'Courier New', monospace";
    ctx.textAlign = 'center';
    ctx.fillText('mid', x, pad.t - 5);
  }

  ctx.font = "13px 'Times New Roman', serif";
  ctx.fillStyle = css('--sub');
  ctx.textAlign = 'left';
  ctx.fillText('buy orders waiting', pad.l + 6, pad.t + 14);
  ctx.textAlign = 'right';
  ctx.fillText('sell orders waiting', pad.l + w - 6, pad.t + 14);
  ctx.textAlign = 'center';
  ctx.fillStyle = css('--faint');
  ctx.font = "11px 'Courier New', monospace";
  ctx.fillText('price', pad.l + w / 2, cssH - 6);
  ctx.save();
  ctx.translate(14, pad.t + h / 2);
  ctx.rotate(-Math.PI / 2);
  ctx.fillText('cumulative quantity', 0, 0);
  ctx.restore();
}

// ---------------------------------------------------------------- rendering

function show(json) {
  const book = JSON.parse(json);
  draw(book);
  el('r-mid').textContent = fmt(book.mid);
  el('r-spread').textContent = book.spread === null ? 'no spread' : fmt(book.spread);
  el('r-levels').textContent = `${book.bid_levels} / ${book.ask_levels}`;
  el('r-msgs').textContent = book.messages.toLocaleString('en-US');
  el('r-at').textContent = stamp(book.at_ns);
  el('r-digest').textContent = '0x' + book.digest.toString(16).padStart(8, '0');

  if (book.suspect) {
    say(
      `${book.suspect_rows.toLocaleString('en-US')} rows in this rebuild fell inside a window ` +
      `the recorder could not vouch for. The book is shown anyway, labelled rather than smoothed.`,
      'suspect',
    );
  } else {
    const p = state.partitions.find((x) => x.venue === state.venue);
    const depth = p && p.feed_depth ? `, truncated to the feed's ${p.feed_depth} levels a side` : '';
    say(
      `Rebuilt from ${book.rows.toLocaleString('en-US')} archived rows${depth}. ` +
      `Nothing in this window was marked suspect.`,
    );
  }
}

function render() {
  if (!state.viewer) return;
  const n = Number(el('scrub').value);
  show(state.viewer.book_after(n, DEPTH));
}

// ---------------------------------------------------------------- loading

async function loadVenue(part) {
  if (state.loading) return;
  state.loading = true;
  state.venue = part.venue;
  for (const b of document.querySelectorAll('#venues button')) {
    b.setAttribute('aria-pressed', String(b.dataset.venue === part.venue));
    b.disabled = true;
  }
  el('scrub').disabled = true;
  say(`Fetching ${(part.bytes / 1e6).toFixed(2)} MB of Parquet for ${part.venue}...`);
  el('cap-what').textContent = `${part.venue} ${part.symbol}`;
  el('cap-where').textContent = `${part.rows.toLocaleString('en-US')} rows over ${part.seconds}s`;

  try {
    const viewer = new Viewer(part.symbol, part.book_level, part.feed_depth ?? undefined);
    for (const path of part.files) {
      const res = await fetch(`${DATA}/${part.venue}/${path}`);
      if (!res.ok) throw new Error(`${path}: HTTP ${res.status}`);
      viewer.add_file(new Uint8Array(await res.arrayBuffer()));
    }
    viewer.seal();

    if (state.viewer) state.viewer.free();
    state.viewer = viewer;
    state.messages = viewer.messages();

    const scrub = el('scrub');
    scrub.max = String(state.messages);
    scrub.value = String(state.messages);
    scrub.disabled = false;
    render();
  } catch (err) {
    say(`Could not load ${part.venue}: ${err}`, 'err');
  } finally {
    for (const b of document.querySelectorAll('#venues button')) b.disabled = false;
    state.loading = false;
  }
}

async function loadCoverage() {
  try {
    const res = await fetch(`${DATA}/coverage.json`);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const cov = await res.json();
    const table = el('cov');
    table.innerHTML = '';
    for (const s of cov.series) {
      const tr = document.createElement('tr');
      const th = document.createElement('th');
      th.textContent = `${s.venue} ${s.symbol}`;
      tr.appendChild(th);
      for (const b of s.buckets) {
        const td = document.createElement('td');
        const i = document.createElement('i');
        i.className = 'cell ' + b.state;
        i.title =
          `${s.venue} ${stamp(b.at_ns)}\n${b.state}\n` +
          `${b.messages.toLocaleString('en-US')} messages, ${b.suspect_rows} suspect rows`;
        td.appendChild(i);
        tr.appendChild(td);
      }
      table.appendChild(tr);
    }
    const mins = Math.round((cov.bucket_seconds / 60) * 10) / 10;
    el('cov-cap').textContent = `${cov.series.length} venues, ${mins} minute buckets`;
    el('cov-span').textContent = `${stamp(cov.from_ns)} to ${stamp(cov.to_ns)}`;
  } catch (err) {
    el('cov-cap').textContent = `coverage unavailable: ${err}`;
  }
}

// ---------------------------------------------------------------- start

async function main() {
  await init();

  const res = await fetch(`${DATA}/index.json`);
  if (!res.ok) {
    say(`No dataset alongside this page (HTTP ${res.status}).`, 'err');
    return;
  }
  state.partitions = (await res.json()).partitions;

  const bar = el('venues');
  for (const p of state.partitions) {
    const b = document.createElement('button');
    b.textContent = p.venue;
    b.dataset.venue = p.venue;
    b.setAttribute('aria-pressed', 'false');
    b.addEventListener('click', () => loadVenue(p));
    bar.appendChild(b);
  }

  el('scrub').addEventListener('input', render);
  el('back').addEventListener('click', () => {
    const s = el('scrub');
    s.value = String(Math.max(0, Number(s.value) - 1));
    render();
  });
  el('fwd').addEventListener('click', () => {
    const s = el('scrub');
    s.value = String(Math.min(state.messages, Number(s.value) + 1));
    render();
  });
  window.addEventListener('resize', () => { if (state.viewer) render(); });

  loadCoverage();

  // Kraken first: ten levels a side is the most legible book of the six, and
  // its checksum makes it the one that can prove the most about itself.
  const first = state.partitions.find((p) => p.venue === 'kraken') || state.partitions[0];
  if (first) await loadVenue(first);
}

main().catch((err) => say(`The engine failed to start: ${err}`, 'err'));
