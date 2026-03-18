const channelGrid = document.getElementById('channelGrid');
const lastUpdated = document.getElementById('lastUpdated');
const summaryBadges = document.getElementById('summaryBadges');
const countRed = document.getElementById('countRed');
const countYellow = document.getElementById('countYellow');
const countGreen = document.getElementById('countGreen');
const countWhite = document.getElementById('countWhite');

const alarmPopup = document.getElementById('alarmPopup');
const alarmChannel = document.getElementById('alarmChannel');
const alarmStatus = document.getElementById('alarmStatus');
const alarmTime = document.getElementById('alarmTime');
const alarmAckButton = document.getElementById('alarmAckButton');
const snoozeControls = document.getElementById('snoozeControls');
const snoozeRange = document.getElementById('snoozeRange');
const snoozeValue = document.getElementById('snoozeValue');
const snoozeButton = document.getElementById('snoozeButton');
const alarmAudio = document.getElementById('alarmAudio');

const MAX_GRID_SLOTS = 50;
const FALLBACK_POLL_MS = 1000;
const SSE_URL = '/api/logo_status/stream';
const SNOOZE_STORAGE_KEY = 'noc_stream_issue_snooze_v1';
const CACHE_PURGE_FLAG = 'noc_cache_purge_done_v1';

let fetchInFlight = false;
let eventSource = null;
let reconnectTimer = null;
let reconnectDelayMs = 1000;
let pollingTimer = null;
let lastRenderKey = '';

let activeRedChannels = new Set();
let redRowsByChannel = new Map();
let acknowledgedRedChannels = new Set();
let currentPopupChannel = null;
const snoozedUntilByChannel = new Map();

const cardByChannel = new Map();
const rowSignatureByChannel = new Map();

async function purgeLegacyClientCaches() {
  try {
    if (localStorage.getItem(CACHE_PURGE_FLAG) === '1') {
      return;
    }
  } catch {
    // Ignore storage issues.
  }

  try {
    if ('serviceWorker' in navigator) {
      const registrations = await navigator.serviceWorker.getRegistrations();
      await Promise.all(registrations.map((registration) => registration.unregister()));
    }
  } catch {
    // Ignore service worker errors.
  }

  try {
    if ('caches' in window) {
      const keys = await caches.keys();
      await Promise.all(keys.map((key) => caches.delete(key)));
    }
  } catch {
    // Ignore cache storage errors.
  }

  try {
    localStorage.setItem(CACHE_PURGE_FLAG, '1');
  } catch {
    // Ignore storage issues.
  }
}

function escapeHtml(value) {
  return String(value || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/\"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function parseStatus(raw) {
  const normalized = String(raw || '').toUpperCase();
  if (normalized === 'RED' || normalized === 'YELLOW' || normalized === 'WHITE' || normalized === 'GREEN') {
    return normalized;
  }
  return 'YELLOW';
}

function formatTime(value) {
  const date = value ? new Date(value) : new Date();
  if (Number.isNaN(date.getTime())) {
    return '--:--:--';
  }
  return date.toLocaleTimeString('en-US', { hour12: false });
}

function formatStability(value) {
  const number = Number(value);
  if (!Number.isFinite(number)) {
    return '--';
  }
  return `${number.toFixed(1)}%`;
}

function formatDurationMinutes(totalMinutes) {
  const minutes = Math.max(0, Math.round(totalMinutes));
  const days = Math.floor(minutes / (24 * 60));
  const remAfterDays = minutes - (days * 24 * 60);
  const hours = Math.floor(remAfterDays / 60);
  const mins = remAfterDays % 60;

  const parts = [];
  if (days > 0) parts.push(`${days}d`);
  if (hours > 0 || days > 0) parts.push(`${hours}h`);
  parts.push(`${mins}m`);
  return parts.join(' ');
}

function updateSnoozeValueLabel() {
  snoozeValue.textContent = `Duration: ${formatDurationMinutes(Number(snoozeRange.value || 0))}`;
}

function loadSnoozes() {
  try {
    const raw = localStorage.getItem(SNOOZE_STORAGE_KEY);
    if (!raw) {
      return;
    }
    const parsed = JSON.parse(raw);
    const now = Date.now();
    Object.keys(parsed || {}).forEach((channel) => {
      const until = Number(parsed[channel]);
      if (Number.isFinite(until) && until > now) {
        snoozedUntilByChannel.set(channel, until);
      }
    });
  } catch {
    // Ignore malformed storage.
  }
}

function persistSnoozes() {
  const now = Date.now();
  const payload = {};
  Array.from(snoozedUntilByChannel.entries()).forEach(([channel, until]) => {
    if (until > now) {
      payload[channel] = until;
    }
  });
  try {
    localStorage.setItem(SNOOZE_STORAGE_KEY, JSON.stringify(payload));
  } catch {
    // Ignore storage failures.
  }
}

function cleanupExpiredSnoozes() {
  const now = Date.now();
  let changed = false;
  Array.from(snoozedUntilByChannel.entries()).forEach(([channel, until]) => {
    if (until <= now) {
      snoozedUntilByChannel.delete(channel);
      changed = true;
    }
  });
  if (changed) {
    persistSnoozes();
  }
}

function isStreamIssue(row) {
  const normalized = String(row?.raw_status || '').toUpperCase().replace(/_/g, ' ').trim();
  return (
    normalized.startsWith('STREAM ') ||
    normalized === 'NO PACKETS RECEIVED' ||
    normalized === 'FROZEN FRAME' ||
    normalized === 'STREAM DOWN'
  );
}

function statusClass(status, row = null) {
  if (status === 'RED') {
    return isStreamIssue(row) ? 'status-red-stream' : 'status-red-missing';
  }
  if (status === 'YELLOW') return 'status-yellow';
  if (status === 'WHITE') return 'status-white';
  return 'status-green';
}

function rowSignature(row) {
  return [
    parseStatus(row.status),
    String(row.raw_status || ''),
    String(row.error || ''),
    String(row.logo || ''),
    typeof row.confidence === 'number' ? row.confidence.toFixed(3) : 'na',
  ].join('|');
}

function buildRenderKey(rows) {
  const limited = rows.slice(0, MAX_GRID_SLOTS);
  return limited
    .map((row) => `${String(row.channel || 'Unknown')}|${rowSignature(row)}`)
    .join('||');
}

function updateLastUpdated(rows) {
  let latestMs = 0;
  rows.forEach((row) => {
    const t = Date.parse(row.last_detection_time || '');
    if (Number.isFinite(t) && t > latestMs) {
      latestMs = t;
    }
  });
  lastUpdated.textContent = `Last Updated: ${formatTime(latestMs || Date.now())}`;
}

function updateCounters(rows) {
  let red = 0;
  let yellow = 0;
  let green = 0;
  let white = 0;

  rows.forEach((row) => {
    const status = parseStatus(row.status);
    if (status === 'RED') red += 1;
    else if (status === 'YELLOW') yellow += 1;
    else if (status === 'WHITE') white += 1;
    else green += 1;
  });

  countRed.textContent = red;
  countYellow.textContent = yellow;
  countGreen.textContent = green;
  countWhite.textContent = white;

  const shouldShow = red > 1 || yellow > 1 || white > 1;
  summaryBadges.classList.toggle('hidden', !shouldShow);
}

function buildTooltip(item) {
  const lines = [];
  if (item.raw_status) {
    lines.push(`State: ${item.raw_status}`);
  }
  if (item.error) {
    lines.push(`Error: ${item.error}`);
  }
  if (typeof item.confidence === 'number') {
    lines.push(`Score: ${item.confidence.toFixed(2)}`);
  }
  lines.push(`Checked: ${item.last_checked || '--:--:--'}`);
  return lines.join('\n');
}

function createCard(channel) {
  const card = document.createElement('article');
  card.className = 'channel-card';

  const img = document.createElement('img');
  img.className = 'channel-logo';
  img.alt = channel;
  img.onerror = () => {
    img.src = '/favicon.ico';
  };

  const meta = document.createElement('div');
  meta.className = 'channel-meta';

  const name = document.createElement('p');
  name.className = 'channel-name';

  const status = document.createElement('p');
  status.className = 'channel-status';

  const foot = document.createElement('div');
  foot.className = 'channel-foot';

  const time = document.createElement('p');
  time.className = 'channel-time';

  const confidence = document.createElement('p');
  confidence.className = 'channel-confidence';

  foot.appendChild(time);
  foot.appendChild(confidence);

  meta.appendChild(name);
  meta.appendChild(status);
  meta.appendChild(foot);

  card.appendChild(img);
  card.appendChild(meta);

  card._refs = { img, name, status, time, confidence };
  return card;
}

function updateCard(card, row) {
  const refs = card._refs;
  const status = parseStatus(row.status);

  card.className = `channel-card ${statusClass(status, row)}`;
  card.setAttribute('data-tooltip', buildTooltip(row));

  const channelName = String(row.channel || 'Unknown');
  if (refs.name.textContent !== channelName) {
    refs.name.textContent = channelName;
  }

  const rawStatus = String(row.raw_status || status);
  if (refs.status.textContent !== rawStatus) {
    refs.status.textContent = rawStatus;
  }

  const checked = String(row.last_checked || '--:--:--');
  if (refs.time.textContent !== checked) {
    refs.time.textContent = checked;
  }

  const confidenceText = typeof row.confidence === 'number'
    ? `Score ${row.confidence.toFixed(2)}`
    : 'Score --';
  if (refs.confidence.textContent !== confidenceText) {
    refs.confidence.textContent = confidenceText;
  }

  const nextLogo = row.logo || '/favicon.ico';
  if (refs.img.src !== new URL(nextLogo, window.location.origin).href) {
    refs.img.src = nextLogo;
  }
}

function renderChannels(rows) {
  const fragment = document.createDocumentFragment();
  const channels = rows.slice(0, MAX_GRID_SLOTS);
  const seen = new Set();

  channels.forEach((row) => {
    const channel = String(row.channel || 'Unknown');
    seen.add(channel);

    let card = cardByChannel.get(channel);
    if (!card) {
      card = createCard(channel);
      cardByChannel.set(channel, card);
    }

    const nextSignature = rowSignature(row);
    const prevSignature = rowSignatureByChannel.get(channel);

    if (nextSignature !== prevSignature) {
      updateCard(card, row);
      rowSignatureByChannel.set(channel, nextSignature);
    } else {
      // Time can change without status changes; keep this field current.
      const checked = String(row.last_checked || '--:--:--');
      if (card._refs.time.textContent !== checked) {
        card._refs.time.textContent = checked;
      }
      card.setAttribute('data-tooltip', buildTooltip(row));
    }

    fragment.appendChild(card);
  });

  Array.from(cardByChannel.keys()).forEach((channel) => {
    if (!seen.has(channel)) {
      cardByChannel.delete(channel);
      rowSignatureByChannel.delete(channel);
    }
  });

  const placeholders = Math.max(0, MAX_GRID_SLOTS - channels.length);
  for (let i = 0; i < placeholders; i += 1) {
    const card = document.createElement('article');
    card.className = 'channel-card placeholder';
    card.innerHTML = '<div class="channel-meta"><p class="channel-name">&nbsp;</p></div>';
    fragment.appendChild(card);
  }

  channelGrid.replaceChildren(fragment);
}

function stopAlarmAudio() {
  alarmAudio.pause();
  alarmAudio.currentTime = 0;
}

function refreshAlarmAudio() {
  if (alarmPopup.classList.contains('hidden')) {
    stopAlarmAudio();
    return;
  }

  const playPromise = alarmAudio.play();
  if (playPromise !== undefined) {
    playPromise.catch(() => {
      // Browser may require user interaction before autoplay.
    });
  }
}

function updateAlarmPopup() {
  const pendingChannels = Array.from(activeRedChannels)
    .filter((channel) => !acknowledgedRedChannels.has(channel))
    .filter((channel) => {
      const until = Number(snoozedUntilByChannel.get(channel) || 0);
      return !(until > Date.now());
    })
    .sort((a, b) => a.localeCompare(b));

  if (pendingChannels.length === 0) {
    currentPopupChannel = null;
    alarmPopup.classList.add('hidden');
    refreshAlarmAudio();
    return;
  }

  if (!currentPopupChannel || !pendingChannels.includes(currentPopupChannel)) {
    currentPopupChannel = pendingChannels[0];
  }

  const row = redRowsByChannel.get(currentPopupChannel);
  if (!row) {
    alarmPopup.classList.add('hidden');
    refreshAlarmAudio();
    return;
  }

  alarmChannel.textContent = `Channel: ${currentPopupChannel}`;
  alarmStatus.textContent = `Status: ${row.raw_status || 'MISSING DETECTED'}`;
  alarmTime.textContent = `Detected: ${formatTime(row.last_detection_time || Date.now())}`;
  alarmPopup.classList.remove('popup-missing', 'popup-stream');

  if (isStreamIssue(row)) {
    snoozeControls.classList.remove('hidden');
    alarmPopup.classList.add('popup-stream');
  } else {
    snoozeControls.classList.add('hidden');
    alarmPopup.classList.add('popup-missing');
  }

  alarmPopup.classList.remove('hidden');
  refreshAlarmAudio();
}

function syncRedState(rows) {
  const nextRedChannels = new Set();
  const nextRedMap = new Map();

  rows.forEach((row) => {
    if (parseStatus(row.status) !== 'RED') {
      return;
    }

    const channel = String(row.channel || 'Unknown');
    nextRedChannels.add(channel);
    nextRedMap.set(channel, row);
  });

  Array.from(acknowledgedRedChannels).forEach((channel) => {
    if (!nextRedChannels.has(channel)) {
      acknowledgedRedChannels.delete(channel);
      snoozedUntilByChannel.delete(channel);
    }
  });

  activeRedChannels = nextRedChannels;
  redRowsByChannel = nextRedMap;
  persistSnoozes();
  updateAlarmPopup();
}

function applyRows(rows) {
  if (!Array.isArray(rows)) {
    return;
  }

  rows.sort((a, b) => {
    function getRank(row) {
      const status = parseStatus(row.status);
      if (status === 'RED') {
        return isStreamIssue(row) ? 2 : 1;
      }
      if (status === 'YELLOW') return 3;
      if (status === 'WHITE') return 4;
      return 5;
    }
    const rankA = getRank(a);
    const rankB = getRank(b);
    if (rankA !== rankB) {
      return rankA - rankB;
    }
    return String(a.channel || '').localeCompare(String(b.channel || ''));
  });

  const nextRenderKey = buildRenderKey(rows);
  if (nextRenderKey === lastRenderKey) {
    return;
  }
  lastRenderKey = nextRenderKey;

  updateLastUpdated(rows);
  updateCounters(rows);
  renderChannels(rows);
  syncRedState(rows);
}

async function fetchStatus() {
  if (fetchInFlight) {
    return;
  }

  fetchInFlight = true;
  try {
    const response = await fetch('/api/logo_status', { cache: 'no-store' });
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }

    const rows = await response.json();
    applyRows(rows);
  } catch (err) {
    console.error('Failed to fetch status:', err);
  } finally {
    fetchInFlight = false;
  }
}

function startPollingFallback() {
  if (pollingTimer !== null) {
    return;
  }
  pollingTimer = setInterval(fetchStatus, FALLBACK_POLL_MS);
}

function stopPollingFallback() {
  if (pollingTimer === null) {
    return;
  }
  clearInterval(pollingTimer);
  pollingTimer = null;
}

function scheduleReconnect() {
  if (reconnectTimer !== null) {
    return;
  }

  startPollingFallback();
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    connectSse();
  }, reconnectDelayMs);
  reconnectDelayMs = Math.min(15000, reconnectDelayMs * 2);
}

function connectSse() {
  if (typeof window.EventSource === 'undefined') {
    startPollingFallback();
    return;
  }

  if (eventSource !== null) {
    eventSource.close();
    eventSource = null;
  }

  eventSource = new EventSource(SSE_URL);

  eventSource.onopen = () => {
    reconnectDelayMs = 1000;
    stopPollingFallback();
  };

  eventSource.onmessage = (event) => {
    try {
      const rows = JSON.parse(event.data);
      applyRows(rows);
    } catch (err) {
      console.error('Invalid SSE payload:', err);
    }
  };

  eventSource.onerror = () => {
    if (eventSource !== null) {
      eventSource.close();
      eventSource = null;
    }
    scheduleReconnect();
  };
}

alarmAckButton.addEventListener('click', () => {
  if (currentPopupChannel) {
    acknowledgedRedChannels.add(currentPopupChannel);
    updateAlarmPopup();
  }
});

snoozeRange.addEventListener('input', updateSnoozeValueLabel);

snoozeButton.addEventListener('click', () => {
  if (!currentPopupChannel) {
    return;
  }
  const row = redRowsByChannel.get(currentPopupChannel);
  if (!isStreamIssue(row)) {
    return;
  }

  const minutes = Number(snoozeRange.value || 0);
  if (!Number.isFinite(minutes) || minutes <= 0) {
    return;
  }

  const until = Date.now() + (minutes * 60 * 1000);
  snoozedUntilByChannel.set(currentPopupChannel, until);
  persistSnoozes();
  updateAlarmPopup();
});

window.addEventListener('visibilitychange', () => {
  if (!document.hidden) {
    fetchStatus();
  }
});

async function bootstrap() {
  await purgeLegacyClientCaches();
  loadSnoozes();
  updateSnoozeValueLabel();
  setInterval(() => {
    cleanupExpiredSnoozes();
    updateAlarmPopup();
  }, 1000);
  fetchStatus();
  connectSse();
  startPollingFallback();

  try {
    const configRes = await fetch('/api/config');
    if (configRes.ok) {
      const config = await configRes.json();
      const intervalSec = Number(config.web_refresh_interval_seconds);
      if (Number.isFinite(intervalSec) && intervalSec > 0) {
        setTimeout(() => {
          window.location.reload();
        }, intervalSec * 1000);
      }
    }
  } catch (err) {
    console.error('Failed to init auto-refresh:', err);
  }
}

bootstrap();
