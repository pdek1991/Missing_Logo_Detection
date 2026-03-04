const REFRESH_INTERVAL_MS = 30_000;
const API_ENDPOINT = "/api/logo_status";
const STATUS_PRIORITY = {
  RED: 0,
  YELLOW: 1,
  GREEN: 2,
};

const gridElement = document.getElementById("channelGrid");
const lastUpdatedElement = document.getElementById("lastUpdated");
const countRedElement = document.getElementById("countRed");
const countYellowElement = document.getElementById("countYellow");
const countGreenElement = document.getElementById("countGreen");
const redBadgeElement = countRedElement.closest(".badge");
const yellowBadgeElement = countYellowElement.closest(".badge");
const greenBadgeElement = countGreenElement.closest(".badge");

const alertBackdrop = document.getElementById("alertBackdrop");
const alertChannelElement = document.getElementById("alertChannel");
const alertTimeElement = document.getElementById("alertTime");
const ackButton = document.getElementById("ackButton");
const alarmAudio = document.getElementById("alarmAudio");

const previousStatusByChannel = new Map();
const redAlertQueue = [];
let isModalOpen = false;

ackButton.addEventListener("click", acknowledgeAlert);

window.addEventListener("load", () => {
  registerServiceWorker();
  refreshDashboard();
  setInterval(refreshDashboard, REFRESH_INTERVAL_MS);
});

async function refreshDashboard() {
  try {
    const response = await fetch(API_ENDPOINT, { cache: "no-store" });
    if (!response.ok) {
      throw new Error(`API error ${response.status}`);
    }

    const payload = await response.json();
    const rows = Array.isArray(payload) ? payload.map(normalizeRow) : [];
    rows.sort(sortByStatusThenName);

    processAlarmTransitions(rows);
    renderGrid(rows);

    const now = formatClockTime(new Date());
    lastUpdatedElement.textContent = `Last Updated: ${now}`;
  } catch (error) {
    console.error("Dashboard refresh failed:", error);
    const now = formatClockTime(new Date());
    lastUpdatedElement.textContent = `Last Updated: ${now}`;
  }
}

function normalizeRow(row) {
  const status = ["RED", "YELLOW", "GREEN"].includes(row.status) ? row.status : "YELLOW";

  const confidenceRaw = typeof row.confidence === "number" ? row.confidence : row.detection_confidence;
  const confidence = typeof confidenceRaw === "number" ? confidenceRaw : null;

  return {
    channel: String(row.channel || "UNKNOWN"),
    logo: row.logo || "",
    status,
    lastChecked: String(row.last_checked || "--:--:--"),
    confidence,
    lastDetectionTime: String(row.last_detection_time || "N/A"),
    rawStatus: String(row.raw_status || status),
    error: row.error ? String(row.error) : "",
  };
}

function sortByStatusThenName(a, b) {
  const byStatus = STATUS_PRIORITY[a.status] - STATUS_PRIORITY[b.status];
  if (byStatus !== 0) {
    return byStatus;
  }
  return a.channel.localeCompare(b.channel);
}

function processAlarmTransitions(rows) {
  let shouldPlayAlarm = false;

  rows.forEach((row) => {
    const previousStatus = previousStatusByChannel.get(row.channel);
    const wasAlarm = previousStatus === "RED" || previousStatus === "YELLOW";
    const isAlarm = row.status === "RED" || row.status === "YELLOW";
    const escalatedToRed = row.status === "RED" && previousStatus !== "RED";

    if ((isAlarm && !wasAlarm) || escalatedToRed) {
      shouldPlayAlarm = true;
    }

    if (row.status === "RED" && previousStatus !== "RED") {
      redAlertQueue.push({
        channel: row.channel,
        time: row.lastChecked,
      });
    }

    previousStatusByChannel.set(row.channel, row.status);
  });

  if (shouldPlayAlarm) {
    playAlarmOnce();
  }

  showNextAlertIfNeeded();
}

function renderGrid(rows) {
  const counts = { RED: 0, YELLOW: 0, GREEN: 0 };
  const fragment = document.createDocumentFragment();

  rows.forEach((row) => {
    counts[row.status] += 1;

    const card = document.createElement("article");
    card.className = `channel-card status-${row.status.toLowerCase()}`;
    card.dataset.tooltip = buildTooltipText(row);

    const confidenceText = row.confidence === null ? "N/A" : `${(row.confidence * 100).toFixed(1)}%`;

    const logoSource = row.logo || "/assets/icons/icon-192x192.png";

    card.innerHTML = `
      <img class="channel-logo" src="${escapeHtml(logoSource)}" alt="${escapeHtml(row.channel)} logo" loading="lazy" />
      <div class="channel-meta">
        <p class="channel-name">${escapeHtml(row.channel)}</p>
        <p class="channel-status">Status: ${row.status} (${confidenceText})</p>
        <p class="channel-time">Checked: ${escapeHtml(row.lastChecked)}</p>
      </div>
    `;

    fragment.appendChild(card);
  });

  gridElement.replaceChildren(fragment);
  countRedElement.textContent = String(counts.RED);
  countYellowElement.textContent = String(counts.YELLOW);
  countGreenElement.textContent = String(counts.GREEN);

  updateAlarmFlashing(counts);
}

function updateAlarmFlashing(counts) {
  const hasAlarm = counts.RED > 0 || counts.YELLOW > 0;
  document.body.classList.toggle("alarm-active", hasAlarm);

  redBadgeElement.classList.toggle("flash-red", counts.RED > 0);
  yellowBadgeElement.classList.toggle("flash-yellow", counts.YELLOW > 0);
  greenBadgeElement.classList.remove("flash-green");
}

function buildTooltipText(row) {
  const confidence = row.confidence === null ? "N/A" : `${(row.confidence * 100).toFixed(2)}%`;
  return `Channel Name: ${row.channel}\nDetection Confidence: ${confidence}\nLast Detection Time: ${row.lastDetectionTime}`;
}

function playAlarmOnce() {
  if (!alarmAudio) {
    return;
  }

  alarmAudio.currentTime = 0;
  alarmAudio.play().catch(() => {
    // Browser autoplay policies may block this until user interacts with the page.
  });
}

function showNextAlertIfNeeded() {
  if (isModalOpen || redAlertQueue.length === 0) {
    return;
  }

  const nextAlert = redAlertQueue.shift();
  alertChannelElement.textContent = `Channel: ${nextAlert.channel}`;
  alertTimeElement.textContent = `Time: ${nextAlert.time}`;
  alertBackdrop.classList.remove("hidden");
  isModalOpen = true;
}

function acknowledgeAlert() {
  alertBackdrop.classList.add("hidden");
  isModalOpen = false;
  showNextAlertIfNeeded();
}

function registerServiceWorker() {
  if (!("serviceWorker" in navigator)) {
    return;
  }

  navigator.serviceWorker
    .register("/service-worker.js")
    .catch((error) => console.error("Service worker registration failed:", error));
}

function formatClockTime(date) {
  return date.toLocaleTimeString("en-GB", { hour12: false });
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}
