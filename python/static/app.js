const ws = new WebSocket(`ws://${location.host}/ws`);
const laps = {};
const channels = [];
const channelState = Array(40).fill(null);
let selectedLap = null;
let selectedUap = null;

const chanDiv = document.getElementById("channels");
for (let i = 0; i < 40; i++) {
  const d = document.createElement("div");
  d.className = "channel";
  chanDiv.appendChild(d);
  channels.push(d);
}


const RSSI_MIN = -100;
const RSSI_MAX = -20;

function rssiToHeight(rssi, maxHeight) {
    rssi = Math.max(RSSI_MIN, Math.min(RSSI_MAX, rssi));
    return ((rssi - RSSI_MIN) / (RSSI_MAX - RSSI_MIN)) * maxHeight;
}
function rssiToPercent(rssi) {
  rssi = Math.max(RSSI_MIN, Math.min(RSSI_MAX, rssi));
  return ((rssi - RSSI_MIN) / (RSSI_MAX - RSSI_MIN)) * 100;
}
function drawRssiScale(ctx, height) {
    ctx.fillStyle = "#888";
    ctx.font = "12px monospace";

    for (let db = -100; db <= -20; db += 20) {
        const y = height - rssiToHeight(db, height);
        ctx.fillText(db + " dBm", 5, y);
        ctx.beginPath();
        ctx.moveTo(60, y);
        ctx.lineTo(ctx.canvas.width, y);
        ctx.strokeStyle = "#222";
        ctx.stroke();
    }
}

ws.onmessage = ev => {
  const msg = JSON.parse(ev.data);

  if (msg.type === "init") {
    msg.laps.forEach(updateLap);
  }

  if (msg.type === "packet") {
    updateLap(msg);
    updateChannel(msg);
    if (selectedLap === msg.lap && selectedUap === msg.uap) {
      updateRssiGauge(msg.rssi);
    }
  }
};

function updateRssiGauge(rssi) {
  const percent = rssiToPercent(rssi);
  const gauge = document.getElementById("rssi-gauge");
  const needle = document.getElementById("rssi-needle");
  const label = document.getElementById("rssi-label");

  needle.style.bottom = `${percent}%`;
  label.textContent = `${rssi} dBm`;
}


function selectLap(lap) {
  const dev = laps[lap];
  if (!dev) return;

  selectedLap = dev.lap;
  selectedUap = dev.uap;

  document.getElementById("rssi-label").textContent = "Selected";
}

function deviceColor(lap, uap) {
  const key = ((lap & 0xFFFFFF) << 8) | (uap & 0xFF);
  const hue = (key * 53) % 360;
  return `hsl(${hue}, 85%, 55%)`;
}

function formatUAP(uap) {
    if (uap === 0xFF) return "??";
    return "0x" + uap.toString(16).toUpperCase().padStart(2, "0");
}
function updateLap(msg) {
  laps[msg.lap] = msg;

  let row = document.getElementById("lap-" + msg.lap);
  const rowColor = deviceColor(msg.lap, msg.uap);

  if (!row) {
    row = document.createElement("tr");
    row.id = "lap-" + msg.lap;
    row.onclick = () => selectLap(msg.lap);
    row.style.color = rowColor;
    row.classList.add("active");
    row.style.borderBottom = `2px solid ${rowColor}`;

    setTimeout(() => {
      row.classList.remove("active");
      row.style.borderBottom = "";
    }, 500);
    document.getElementById("lap-table").appendChild(row);
  }


  row.innerHTML = `
    <td>${msg.lap.toString(16).toUpperCase().padStart(6, "0")}</td>
    <td>${formatUAP(msg.uap)}</td>
    <td>${msg.rssi}</td>
    <td>${msg.count}</td>`;
}

function renderChannels() {
  const now = Date.now();

  for (let ch = 0; ch < 40; ch++) {
    const c = channels[ch];
    c.innerHTML = "";

    const s = channelState[ch];
    if (!s) continue;

    // decay after 800 ms
    if (now - s.time > 1500) {
      channelState[ch] = null;
      continue;
    }

    const b = document.createElement("div");
    b.className = "bar";
    b.style.height = `${rssiToPercent(s.rssi)}%`;
    //b.style.background = lapColor(s.lap);
    b.style.background = deviceColor(s.lap, s.uap);
    c.appendChild(b);
  }
}
function lapColor(lap) {
  const hue = (lap * 47) % 360;
  return `hsl(${hue}, 80%, 50%)`;
}

setInterval(renderChannels, 100);
function updateChannel(msg) {
  channelState[msg.channel] = {
    rssi: msg.rssi,
    lap: msg.lap,
    time: Date.now()
  };
  const c = channels[msg.channel];
  c.innerHTML = "";
  const b = document.createElement("div");
  b.style.height = `${rssiToPercent(msg.rssi)}%`;
  b.className = "bar";
  //b.style.height = `${msg.rssi + 100}%`;
  b.style.background = "red";
  c.appendChild(b);
}

function selectLap(lap) {
  console.log("Selected", lap);
}
