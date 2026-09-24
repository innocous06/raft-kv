const state = {
  nodes: {},
  clientId: "client-" + Math.floor(Math.random() * 100000),
  seqNum: 1
};

const nodesGrid = document.getElementById("nodesGrid");
const timelineList = document.getElementById("timelineList");
const kvTableBody = document.getElementById("kvTableBody");
const opResult = document.getElementById("opResult");
const currentLeaderText = document.getElementById("currentLeaderText");

function renderNodes() {
  nodesGrid.innerHTML = "";
  const nodeKeys = Object.keys(state.nodes).sort();

  if (nodeKeys.length === 0) {
    nodesGrid.innerHTML = `<div style="color: var(--text-muted); font-size: 0.8rem; font-family: var(--font-mono); padding: 12px;">Initializing node connections...</div>`;
    return;
  }

  let leaderFound = false;
  for (const id of nodeKeys) {
    const node = state.nodes[id];
    const roleLower = (node.role || "follower").toLowerCase();
    const isLeader = roleLower === "leader";

    if (isLeader) {
      leaderFound = true;
      currentLeaderText.textContent = id;
    }

    const card = document.createElement("div");
    card.className = `node-card ${roleLower}`;
    card.innerHTML = `
      <div class="node-header">
        <span class="node-title">${id}</span>
        <span class="role-pill ${roleLower}">${node.role || "OFFLINE"}</span>
      </div>
      <div class="node-metrics">
        <span><span>Term</span> <b>${node.term || 0}</b></span>
        <span><span>Commit Index</span> <b>${node.commitIndex || 0}</b></span>
        <span><span>Last Applied</span> <b>${node.lastApplied || 0}</b></span>
        <span><span>Log Length</span> <b>${node.logLength || 0}</b></span>
      </div>
    `;
    nodesGrid.appendChild(card);
  }

  if (!leaderFound) {
    currentLeaderText.textContent = "Electing...";
  }
}

function appendEvent(evt) {
  const item = document.createElement("div");
  item.className = `timeline-item ${evt.type}`;

  const timeStr = evt.timestamp ? new Date(evt.timestamp).toLocaleTimeString([], { hour12: false }) : new Date().toLocaleTimeString([], { hour12: false });
  const nodeId = evt.nodeId || "cluster";
  
  item.innerHTML = `
    <span class="item-time">${timeStr}</span>
    <span class="item-tag">${nodeId} &rsaquo; ${evt.type}</span>
    <span class="item-desc">${evt.details || ""}</span>
  `;

  timelineList.insertBefore(item, timelineList.firstChild);

  while (timelineList.children.length > 250) {
    timelineList.removeChild(timelineList.lastChild);
  }
}

function setupEventStream() {
  const sse = new EventSource("/api/v1/events/stream");

  sse.onmessage = (event) => {
    try {
      const evt = JSON.parse(event.data);
      appendEvent(evt);

      if (evt.nodeId && evt.nodeId !== "cluster") {
        if (!state.nodes[evt.nodeId]) {
          state.nodes[evt.nodeId] = { id: evt.nodeId };
        }
        if (evt.role) state.nodes[evt.nodeId].role = evt.role;
        if (evt.term) state.nodes[evt.nodeId].term = evt.term;
        renderNodes();
      }
    } catch (e) {
      console.error("SSE parse error:", e);
    }
  };

  sse.onerror = () => {
    console.warn("SSE connection retry...");
  };
}

async function fetchStatus() {
  try {
    const res = await fetch("/api/v1/cluster/status");
    if (res.ok) {
      const data = await res.json();
      if (Array.isArray(data)) {
        for (const item of data) {
          state.nodes[item.id] = item;
        }
      } else if (data && data.id) {
        state.nodes[data.id] = data;
      }
      renderNodes();
    }
  } catch (e) {}
}

async function fetchKV() {
  try {
    const res = await fetch("/api/v1/kv/all");
    if (res.ok) {
      const data = await res.json();
      kvTableBody.innerHTML = "";
      const keys = Object.keys(data).sort();
      if (keys.length === 0) {
        kvTableBody.innerHTML = `<tr><td colspan="3" style="text-align: center; color: var(--text-muted); padding: 18px;">No entries committed to state machine</td></tr>`;
        return;
      }
      for (const k of keys) {
        const row = document.createElement("tr");
        row.innerHTML = `
          <td><b>${k}</b></td>
          <td>${data[k]}</td>
          <td style="color: var(--text-muted); font-size: 0.72rem; text-transform: uppercase;">[Committed]</td>
        `;
        kvTableBody.appendChild(row);
      }
    }
  } catch (e) {}
}

// Client Operation Handlers
document.getElementById("btnPut").addEventListener("click", async () => {
  const key = document.getElementById("opKey").value.trim();
  const value = document.getElementById("opValue").value.trim();
  if (!key) return;

  opResult.textContent = "[PROCESSING] Submitting PUT command...";
  const start = performance.now();

  try {
    const res = await fetch("/api/v1/kv/put", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        key,
        value,
        clientId: state.clientId,
        seqNum: state.seqNum++
      })
    });
    const latency = Math.round(performance.now() - start);
    const data = await res.json();
    if (res.ok && data.success) {
      opResult.innerHTML = `<span style="color: var(--role-leader-text)">[SUCCESS] PUT '${key}' committed in ${latency}ms</span>`;
      fetchKV();
    } else {
      opResult.innerHTML = `<span style="color: var(--role-crashed-text)">[REJECTED] ${data.error || "Execution failed"} (${latency}ms)</span>`;
    }
  } catch (e) {
    opResult.innerHTML = `<span style="color: var(--role-crashed-text)">[ERROR] ${e.message}</span>`;
  }
});

document.getElementById("btnGet").addEventListener("click", async () => {
  const key = document.getElementById("opKey").value.trim();
  if (!key) return;

  opResult.textContent = "[PROCESSING] Submitting GET query...";
  const start = performance.now();

  try {
    const res = await fetch(`/api/v1/kv/get?key=${encodeURIComponent(key)}`);
    const latency = Math.round(performance.now() - start);
    const data = await res.json();
    if (res.ok && data.success) {
      document.getElementById("opValue").value = data.value;
      opResult.innerHTML = `<span style="color: var(--role-leader-text)">[SUCCESS] GET '${key}' = "${data.value}" in ${latency}ms</span>`;
    } else {
      opResult.innerHTML = `<span style="color: var(--role-candidate-text)">[NOT FOUND] ${data.error || "Key absent"} (${latency}ms)</span>`;
    }
  } catch (e) {
    opResult.innerHTML = `<span style="color: var(--role-crashed-text)">[ERROR] ${e.message}</span>`;
  }
});

document.getElementById("btnDelete").addEventListener("click", async () => {
  const key = document.getElementById("opKey").value.trim();
  if (!key) return;

  opResult.textContent = "[PROCESSING] Submitting DELETE command...";
  const start = performance.now();

  try {
    const res = await fetch("/api/v1/kv/delete", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        key,
        clientId: state.clientId,
        seqNum: state.seqNum++
      })
    });
    const latency = Math.round(performance.now() - start);
    const data = await res.json();
    if (res.ok && data.success) {
      opResult.innerHTML = `<span style="color: var(--role-leader-text)">[SUCCESS] DELETE '${key}' committed in ${latency}ms</span>`;
      fetchKV();
    } else {
      opResult.innerHTML = `<span style="color: var(--role-crashed-text)">[REJECTED] ${data.error || "Execution failed"}</span>`;
    }
  } catch (e) {
    opResult.innerHTML = `<span style="color: var(--role-crashed-text)">[ERROR] ${e.message}</span>`;
  }
});

document.getElementById("btnRefresh").addEventListener("click", () => {
  fetchStatus();
  fetchKV();
});

document.getElementById("btnClearTrace").addEventListener("click", () => {
  timelineList.innerHTML = "";
});

// Chaos Handlers
document.getElementById("btnIsolateLeader").addEventListener("click", async () => {
  try {
    const res = await fetch("/api/v1/chaos/isolate_leader", { method: "POST" });
    const data = await res.json();
    appendEvent({ type: "PartitionCreated", details: `Chaos: Leader ${data.isolated || ""} partitioned into isolated minority` });
    fetchStatus();
  } catch (e) {}
});

document.getElementById("btnPartition").addEventListener("click", async () => {
  try {
    const res = await fetch("/api/v1/chaos/partition", { method: "POST" });
    const data = await res.json();
    appendEvent({ type: "PartitionCreated", details: `Chaos: Network split: Majority=[${data.majority.join(", ")}] vs Minority=[${data.minority.join(", ")}]` });
    fetchStatus();
  } catch (e) {}
});

document.getElementById("btnHeal").addEventListener("click", async () => {
  try {
    await fetch("/api/v1/chaos/heal", { method: "POST" });
    appendEvent({ type: "PartitionHealed", details: "Chaos: Network partition healed; full communication restored" });
    fetchStatus();
  } catch (e) {}
});

// Bootstrap
setupEventStream();
fetchStatus();
fetchKV();
setInterval(fetchStatus, 1200);
setInterval(fetchKV, 2000);
