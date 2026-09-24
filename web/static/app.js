const state = {
  nodes: {},
  clientId: "client-" + Math.floor(Math.random() * 100000),
  seqNum: 1,
  isSimulated: false,
  failedFetches: 0,
  simKv: {},
  simTerm: 1,
  simLeader: "node-3"
};

const nodesGrid = document.getElementById("nodesGrid");
const timelineList = document.getElementById("timelineList");
const kvTableBody = document.getElementById("kvTableBody");
const opResult = document.getElementById("opResult");
const currentLeaderText = document.getElementById("currentLeaderText");
const clusterStatusBadge = document.getElementById("clusterStatusBadge");

function renderNodes() {
  nodesGrid.innerHTML = "";
  const nodeKeys = Object.keys(state.nodes).sort();

  if (nodeKeys.length === 0) {
    nodesGrid.innerHTML = `<div style="color: var(--text-muted); font-size: 0.8rem; font-family: var(--font-mono); padding: 12px;">Initializing node connections...</div>`;
    return;
  }

  const targetSelect = document.getElementById("chaosTargetSelect");
  const previousSelected = targetSelect ? targetSelect.value : "";
  if (targetSelect) {
    targetSelect.innerHTML = `<option value="">Select Node...</option>`;
  }

  let leaderFound = false;
  for (const id of nodeKeys) {
    const node = state.nodes[id];
    const roleLower = (node.role || "follower").toLowerCase();
    const isLeader = roleLower === "leader";
    const isAlive = node.isAlive !== false && roleLower !== "offline" && roleLower !== "crashed";

    if (isLeader) {
      leaderFound = true;
      currentLeaderText.textContent = id;
    }

    if (targetSelect) {
      const opt = document.createElement("option");
      opt.value = id;
      opt.textContent = `${id} [${(node.role || "OFFLINE").toUpperCase()}]`;
      if (id === previousSelected) opt.selected = true;
      targetSelect.appendChild(opt);
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
      <div class="node-actions" style="margin-top: 12px; display: flex; gap: 8px; border-top: 1px solid var(--border-subtle); padding-top: 10px;">
        <button class="btn btn-sm btn-danger" onclick="killNode('${id}')" ${!isAlive ? 'disabled style="opacity: 0.35; cursor: not-allowed;"' : ''}>Kill</button>
        <button class="btn btn-sm" onclick="restartNode('${id}')" ${isAlive ? 'disabled style="opacity: 0.35; cursor: not-allowed;"' : ''}>Restart</button>
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
  if (window.location.protocol === "https:" && window.location.hostname.endsWith("github.io")) {
    return;
  }
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
    // Falls back gracefully
  };
}

function initSimulationEngine() {
  if (state.isSimulated) return;
  state.isSimulated = true;
  if (clusterStatusBadge) {
    clusterStatusBadge.textContent = "[SIMULATION DEMO]";
    clusterStatusBadge.title = "Interactive browser-side Raft cluster simulation for GitHub Pages demo";
  }

  state.simTerm = 1;
  state.simLeader = "node-3";
  state.nodes = {
    "node-1": { id: "node-1", role: "Follower", term: 1, commitIndex: 0, lastApplied: 0, logLength: 0, isAlive: true },
    "node-2": { id: "node-2", role: "Follower", term: 1, commitIndex: 0, lastApplied: 0, logLength: 0, isAlive: true },
    "node-3": { id: "node-3", role: "Leader", term: 1, commitIndex: 0, lastApplied: 0, logLength: 0, isAlive: true },
    "node-4": { id: "node-4", role: "Follower", term: 1, commitIndex: 0, lastApplied: 0, logLength: 0, isAlive: true },
    "node-5": { id: "node-5", role: "Follower", term: 1, commitIndex: 0, lastApplied: 0, logLength: 0, isAlive: true },
  };

  appendEvent({ type: "NodeRestarted", nodeId: "cluster", details: "Raft 5-node quorum initialized in browser simulation mode" });
  appendEvent({ type: "LeaderElected", nodeId: "node-3", details: "node-3 received majority quorum votes; stepped up as Leader for Term 1" });

  renderNodes();
  renderSimKV();

  // Periodic heartbeat animation in simulation mode
  setInterval(() => {
    if (!state.isSimulated) return;
    const leader = state.nodes[state.simLeader];
    if (leader && leader.isAlive && leader.role === "Leader") {
      // Pulse alive followers
      for (const id in state.nodes) {
        if (id !== state.simLeader && state.nodes[id].isAlive) {
          state.nodes[id].term = leader.term;
        }
      }
    }
  }, 2500);
}

function renderSimKV() {
  kvTableBody.innerHTML = "";
  const keys = Object.keys(state.simKv).sort();
  if (keys.length === 0) {
    kvTableBody.innerHTML = `<tr><td colspan="3" style="text-align: center; color: var(--text-muted); padding: 18px;">No entries committed to state machine</td></tr>`;
    return;
  }
  for (const k of keys) {
    const row = document.createElement("tr");
    row.innerHTML = `
      <td><b>${k}</b></td>
      <td>${state.simKv[k]}</td>
      <td style="color: var(--text-muted); font-size: 0.72rem; text-transform: uppercase;">[Committed]</td>
    `;
    kvTableBody.appendChild(row);
  }
}

async function fetchStatus() {
  if (state.isSimulated) return;
  try {
    const res = await fetch("/api/v1/cluster/status");
    if (res.ok) {
      const data = await res.json();
      state.failedFetches = 0;
      if (clusterStatusBadge) clusterStatusBadge.textContent = "[ONLINE]";
      if (Array.isArray(data)) {
        for (const item of data) {
          state.nodes[item.id] = item;
        }
      } else if (data && data.id) {
        state.nodes[data.id] = data;
      }
      renderNodes();
      return;
    }
  } catch (e) {
    state.failedFetches++;
    if (state.failedFetches >= 2 || window.location.hostname.endsWith("github.io")) {
      initSimulationEngine();
    }
  }
}

async function fetchKV() {
  if (state.isSimulated) {
    renderSimKV();
    return;
  }
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
  if (!key) {
    opResult.innerHTML = `<span style="color: var(--role-crashed-text)">[ERROR] Key cannot be empty</span>`;
    return;
  }

  opResult.textContent = "[PROCESSING] Submitting PUT command...";
  const start = performance.now();

  if (state.isSimulated) {
    setTimeout(() => {
      const leader = state.nodes[state.simLeader];
      if (!leader || !leader.isAlive || leader.role !== "Leader") {
        opResult.innerHTML = `<span style="color: var(--role-crashed-text)">[REJECTED] No leader elected in current term</span>`;
        return;
      }
      state.simKv[key] = value;
      leader.commitIndex++;
      leader.lastApplied++;
      leader.logLength++;
      for (const id in state.nodes) {
        if (id !== state.simLeader && state.nodes[id].isAlive) {
          state.nodes[id].commitIndex = leader.commitIndex;
          state.nodes[id].lastApplied = leader.lastApplied;
          state.nodes[id].logLength = leader.logLength;
        }
      }
      const latency = Math.round(performance.now() - start + 8);
      opResult.innerHTML = `<span style="color: var(--role-leader-text)">[SUCCESS] PUT '${key}' committed at index ${leader.commitIndex} in ${latency}ms</span>`;
      appendEvent({ type: "CommandCommitted", nodeId: state.simLeader, details: `Entry '${key}'='${value}' committed at index ${leader.commitIndex} by quorum` });
      renderNodes();
      renderSimKV();
    }, 40);
    return;
  }

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

  if (state.isSimulated) {
    setTimeout(() => {
      const val = state.simKv[key];
      const latency = Math.round(performance.now() - start + 2);
      if (val !== undefined) {
        document.getElementById("opValue").value = val;
        opResult.innerHTML = `<span style="color: var(--role-leader-text)">[SUCCESS] GET '${key}' = "${val}" in ${latency}ms</span>`;
      } else {
        opResult.innerHTML = `<span style="color: var(--role-candidate-text)">[NOT FOUND] Key '${key}' absent from state machine (${latency}ms)</span>`;
      }
    }, 20);
    return;
  }

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

  if (state.isSimulated) {
    setTimeout(() => {
      const leader = state.nodes[state.simLeader];
      if (!leader || !leader.isAlive || leader.role !== "Leader") {
        opResult.innerHTML = `<span style="color: var(--role-crashed-text)">[REJECTED] No leader elected in current term</span>`;
        return;
      }
      delete state.simKv[key];
      leader.commitIndex++;
      leader.lastApplied++;
      leader.logLength++;
      for (const id in state.nodes) {
        if (id !== state.simLeader && state.nodes[id].isAlive) {
          state.nodes[id].commitIndex = leader.commitIndex;
          state.nodes[id].lastApplied = leader.lastApplied;
          state.nodes[id].logLength = leader.logLength;
        }
      }
      const latency = Math.round(performance.now() - start + 8);
      opResult.innerHTML = `<span style="color: var(--role-leader-text)">[SUCCESS] DELETE '${key}' committed in ${latency}ms</span>`;
      appendEvent({ type: "CommandCommitted", nodeId: state.simLeader, details: `Entry '${key}' tombstoned at index ${leader.commitIndex}` });
      renderNodes();
      renderSimKV();
    }, 30);
    return;
  }

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
  if (state.isSimulated) {
    renderNodes();
    renderSimKV();
  } else {
    fetchStatus();
    fetchKV();
  }
});

document.getElementById("btnClearTrace").addEventListener("click", () => {
  timelineList.innerHTML = "";
});

// Chaos Handlers
document.getElementById("btnIsolateLeader").addEventListener("click", async () => {
  if (state.isSimulated) {
    const oldLeader = state.simLeader;
    if (state.nodes[oldLeader]) {
      state.nodes[oldLeader].role = "OFFLINE";
      state.nodes[oldLeader].isAlive = false;
    }
    appendEvent({ type: "PartitionCreated", details: `Chaos: Leader ${oldLeader} isolated into network partition` });
    // Elect replacement
    setTimeout(() => {
      state.simTerm++;
      const candidates = ["node-1", "node-2", "node-4", "node-5"].filter(id => state.nodes[id].isAlive);
      if (candidates.length >= 3) {
        state.simLeader = candidates[0];
        state.nodes[state.simLeader].role = "Leader";
        state.nodes[state.simLeader].term = state.simTerm;
        appendEvent({ type: "LeaderElected", nodeId: state.simLeader, details: `Quorum elected replacement leader ${state.simLeader} in Term ${state.simTerm}` });
      }
      renderNodes();
    }, 400);
    renderNodes();
    return;
  }
  try {
    const res = await fetch("/api/v1/chaos/isolate_leader", { method: "POST" });
    const data = await res.json();
    appendEvent({ type: "PartitionCreated", details: `Chaos: Leader ${data.isolated || ""} partitioned into isolated minority` });
    fetchStatus();
  } catch (e) {}
});

document.getElementById("btnPartition").addEventListener("click", async () => {
  if (state.isSimulated) {
    state.nodes["node-4"].role = "OFFLINE";
    state.nodes["node-5"].role = "OFFLINE";
    appendEvent({ type: "PartitionCreated", details: "Chaos: Split brain partition active: Majority=[node-1, node-2, node-3], Minority=[node-4, node-5]" });
    renderNodes();
    return;
  }
  try {
    const res = await fetch("/api/v1/chaos/partition", { method: "POST" });
    const data = await res.json();
    appendEvent({ type: "PartitionCreated", details: `Chaos: Network split: Majority=[${data.majority.join(", ")}] vs Minority=[${data.minority.join(", ")}]` });
    fetchStatus();
  } catch (e) {}
});

document.getElementById("btnHeal").addEventListener("click", async () => {
  if (state.isSimulated) {
    for (const id in state.nodes) {
      state.nodes[id].isAlive = true;
      if (id !== state.simLeader) state.nodes[id].role = "Follower";
      state.nodes[id].term = state.simTerm;
    }
    appendEvent({ type: "PartitionHealed", details: "Chaos: Network partition healed; full communication restored across all 5 nodes" });
    renderNodes();
    return;
  }
  try {
    await fetch("/api/v1/chaos/heal", { method: "POST" });
    appendEvent({ type: "PartitionHealed", details: "Chaos: Network partition healed; full communication restored" });
    fetchStatus();
  } catch (e) {}
});

window.killNode = async function(id) {
  if (!id) return;
  if (state.isSimulated) {
    if (state.nodes[id]) {
      state.nodes[id].role = "OFFLINE";
      state.nodes[id].isAlive = false;
      appendEvent({ type: "NodeCrashed", nodeId: id, details: `Chaos: Node ${id} manually stopped` });
      if (state.simLeader === id) {
        state.simLeader = "";
        setTimeout(() => {
          const alive = Object.keys(state.nodes).filter(nid => state.nodes[nid].isAlive);
          if (alive.length >= 3) {
            state.simTerm++;
            state.simLeader = alive[0];
            state.nodes[state.simLeader].role = "Leader";
            state.nodes[state.simLeader].term = state.simTerm;
            appendEvent({ type: "LeaderElected", nodeId: state.simLeader, details: `Replacement leader ${state.simLeader} elected for Term ${state.simTerm}` });
          }
          renderNodes();
        }, 500);
      }
      renderNodes();
    }
    return;
  }
  try {
    const res = await fetch(`/api/v1/chaos/kill?node=${encodeURIComponent(id)}`, { method: "POST" });
    const data = await res.json();
    appendEvent({ type: "NodeCrashed", nodeId: id, details: `Chaos: Node ${id} manually stopped` });
    fetchStatus();
  } catch (e) {
    console.error("killNode error:", e);
  }
};

window.restartNode = async function(id) {
  if (!id) return;
  if (state.isSimulated) {
    if (state.nodes[id]) {
      state.nodes[id].isAlive = true;
      state.nodes[id].role = "Follower";
      state.nodes[id].term = state.simTerm;
      appendEvent({ type: "NodeRestarted", nodeId: id, details: `Chaos: Node ${id} restarted and rejoining consensus` });
      renderNodes();
    }
    return;
  }
  try {
    const res = await fetch(`/api/v1/chaos/restart?node=${encodeURIComponent(id)}`, { method: "POST" });
    const data = await res.json();
    appendEvent({ type: "NodeRestarted", nodeId: id, details: `Chaos: Node ${id} restarted and rejoining consensus` });
    fetchStatus();
  } catch (e) {
    console.error("restartNode error:", e);
  }
};

const btnKillTarget = document.getElementById("btnKillTarget");
if (btnKillTarget) {
  btnKillTarget.addEventListener("click", () => {
    const sel = document.getElementById("chaosTargetSelect");
    if (sel && sel.value) {
      window.killNode(sel.value);
    }
  });
}

const btnRestartTarget = document.getElementById("btnRestartTarget");
if (btnRestartTarget) {
  btnRestartTarget.addEventListener("click", () => {
    const sel = document.getElementById("chaosTargetSelect");
    if (sel && sel.value) {
      window.restartNode(sel.value);
    }
  });
}

// Bootstrap
if (window.location.hostname.endsWith("github.io")) {
  initSimulationEngine();
} else {
  setupEventStream();
  fetchStatus();
  fetchKV();
  setInterval(fetchStatus, 1200);
  setInterval(fetchKV, 2000);
}
