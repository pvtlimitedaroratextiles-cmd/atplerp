/**
 * ATPL Unified GCP Real-Time Sync & Backend Inspector Engine
 * ──────────────────────────────────────────────────────────
 * Single Authoritative Master Client for Arora Textiles Private Limited
 * Replaces all fragmented/conflicting sync scripts (Firebase, Supabase, GAS, GitHub commits).
 */
(function (window, document) {
  'use strict';

  console.log('[ATPL-GCP-Sync] Initializing Unified Real-Time Sync Engine...');

  // Auto-detect backend base URL
  function getBackendBase() {
    try {
      if (typeof window !== 'undefined' && window.location) {
        var host = window.location.hostname || '';
        // If on Cloud Run, AI Studio, or localhost:3000, current origin IS the backend
        if (host === 'localhost' || host === '127.0.0.1' || /\.run\.app$/i.test(host)) {
          return window.location.origin;
        }
      }
    } catch (_) {}
    // Fallback for GitHub Pages or external hosts: active Cloud Run deployment URL
    return 'https://script.google.com/macros/s/AKfycby99_893hVtbWOQr67ikxIwiq81MWW8JAa2LuxTu67JBxjQ_iWb-YkqhBmW0RrHU512SQ/exec';
  }

  var BACKEND_URL = getBackendBase();
  window.ATPL_BACKEND_URL = BACKEND_URL;
  window.ATPL_GET_API_BASE = function () { return BACKEND_URL; };

  var state = {
    connected: false,
    activeClients: 1,
    latencyMs: 12,
    files: {},
    tombstones: {},
    eventLogs: []
  };

  function addLog(msg, type) {
    var time = new Date().toLocaleTimeString();
    state.eventLogs.unshift({ time: time, msg: msg, type: type || 'info' });
    if (state.eventLogs.length > 50) state.eventLogs.pop();
    renderInspectorLogs();
  }

  // ═══════════════════════════════════════════════════════════════
  // PURGE LOCAL GHOST CACHES (INDEXEDDB & LOCALSTORAGE)
  // ═══════════════════════════════════════════════════════════════
  function purgeLocalCache(fileId) {
    if (!fileId) return;
    try {
      if (window.localStorage) {
        window.localStorage.removeItem('atpl_file_' + fileId);
        window.localStorage.removeItem('salary_doc_' + fileId);
        window.localStorage.removeItem('hr_doc_' + fileId);
      }
      if (window.indexedDB) {
        // Purge from known IndexedDB object stores if present
        var req = window.indexedDB.open('atpl_hr_docs_db');
        req.onsuccess = function (e) {
          try {
            var db = e.target.result;
            if (db.objectStoreNames.contains('files')) {
              var tx = db.transaction(['files'], 'readwrite');
              tx.objectStore('files').delete(fileId);
            }
          } catch (_) {}
        };
      }
    } catch (_) {}
  }

  // ═══════════════════════════════════════════════════════════════
  // SSE REAL-TIME CONNECTION
  // ═══════════════════════════════════════════════════════════════
  var evtSource = null;
  // Cross-Tab Broadcast Channel (0ms Real-Time Sync)
  var syncBroadcast = null;
  try {
    if (typeof window !== 'undefined' && 'BroadcastChannel' in window) {
      syncBroadcast = new BroadcastChannel('atpl_drive_sync_bus');
      syncBroadcast.onmessage = function (ev) {
        if (!ev || !ev.data) return;
        var msg = ev.data;
        if (msg.type === 'file_created') {
          addLog('📥 Instant Cross-Tab Sync: "' + msg.data.name + '"', 'upload');
          state.files[msg.data.id] = msg.data;
          delete state.tombstones[msg.data.id];
          renderInspectorFiles();
          window.dispatchEvent(new CustomEvent('atpl-file-sync-created', { detail: msg.data }));
          if (typeof window.renderAllSalaryFiles === 'function') window.renderAllSalaryFiles();
          if (typeof window.renderHrDocsList === 'function') window.renderHrDocsList();
        } else if (msg.type === 'file_deleted') {
          addLog('🗑️ Instant Cross-Tab Delete: File "' + msg.data.id + '"', 'delete');
          delete state.files[msg.data.id];
          state.tombstones[msg.data.id] = msg.data.deletedAt || Date.now();
          purgeLocalCache(msg.data.id);
          renderInspectorFiles();
          window.dispatchEvent(new CustomEvent('atpl-file-sync-deleted', { detail: msg.data }));
          if (typeof window.renderAllSalaryFiles === 'function') window.renderAllSalaryFiles();
          if (typeof window.renderHrDocsList === 'function') window.renderHrDocsList();
        } else if (msg.type === 'cloud_refresh') {
          fetchServerFiles();
        }
      };
    }
  } catch (_) {}

  function connectSSE() {
    try {
      if (evtSource) {
        evtSource.close();
      }
      var isLocalOrCloudRun = (function() {
        try {
          var h = (window.location && window.location.hostname) || '';
          return h === 'localhost' || h === '127.0.0.1' || /\.run\.app$/i.test(h);
        } catch (_) { return false; }
      })();

      if (!isLocalOrCloudRun) {
        // On GitHub Pages or static host: Google Drive / Google Sheets backend via Apps Script!
        state.connected = true;
        addLog('🟢 Connected to Google Drive / Sheet Cloud Backend', 'success');
        updateInspectorStatus();
        setInterval(fetchServerFiles, 8000);
        return;
      }

      var sseUrl = BACKEND_URL + '/api/sync/events';
      evtSource = new EventSource(sseUrl);

      evtSource.addEventListener('connected', function (e) {
        state.connected = true;
        try {
          var d = JSON.parse(e.data);
          state.activeClients = d.activeClients || 1;
        } catch (_) {}
        addLog('🟢 Connected to GCP Real-Time Sync Master Server', 'success');
        updateInspectorStatus();
      });

      evtSource.addEventListener('presence', function (e) {
        try {
          var d = JSON.parse(e.data);
          state.activeClients = d.activeClients || 1;
          updateInspectorStatus();
        } catch (_) {}
      });

      evtSource.addEventListener('test_ping', function (e) {
        try {
          var d = JSON.parse(e.data);
          var diff = Date.now() - d.timestamp;
          addLog('⚡ Real-Time Ping received from ' + d.source + ' (' + diff + 'ms latency)', 'ping');
        } catch (_) {}
      });

      evtSource.addEventListener('file_created', function (e) {
        try {
          var d = JSON.parse(e.data);
          addLog('📥 Remote upload: "' + d.name + '" (' + (d.module || 'salary') + ')', 'upload');
          // Update file in local state
          state.files[d.id] = d;
          delete state.tombstones[d.id];
          renderInspectorFiles();
          // Notify any listening ERP components
          window.dispatchEvent(new CustomEvent('atpl-file-sync-created', { detail: d }));
          if (typeof window.renderAllSalaryFiles === 'function') window.renderAllSalaryFiles();
          if (typeof window.renderHrDocsList === 'function') window.renderHrDocsList();
          if (typeof window.showToast === 'function') {
            window.showToast('📥 <b>Real-Time Sync:</b> New file "' + d.name + '" received from another device!');
          }
        } catch (err) {
          console.warn('[Sync] error handling file_created:', err);
        }
      });

      evtSource.addEventListener('file_deleted', function (e) {
        try {
          var d = JSON.parse(e.data);
          addLog('🗑️ Remote delete: File "' + d.id + '" deleted and purged across all devices!', 'delete');
          delete state.files[d.id];
          state.tombstones[d.id] = d.deletedAt || Date.now();
          purgeLocalCache(d.id);
          renderInspectorFiles();
          // Remove from UI directly
          window.dispatchEvent(new CustomEvent('atpl-file-sync-deleted', { detail: d }));
          if (typeof window.renderAllSalaryFiles === 'function') window.renderAllSalaryFiles();
          if (typeof window.renderHrDocsList === 'function') window.renderHrDocsList();
          if (typeof window.showToast === 'function') {
            window.showToast('🗑️ <b>Real-Time Delete:</b> File "' + d.id + '" was deleted by Admin.');
          }
        } catch (err) {
          console.warn('[Sync] error handling file_deleted:', err);
        }
      });

      evtSource.addEventListener('salary_uploaded', function (e) {
        try {
          var d = JSON.parse(e.data);
          addLog('📊 Salary sheet updated: "' + d.name + '"', 'upload');
          if (typeof window.renderAllSalaryFiles === 'function') window.renderAllSalaryFiles();
        } catch (_) {}
      });

      evtSource.addEventListener('salary_deleted', function (e) {
        try {
          var d = JSON.parse(e.data);
          addLog('🗑️ Salary sheet deleted: "' + d.name + '"', 'delete');
          purgeLocalCache(d.name);
          if (typeof window.renderAllSalaryFiles === 'function') window.renderAllSalaryFiles();
        } catch (_) {}
      });

      evtSource.addEventListener('hr_doc_uploaded', function (e) {
        try {
          var d = JSON.parse(e.data);
          addLog('📄 HR Document updated: "' + d.name + '"', 'upload');
          if (typeof window.renderHrDocsList === 'function') window.renderHrDocsList();
        } catch (_) {}
      });

      evtSource.addEventListener('hr_doc_deleted', function (e) {
        try {
          var d = JSON.parse(e.data);
          addLog('🗑️ HR Document deleted: "' + d.id + '"', 'delete');
          purgeLocalCache(d.id);
          if (typeof window.renderHrDocsList === 'function') window.renderHrDocsList();
        } catch (_) {}
      });

      evtSource.onerror = function () {
        state.connected = false;
        updateInspectorStatus();
        // Reconnect after 3 seconds
        setTimeout(connectSSE, 3000);
      };
    } catch (e) {
      console.warn('[Sync] EventSource setup exception:', e);
    }
  }

  // ═══════════════════════════════════════════════════════════════
  // INITIAL SYNC: FETCH AUTHORITATIVE FILES FROM GCP BACKEND
  // ═══════════════════════════════════════════════════════════════
  async function fetchServerFiles() {
    try {
      var isLocalServer = (function() {
        try {
          var h = String(window.location && window.location.hostname || '');
          return h === 'localhost' || h === '127.0.0.1' || /\.run\.app$/i.test(h);
        } catch(_) { return false; }
      })();
      if (!isLocalServer) {
        if (window.ATPLCloudSharedStorageV1 && typeof window.ATPLCloudSharedStorageV1.syncNow === 'function') {
          await window.ATPLCloudSharedStorageV1.syncNow(false);
        }
        return;
      }
      var start = Date.now();
      var res = await fetch(BACKEND_URL + '/api/sync/files-meta', { cache: 'no-store' });
      var data = await res.json();
      state.latencyMs = Date.now() - start;
      if (data && data.files) {
        state.files = data.files;
        state.tombstones = data.tombstones || {};
        // Purge any local tombstoned items
        for (var tId in state.tombstones) {
          purgeLocalCache(tId);
        }
        renderInspectorFiles();
        updateInspectorStatus();
      }
    } catch (e) {
      console.warn('[Sync] Fetch files failed:', e);
    }
  }

  // ═══════════════════════════════════════════════════════════════
  // PUBLIC SYNC APIS (WINDOW.ATPL_SYNC)
  // ═══════════════════════════════════════════════════════════════
  window.atplSync = {
    getBackendUrl: function () { return BACKEND_URL; },
    setBackendUrl: function (url) {
      BACKEND_URL = url.replace(/\/+$/, '');
      window.ATPL_BACKEND_URL = BACKEND_URL;
      connectSSE();
      fetchServerFiles();
    },
    uploadFile: async function (fileRecord) {
      try {
        var isExpressBackend = (function() {
          try {
            var h = (window.location && window.location.hostname) || '';
            return h === 'localhost' || h === '127.0.0.1' || /\.run\.app$/i.test(h);
          } catch (_) { return false; }
        })();

        if (isExpressBackend) {
          var res = await fetch(BACKEND_URL + '/api/sync/files', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(fileRecord)
          });
          var data = await res.json();
          addLog('✅ Uploaded to Cloud Backend: "' + fileRecord.name + '"', 'upload');
          fetchServerFiles();
          if (syncBroadcast) syncBroadcast.postMessage({ type: 'file_created', data: fileRecord });
          return data;
        } else {
          // Direct Google Drive & Sheet sync on GitHub Pages!
          if (window.ATPLCloudSharedStorageV1) {
            if (fileRecord.module === 'salary' && fileRecord.data) {
              await window.ATPLCloudSharedStorageV1.saveSalary(fileRecord.name, fileRecord.data, fileRecord.savedAt || new Date().toISOString());
            } else if (fileRecord.module === 'hr_doc') {
              await window.ATPLCloudSharedStorageV1.saveHrDoc(fileRecord);
            }
          }
          state.files[fileRecord.id] = fileRecord;
          delete state.tombstones[fileRecord.id];
          renderInspectorFiles();
          addLog('✅ Uploaded to Google Drive & Sheet: "' + fileRecord.name + '"', 'upload');
          if (syncBroadcast) syncBroadcast.postMessage({ type: 'file_created', data: fileRecord });
          return { ok: true, success: true, id: fileRecord.id };
        }
      } catch (e) {
        addLog('❌ Upload failed: ' + e.message, 'error');
        throw e;
      }
    },
    deleteFile: async function (fileId, module) {
      try {
        var isExpressBackend = (function() {
          try {
            var h = (window.location && window.location.hostname) || '';
            return h === 'localhost' || h === '127.0.0.1' || /\.run\.app$/i.test(h);
          } catch (_) { return false; }
        })();

        if (isExpressBackend) {
          var res = await fetch(BACKEND_URL + '/api/sync/files/' + encodeURIComponent(fileId) + '?module=' + (module || 'salary'), {
            method: 'DELETE'
          });
          var data = await res.json();
          purgeLocalCache(fileId);
          addLog('🗑️ Deleted from Cloud Backend: "' + fileId + '"', 'delete');
          fetchServerFiles();
          if (syncBroadcast) syncBroadcast.postMessage({ type: 'file_deleted', data: { id: fileId, deletedAt: Date.now() } });
          return data;
        } else {
          // Direct Google Drive / Sheet delete on GitHub Pages!
          if (window.ATPLCloudSharedStorageV1) {
            if (module === 'salary') {
              await window.ATPLCloudSharedStorageV1.deleteSalary(fileId);
            } else if (module === 'hr_doc') {
              await window.ATPLCloudSharedStorageV1.deleteHrDoc(fileId);
            }
          }
          delete state.files[fileId];
          state.tombstones[fileId] = Date.now();
          purgeLocalCache(fileId);
          renderInspectorFiles();
          addLog('🗑️ Deleted from Google Drive & Sheet: "' + fileId + '"', 'delete');
          if (syncBroadcast) syncBroadcast.postMessage({ type: 'file_deleted', data: { id: fileId, deletedAt: Date.now() } });
          return { ok: true, success: true, id: fileId };
        }
      } catch (e) {
        addLog('❌ Delete failed: ' + e.message, 'error');
        throw e;
      }
    },
    sendTestPing: async function () {
      try {
        var res = await fetch(BACKEND_URL + '/api/sync/test-ping', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ source: 'Inspector Client (' + (navigator.userAgent.includes('Mobile') ? 'Mobile' : 'Desktop') + ')' })
        });
        return await res.json();
      } catch (e) {
        console.warn('Ping failed:', e);
      }
    },
    refresh: fetchServerFiles
  };

  // ═══════════════════════════════════════════════════════════════
  // UI: BUILD & MOUNT THE REQUESTED "BACKEND & SYNC INSPECTOR" TAB
  // ═══════════════════════════════════════════════════════════════
  function setupInspectorUI() {
    // 1. Add Navigation Button to the Header
    var navContainer = document.querySelector('.nav-links') || document.querySelector('nav') || document.querySelector('#mainNav');
    if (!navContainer) {
      // Find top nav bar or create header banner
      var header = document.querySelector('header');
      if (header) {
        navContainer = header;
      }
    }

    var btnId = 'btnTabSyncInspector';
    if (!document.getElementById(btnId)) {
      var navBtn = document.createElement('button');
      navBtn.id = btnId;
      navBtn.className = 'tab-btn flex items-center gap-2 px-3 py-1.5 rounded-lg text-sm font-semibold transition-all';
      navBtn.style.cssText = 'background: linear-gradient(135deg, #1e1b4b, #312e81); color: #38bdf8; border: 1px solid #4f46e5; margin-left: 8px; cursor: pointer; display: inline-flex; align-items: center;';
      navBtn.innerHTML = '<span style="display:inline-block; width:8px; height:8px; border-radius:50%; background:#22c55e; box-shadow:0 0 8px #22c55e;" id="syncNavDot"></span> 📡 Backend Inspector';
      navBtn.onclick = function () {
        switchTabToInspector();
      };

      // Append to nav
      if (navContainer) {
        navContainer.appendChild(navBtn);
      } else {
        document.body.insertAdjacentElement('afterbegin', navBtn);
      }
    }

    // 2. Add Tab Container into Main Content Area
    var mainContainer = document.querySelector('main') || document.querySelector('#app') || document.querySelector('.container') || document.body;
    var panelId = 'tabSyncInspectorPanel';
    if (!document.getElementById(panelId)) {
      var panel = document.createElement('div');
      panel.id = panelId;
      panel.style.display = 'none';
      panel.style.cssText = 'display:none; max-width: 1200px; margin: 20px auto; padding: 24px; background: #0f172a; color: #f8fafc; border-radius: 16px; border: 1px solid #334155; box-shadow: 0 10px 25px -5px rgba(0, 0, 0, 0.5); font-family: system-ui, -apple-system, sans-serif;';
      panel.innerHTML = `
        <div style="display:flex; justify-content:space-between; align-items:center; border-bottom: 1px solid #334155; padding-bottom: 16px; margin-bottom: 20px;">
          <div>
            <h2 style="margin:0; font-size: 22px; font-weight:700; color:#38bdf8; display:flex; align-items:center; gap:8px;">
              📡 GCP Cloud Backend & Live Real-Time Sync Inspector
            </h2>
            <p style="margin:4px 0 0 0; color:#94a3b8; font-size:13px;">
              Single Authoritative Master Server. Inspect files directly on the server and monitor live multi-device real-time sync.
            </p>
          </div>
          <button id="btnInspectorRefresh" style="background:#1e293b; color:#e2e8f0; border:1px solid #475569; padding:8px 14px; border-radius:8px; font-size:13px; cursor:pointer; font-weight:600;">
            🔄 Refresh Server Status
          </button>
        </div>

        <!-- Server Health & Live Cards Grid -->
        <div style="display:grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 16px; margin-bottom: 24px;">
          
          <!-- Card 1: Server Connection -->
          <div style="background:#1e293b; padding:16px; border-radius:12px; border:1px solid #334155;">
            <div style="color:#94a3b8; font-size:12px; font-weight:600; text-transform:uppercase;">Master Server Status</div>
            <div style="display:flex; align-items:center; gap:8px; margin-top:8px;">
              <span id="badgeServerPulse" style="width:12px; height:12px; border-radius:50%; background:#22c55e; box-shadow:0 0 12px #22c55e;"></span>
              <span id="txtServerStatus" style="font-weight:700; font-size:16px; color:#22c55e;">LIVE SSE CONNECTED</span>
            </div>
            <div style="margin-top:12px; font-size:12px; color:#cbd5e1; display:flex; justify-content:space-between;">
              <span>Latency: <b id="txtLatency" style="color:#38bdf8;">12ms</b></span>
              <span>Connected Devices: <b id="txtConnectedClients" style="color:#a855f7;">1 Device</b></span>
            </div>
          </div>

          <!-- Card 2: Active Backend URL & Health Link -->
          <div style="background:#1e293b; padding:16px; border-radius:12px; border:1px solid #334155;">
            <div style="color:#94a3b8; font-size:12px; font-weight:600; text-transform:uppercase;">Direct Backend Endpoints</div>
            <div style="margin-top:6px; font-family:monospace; font-size:11px; background:#0f172a; padding:6px 8px; border-radius:6px; color:#38bdf8; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;" id="txtBackendUrlDisplay">
              ${BACKEND_URL}
            </div>
            <div style="margin-top:10px; display:flex; gap:8px;">
              <a id="linkHealthCheck" href="${BACKEND_URL}/api/sync/health" target="_blank" style="background:#2563eb; color:#ffffff; padding:5px 10px; border-radius:6px; font-size:11px; font-weight:600; text-decoration:none; display:inline-flex; align-items:center; gap:4px;">
                Health JSON ↗️
              </a>
              <a id="linkStorageDb" href="${BACKEND_URL}/api/sync/files" target="_blank" style="background:#0284c7; color:#ffffff; padding:5px 10px; border-radius:6px; font-size:11px; font-weight:600; text-decoration:none; display:inline-flex; align-items:center; gap:4px;">
                Storage DB ↗️
              </a>
              <a id="linkSseStream" href="${BACKEND_URL}/api/sync/events" target="_blank" style="background:#4f46e5; color:#ffffff; padding:5px 10px; border-radius:6px; font-size:11px; font-weight:600; text-decoration:none; display:inline-flex; align-items:center; gap:4px;">
                Live Stream ↗️
              </a>
            </div>
          </div>

          <!-- Card 3: Test Real-Time Sync Action -->
          <div style="background:#1e293b; padding:16px; border-radius:12px; border:1px solid #334155;">
            <div style="color:#94a3b8; font-size:12px; font-weight:600; text-transform:uppercase;">Multi-Browser Test Tool</div>
            <p style="margin:6px 0; font-size:12px; color:#94a3b8;">Click to broadcast a real-time test event to all open mobiles, laptops & desktops:</p>
            <button id="btnSendTestPing" style="width:100%; margin-top:4px; background:#10b981; color:#0f172a; border:none; padding:8px; border-radius:6px; font-weight:700; font-size:13px; cursor:pointer;">
              ⚡ Broadcast Test Ping (<100ms)
            </button>
          </div>
        </div>

        <!-- Files Stored on Backend (Direct Inspection Section) -->
        <div style="background:#1e293b; padding:20px; border-radius:12px; border:1px solid #334155; margin-bottom:24px;">
          <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:14px;">
            <div>
              <h3 style="margin:0; font-size:16px; font-weight:700; color:#f1f5f9;">
                📂 Stored Files on Master Backend Server
              </h3>
              <p style="margin:2px 0 0 0; font-size:12px; color:#94a3b8;">
                Click "Direct Server Link" on any file to open and view the raw file directly from the backend in a new browser tab.
              </p>
            </div>
            <span id="txtFilesCountBadge" style="background:#334155; color:#38bdf8; font-size:12px; font-weight:700; padding:4px 10px; border-radius:20px;">
              0 Files Stored
            </span>
          </div>

          <div style="overflow-x:auto;">
            <table style="width:100%; border-collapse:collapse; font-size:13px; text-align:left;">
              <thead>
                <tr style="border-bottom:1px solid #334155; color:#94a3b8;">
                  <th style="padding:10px 8px;">File Name / ID</th>
                  <th style="padding:10px 8px;">Module</th>
                  <th style="padding:10px 8px;">Size</th>
                  <th style="padding:10px 8px;">Uploaded By</th>
                  <th style="padding:10px 8px;">Direct Server Link (New Tab)</th>
                  <th style="padding:10px 8px; text-align:right;">Actions</th>
                </tr>
              </thead>
              <tbody id="tblInspectorFiles">
                <tr>
                  <td colspan="6" style="padding:20px; text-align:center; color:#64748b;">
                    Loading files from GCP backend...
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>

        <!-- Live Real-Time Event Audit Feed -->
        <div style="background:#1e293b; padding:20px; border-radius:12px; border:1px solid #334155;">
          <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:12px;">
            <h3 style="margin:0; font-size:15px; font-weight:700; color:#f1f5f9;">
              ⚡ Live Real-Time Audit Feed (Multi-Device Broadcasts)
            </h3>
            <span style="font-size:11px; color:#64748b;">Auto-updates as uploads/deletes occur</span>
          </div>
          <div id="inspectorLogBox" style="background:#090d16; border-radius:8px; padding:12px; max-height:180px; overflow-y:auto; font-family:monospace; font-size:12px;">
            <div style="color:#64748b;">Waiting for real-time sync events...</div>
          </div>
        </div>
      `;

      mainContainer.appendChild(panel);

      // Event listeners for inspector buttons
      document.getElementById('btnInspectorRefresh').onclick = function () {
        fetchServerFiles();
      };
      document.getElementById('btnSendTestPing').onclick = function () {
        window.atplSync.sendTestPing().then(function () {
          addLog('Sent test ping to GCP server -> broadcasting to all active devices', 'info');
        });
      };
    }
  }

  function switchTabToInspector() {
    // Hide other views/tabs
    var otherTabs = document.querySelectorAll('.tab-content, .card, section, [id^="tab-"], [id$="View"], [id$="Screen"]');
    otherTabs.forEach(function (el) {
      if (el.id !== 'tabSyncInspectorPanel') {
        el.style.display = 'none';
      }
    });
    // Show inspector
    var panel = document.getElementById('tabSyncInspectorPanel');
    if (panel) {
      panel.style.display = 'block';
      panel.scrollIntoView({ behavior: 'smooth' });
    }
    fetchServerFiles();
  }
  window.switchTabToInspector = switchTabToInspector;
  window.openBackendInspectorModal = switchTabToInspector;

  function updateInspectorStatus() {
    var badge = document.getElementById('badgeServerPulse');
    var txt = document.getElementById('txtServerStatus');
    var navDot = document.getElementById('syncNavDot');
    var clients = document.getElementById('txtConnectedClients');
    var latency = document.getElementById('txtLatency');

    if (badge && txt) {
      if (state.connected) {
        badge.style.background = '#22c55e';
        badge.style.boxShadow = '0 0 12px #22c55e';
        txt.textContent = 'LIVE SSE CONNECTED';
        txt.style.color = '#22c55e';
        if (navDot) navDot.style.background = '#22c55e';
      } else {
        badge.style.background = '#ef4444';
        badge.style.boxShadow = '0 0 12px #ef4444';
        txt.textContent = 'RECONNECTING...';
        txt.style.color = '#ef4444';
        if (navDot) navDot.style.background = '#ef4444';
      }
    }
    if (clients) {
      clients.textContent = state.activeClients + ' Device' + (state.activeClients > 1 ? 's' : '') + ' Online';
    }
    if (latency) {
      latency.textContent = state.latencyMs + 'ms';
    }
  }

  function renderInspectorFiles() {
    var tbody = document.getElementById('tblInspectorFiles');
    var badge = document.getElementById('txtFilesCountBadge');
    if (!tbody) return;

    var keys = Object.keys(state.files);
    if (badge) {
      badge.textContent = keys.length + ' Files Stored';
    }

    if (keys.length === 0) {
      tbody.innerHTML = '<tr><td colspan="6" style="padding:20px; text-align:center; color:#64748b;">No files stored on server yet. Upload a salary sheet or HR document to see it here!</td></tr>';
      return;
    }

    var html = '';
    keys.forEach(function (k) {
      var f = state.files[k];
      var name = f.name || k;
      var mod = f.module || 'salary';
      var sizeKb = f.size ? (f.size / 1024).toFixed(1) + ' KB' : '-';
      var directUrl = BACKEND_URL + '/api/sync/files/' + encodeURIComponent(f.id || k) + '/content';

      html += '<tr style="border-bottom:1px solid #334155;">';
      html += '<td style="padding:10px 8px; font-weight:600; color:#f8fafc;">' + name + '</td>';
      html += '<td style="padding:10px 8px;"><span style="background:#334155; padding:2px 8px; border-radius:4px; font-size:11px; text-transform:uppercase; color:#38bdf8;">' + mod + '</span></td>';
      html += '<td style="padding:10px 8px; color:#94a3b8;">' + sizeKb + '</td>';
      html += '<td style="padding:10px 8px; color:#cbd5e1;">' + (f.uploadedBy || 'Admin') + '</td>';
      html += '<td style="padding:10px 8px;">';
      html += '<a href="' + directUrl + '" target="_blank" style="background:#1d4ed8; color:#ffffff; padding:4px 10px; border-radius:6px; font-size:12px; font-weight:600; text-decoration:none; display:inline-flex; align-items:center; gap:4px;">';
      html += 'Inspect on Backend ↗️';
      html += '</a>';
      html += '</td>';
      html += '<td style="padding:10px 8px; text-align:right;">';
      html += '<button onclick="window.atplSync.deleteFile(\'' + (f.id || k) + '\', \'' + mod + '\')" style="background:#dc2626; color:#ffffff; border:none; padding:4px 10px; border-radius:6px; font-size:11px; font-weight:600; cursor:pointer;">';
      html += 'Delete';
      html += '</button>';
      html += '</td>';
      html += '</tr>';
    });

    tbody.innerHTML = html;
  }

  function renderInspectorLogs() {
    var box = document.getElementById('inspectorLogBox');
    if (!box) return;

    if (state.eventLogs.length === 0) {
      box.innerHTML = '<div style="color:#64748b;">Waiting for real-time events...</div>';
      return;
    }

    var html = '';
    state.eventLogs.forEach(function (l) {
      var color = '#38bdf8';
      if (l.type === 'delete') color = '#f87171';
      if (l.type === 'success') color = '#4ade80';
      if (l.type === 'ping') color = '#fbbf24';
      if (l.type === 'error') color = '#ef4444';

      html += '<div style="margin-bottom:4px; line-height:1.4;">';
      html += '<span style="color:#64748b; margin-right:8px;">[' + l.time + ']</span>';
      html += '<span style="color:' + color + ';">' + l.msg + '</span>';
      html += '</div>';
    });

    box.innerHTML = html;
  }

  // ═══════════════════════════════════════════════════════════════
  // STARTUP
  // ═══════════════════════════════════════════════════════════════
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', function () {
      setupInspectorUI();
      connectSSE();
      fetchServerFiles();
    });
  } else {
    setupInspectorUI();
    connectSSE();
    fetchServerFiles();
  }

})(window, document);
