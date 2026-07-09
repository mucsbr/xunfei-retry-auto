package main

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"
)

func (p *proxyServer) serveAdminHTTP(w http.ResponseWriter, r *http.Request) bool {
	if r.URL.Path != "/admin" && !strings.HasPrefix(r.URL.Path, "/admin/") {
		return false
	}

	switch {
	case r.URL.Path == "/admin" || r.URL.Path == "/admin/":
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		_, _ = w.Write([]byte(adminPageHTML))
		return true
	case r.URL.Path == "/admin/api/metrics" || r.URL.Path == "/admin/api/metrics/":
		writeJSON(w, http.StatusOK, p.metrics.snapshot(time.Now()))
		return true
	case strings.HasPrefix(r.URL.Path, "/admin/api/errors/"):
		id := strings.TrimPrefix(r.URL.Path, "/admin/api/errors/")
		if id == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "missing error id"})
			return true
		}
		detail, ok := p.metrics.getError(id)
		if !ok {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "error detail not found"})
			return true
		}
		writeJSON(w, http.StatusOK, detail)
		return true
	default:
		http.NotFound(w, r)
		return true
	}
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

const adminPageHTML = `<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>xunfei retry proxy</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f6f7f9;
      --panel: #ffffff;
      --text: #17202a;
      --muted: #697586;
      --line: #d7dce2;
      --strong: #0f766e;
      --warn: #b45309;
      --bad: #b91c1c;
      --shadow: 0 1px 2px rgba(15, 23, 42, 0.08);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: var(--bg);
      color: var(--text);
      font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      font-size: 14px;
    }
    header {
      height: 56px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 0 24px;
      border-bottom: 1px solid var(--line);
      background: var(--panel);
    }
    h1 {
      margin: 0;
      font-size: 16px;
      font-weight: 650;
      letter-spacing: 0;
    }
    main {
      max-width: 1360px;
      margin: 0 auto;
      padding: 20px 24px 32px;
    }
    .toolbar {
      display: flex;
      align-items: center;
      gap: 12px;
      color: var(--muted);
      white-space: nowrap;
    }
    button {
      border: 1px solid var(--line);
      background: #fff;
      color: var(--text);
      border-radius: 6px;
      padding: 7px 10px;
      cursor: pointer;
      font: inherit;
    }
    button:hover { border-color: #9aa4b2; }
    .metrics {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
      margin-bottom: 18px;
    }
    .metric {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 14px;
      box-shadow: var(--shadow);
      min-height: 92px;
    }
    .metric .label {
      color: var(--muted);
      font-size: 12px;
      margin-bottom: 8px;
    }
    .metric .value {
      font-size: 24px;
      font-weight: 700;
      letter-spacing: 0;
      line-height: 1.1;
    }
    .metric .sub {
      color: var(--muted);
      font-size: 12px;
      margin-top: 8px;
      min-height: 16px;
    }
    .trend {
      display: flex;
      gap: 6px;
      align-items: end;
      height: 38px;
      margin-top: 10px;
    }
    .bar {
      flex: 1;
      min-width: 0;
      display: flex;
      flex-direction: column;
      justify-content: end;
      gap: 4px;
      color: var(--muted);
      font-size: 10px;
      text-align: center;
    }
    .bar span {
      display: block;
      min-height: 3px;
      background: #0f766e;
      border-radius: 3px 3px 0 0;
    }
    .panel {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      box-shadow: var(--shadow);
      overflow: hidden;
    }
    .panel-header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 12px 14px;
      border-bottom: 1px solid var(--line);
    }
    h2 {
      margin: 0;
      font-size: 14px;
      font-weight: 650;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      table-layout: fixed;
    }
    th, td {
      padding: 10px 12px;
      border-bottom: 1px solid #edf0f3;
      text-align: left;
      vertical-align: middle;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    th {
      color: var(--muted);
      font-size: 12px;
      font-weight: 600;
      background: #fbfcfd;
    }
    tbody tr:hover { background: #f8fafc; }
    .status-ok { color: var(--strong); font-weight: 650; }
    .status-bad { color: var(--bad); font-weight: 650; }
    .muted { color: var(--muted); }
    .right { text-align: right; }
    .modal-backdrop {
      position: fixed;
      inset: 0;
      background: rgba(15, 23, 42, 0.38);
      display: none;
      align-items: center;
      justify-content: center;
      padding: 24px;
      z-index: 20;
    }
    .modal {
      width: min(980px, 100%);
      max-height: min(760px, 90vh);
      background: var(--panel);
      border-radius: 8px;
      border: 1px solid var(--line);
      box-shadow: 0 20px 50px rgba(15, 23, 42, 0.24);
      display: flex;
      flex-direction: column;
      overflow: hidden;
    }
    .modal header {
      height: 48px;
      padding: 0 16px;
    }
    .modal-body {
      padding: 14px 16px 18px;
      overflow: auto;
    }
    pre {
      margin: 8px 0 18px;
      padding: 12px;
      background: #111827;
      color: #e5e7eb;
      border-radius: 6px;
      overflow: auto;
      white-space: pre-wrap;
      word-break: break-word;
      font: 12px/1.45 ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
    }
    @media (max-width: 980px) {
      .metrics { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      main { padding: 16px; }
      th:nth-child(6), td:nth-child(6), th:nth-child(7), td:nth-child(7) { display: none; }
    }
    @media (max-width: 640px) {
      header { padding: 0 14px; }
      .metrics { grid-template-columns: 1fr; }
      th:nth-child(4), td:nth-child(4), th:nth-child(5), td:nth-child(5) { display: none; }
    }
  </style>
</head>
<body>
  <header>
    <h1>xunfei retry proxy</h1>
    <div class="toolbar">
      <span id="updatedAt">--</span>
      <button id="refreshButton" type="button">刷新</button>
    </div>
  </header>
  <main>
    <section class="metrics" id="metrics"></section>
    <section class="panel">
      <div class="panel-header">
        <h2>请求记录</h2>
        <span class="muted" id="recordCount">--</span>
      </div>
      <table>
        <thead>
          <tr>
            <th style="width: 190px;">请求时间</th>
            <th style="width: 110px;">首字时间</th>
            <th style="width: 100px;">状态码</th>
            <th style="width: 90px;">重试轮</th>
            <th style="width: 110px;">上游请求</th>
            <th style="width: 170px;">请求 ID</th>
            <th class="right" style="width: 90px;">错误</th>
          </tr>
        </thead>
        <tbody id="recordsBody"></tbody>
      </table>
    </section>
  </main>

  <div class="modal-backdrop" id="modalBackdrop">
    <div class="modal">
      <header>
        <h1>原始错误</h1>
        <button id="closeModalButton" type="button">关闭</button>
      </header>
      <div class="modal-body">
        <div class="muted" id="errorMeta"></div>
        <h2>Headers</h2>
        <pre id="errorHeaders"></pre>
        <h2>Body</h2>
        <pre id="errorBody"></pre>
      </div>
    </div>
  </div>

  <script>
    const metricsEl = document.getElementById('metrics');
    const recordsBody = document.getElementById('recordsBody');
    const updatedAt = document.getElementById('updatedAt');
    const recordCount = document.getElementById('recordCount');
    const modalBackdrop = document.getElementById('modalBackdrop');
    const errorMeta = document.getElementById('errorMeta');
    const errorHeaders = document.getElementById('errorHeaders');
    const errorBody = document.getElementById('errorBody');

    function fmtPct(value) {
      return Number(value || 0).toFixed(1) + '%';
    }
    function fmtSeconds(ms) {
      if (ms === null || ms === undefined || ms < 0) return '--';
      return (Number(ms) / 1000).toFixed(2) + 's';
    }
    function fmtTime(value) {
      if (!value) return '--';
      return new Date(value).toLocaleString();
    }
    function escapeHTML(value) {
      return String(value ?? '').replace(/[&<>"']/g, function(ch) {
        return ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'})[ch];
      });
    }
    function metric(label, value, sub, trend) {
      let trendHTML = '';
      if (trend && trend.length) {
        const max = Math.max(1, ...trend.map(item => item.count));
        trendHTML = '<div class="trend">' + trend.map(item => {
          const height = Math.max(3, Math.round(item.count / max * 30));
          return '<div class="bar"><span style="height:' + height + 'px"></span><small>' + escapeHTML(item.label) + '</small></div>';
        }).join('') + '</div>';
      }
      return '<div class="metric"><div class="label">' + escapeHTML(label) + '</div><div class="value">' + escapeHTML(value) + '</div><div class="sub">' + escapeHTML(sub || '') + '</div>' + trendHTML + '</div>';
    }
    async function loadMetrics() {
      const res = await fetch('/admin/api/metrics', {cache: 'no-store'});
      if (!res.ok) throw new Error('load metrics failed');
      const data = await res.json();
      renderMetrics(data);
    }
    function renderMetrics(data) {
      const s = data.summary || {};
      updatedAt.textContent = '更新 ' + fmtTime(s.updated_at);
      const retryAttemptSub = String(s.retry_attempts_succeeded || 0) + '/' + String(s.retry_attempts_completed || 0);
      metricsEl.innerHTML = [
        metric('成功率', fmtPct(s.success_rate), String(s.success_requests || 0) + '/' + String(s.total_requests || 0)),
        metric('重试率', fmtPct(s.retry_rate), String(s.retried_requests || 0) + '/' + String(s.total_requests || 0)),
        metric('平均首字时间', fmtSeconds(s.average_first_byte_ms), 'client 首字节'),
        metric('重试成功率', fmtPct(s.retry_attempt_success_rate), retryAttemptSub),
        metric('5h 请求数', String(s.five_hour_success_requests || 0), '成功请求，1h 滑动', s.hour_buckets || []),
        metric('周请求数', String(s.week_success_requests || 0), '成功请求，1d 滑动', s.day_buckets || []),
        metric('月请求数', String(s.month_success_requests || 0), '本月成功请求'),
        metric('请求总数', String(s.total_requests || 0), '内存保留最近记录')
      ].join('');

      const records = data.records || [];
      recordCount.textContent = records.length + ' 条';
      recordsBody.innerHTML = records.map(row => {
        const statusClass = row.status_code === 200 ? 'status-ok' : 'status-bad';
        const button = row.error_id ? '<button type="button" data-error-id="' + escapeHTML(row.error_id) + '">查看</button>' : '<span class="muted">--</span>';
        return '<tr>' +
          '<td title="' + escapeHTML(row.request_id) + '">' + escapeHTML(fmtTime(row.started_at)) + '</td>' +
          '<td>' + escapeHTML(fmtSeconds(row.first_byte_ms)) + '</td>' +
          '<td class="' + statusClass + '">' + escapeHTML(row.status_code) + '</td>' +
          '<td>' + escapeHTML(row.retry_rounds) + '</td>' +
          '<td>' + escapeHTML(row.upstream_requests_issued) + '</td>' +
          '<td>' + escapeHTML(row.request_id) + '</td>' +
          '<td class="right">' + button + '</td>' +
        '</tr>';
      }).join('');
    }
    async function showError(id) {
      const res = await fetch('/admin/api/errors/' + encodeURIComponent(id), {cache: 'no-store'});
      if (!res.ok) throw new Error('load error failed');
      const detail = await res.json();
      errorMeta.textContent = 'request_id=' + detail.request_id + ' status=' + detail.status_code + ' time=' + fmtTime(detail.at) + (detail.truncated ? ' body 已截断' : '');
      errorHeaders.textContent = JSON.stringify(detail.headers || {}, null, 2);
      errorBody.textContent = detail.body || '';
      modalBackdrop.style.display = 'flex';
    }
    recordsBody.addEventListener('click', event => {
      const button = event.target.closest('button[data-error-id]');
      if (button) showError(button.dataset.errorId).catch(err => alert(err.message));
    });
    document.getElementById('refreshButton').addEventListener('click', () => loadMetrics().catch(err => alert(err.message)));
    document.getElementById('closeModalButton').addEventListener('click', () => modalBackdrop.style.display = 'none');
    modalBackdrop.addEventListener('click', event => {
      if (event.target === modalBackdrop) modalBackdrop.style.display = 'none';
    });
    loadMetrics().catch(err => alert(err.message));
    setInterval(() => loadMetrics().catch(() => {}), 10000);
  </script>
</body>
</html>`
