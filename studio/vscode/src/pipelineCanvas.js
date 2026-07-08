// @ts-check
// pipelineCanvas.js — Flogo Data Pipeline visual canvas, loaded as a WebView script.
;(function () {
  'use strict'

  // ── DOM helpers ─────────────────────────────────────────────────────────────
  /** @param {string} id @returns {HTMLInputElement} */
  function byId(id) { return /** @type {HTMLInputElement} */ (document.getElementById(id)) }
  /** @param {string} sel @param {ParentNode|null} [root] @returns {HTMLInputElement} */
  function qs(sel, root) { return /** @type {HTMLInputElement} */ ((root || document).querySelector(sel)) }
  /** @param {string} sel @param {ParentNode|null} [root] @returns {HTMLInputElement[]} */
  function qsa(sel, root) { return /** @type {HTMLInputElement[]} */ (Array.from((root || document).querySelectorAll(sel))) }
  /** @param {Event} e @param {string} [sel] @returns {HTMLInputElement} */
  function tgt(e, sel) {
    var t = /** @type {HTMLInputElement} */ (e.target)
    return sel ? /** @type {HTMLInputElement} */ (t.closest(sel)) : t
  }

  // ── Escape helper ──────────────────────────────────────────────────────────
  function esc(s) {
    return String(s == null ? '' : s)
      .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;').replace(/'/g, '&#39;')
  }

  // ── State ───────────────────────────────────────────────────────────────────
  var vscodeApi = null
  var state = {
    doc: null,
    selected: null,   // { kind: 'source'|'transform'|'sink'|'dlq', index: number|null }
    dirty: false,
    running: false,
    runPort: null,
    metricsInterval: null,
    dragState: null,  // for transform reorder drag
    propsSnapshot: null, // deep-copy of the config section taken when panel opens (for Discard)
  }

  // Active top tab
  var activeTopTab = 'design'

  // Run log lines
  var runLog = []
  var runMetrics = {}

  // ── Node icons ──────────────────────────────────────────────────────────────
  var SOURCE_ICONS = {
    csv: '📄', tsv: '📄', jdbc: '🗄', parquet: '📦',
    fixedwidth: '📃', flatfile: '📃', xml: '📰',
    jsonlines: '📑', ndjson: '📑', kafka: '⚡', objstore: '☁'
  }
  var TRANSFORM_ICONS = {
    filter: '🔍', map: '🔀', mapper: '🔀', validate: '✅',
    lookup: '🔎', hwm: '📈', schemadrift: '🔄', dq: '🧪'
  }
  var SINK_ICONS = {
    jdbc: '🗄', parquet: '📦', objstore: '☁', kafka: '⚡',
    dlq: '⚠', null: '🚫'
  }

  function nodeIcon(kind, type) {
    if (kind === 'source') return SOURCE_ICONS[type] || '📄'
    if (kind === 'transform') return TRANSFORM_ICONS[type] || '🔀'
    if (kind === 'sink' || kind === 'dlq') return SINK_ICONS[type] || '🗄'
    return '▪'
  }

  function nodeLabel(kind, type) {
    var labels = {
      csv: 'CSV', tsv: 'TSV', jdbc: 'JDBC', parquet: 'Parquet',
      fixedwidth: 'Fixed-Width', flatfile: 'Flat File', xml: 'XML',
      jsonlines: 'JSON Lines', ndjson: 'NDJSON', kafka: 'Kafka', objstore: 'Object Store',
      filter: 'Filter', map: 'Map', mapper: 'Mapper', validate: 'Validate',
      lookup: 'Lookup', hwm: 'HWM', schemadrift: 'Schema Drift', dq: 'Data Quality',
      null: 'Null (discard)'
    }
    return labels[type] || (type ? type.charAt(0).toUpperCase() + type.slice(1) : (kind === 'dlq' ? 'DLQ' : 'Unknown'))
  }

  function nodeSummary(kind, obj) {
    if (!obj) return ''
    if (kind === 'source') {
      if (obj.path) return obj.path
      if (obj.dsn && obj.table) return obj.table + ' (' + (obj.driver || 'jdbc') + ')'
      if (obj.brokers) {
        var b = Array.isArray(obj.brokers) ? obj.brokers.join(', ') : obj.brokers
        return obj.topic ? obj.topic + ' @ ' + b : b
      }
      if (obj.bucket) return (obj.bucket || '') + (obj.prefix ? '/' + obj.prefix : '')
    }
    if (kind === 'transform') {
      if (obj.type === 'filter') return obj.expr ? obj.expr.slice(0, 40) + (obj.expr.length > 40 ? '…' : '') : ''
      if (obj.type === 'map' || obj.type === 'mapper') {
        var fc = obj.fields ? Object.keys(obj.fields).length : 0
        return fc + ' field' + (fc !== 1 ? 's' : '')
      }
      if (obj.type === 'validate') return obj.rules ? obj.rules.length + ' rule' + (obj.rules.length !== 1 ? 's' : '') : ''
      if (obj.type === 'lookup') return obj.url ? obj.url.slice(0, 40) : ''
      if (obj.type === 'hwm') return obj.column ? 'on ' + obj.column : ''
      if (obj.type === 'schemadrift') return obj.policy || ''
      if (obj.type === 'dq') return obj.threshold != null ? 'threshold ' + obj.threshold : ''
    }
    if (kind === 'sink' || kind === 'dlq') {
      if (obj.path) return obj.path
      if (obj.dsn && obj.table) return obj.table + ' (' + (obj.driver || 'jdbc') + ')'
      if (obj.brokers) {
        var bk = Array.isArray(obj.brokers) ? obj.brokers.join(', ') : obj.brokers
        return obj.topic ? obj.topic + ' @ ' + bk : bk
      }
      if (obj.bucket) return (obj.bucket || '') + (obj.prefix ? '/' + obj.prefix : '')
    }
    return ''
  }

  // ── Single postMessage call site ─────────────────────────────────────────────
  function send(type, payload) {
    if (!vscodeApi) return
    vscodeApi.postMessage(Object.assign({ type: type }, payload || {}))
  }

  // ── Dirty tracking ──────────────────────────────────────────────────────────
  function setDirty(val) {
    state.dirty = val
    // No visible dirty indicator in the current HTML — state tracked in memory only.
  }

  function postUpdate() {
    if (!state.doc) return
    send('update', { text: JSON.stringify(state.doc, null, 2) })
    setDirty(false)
  }

  // ── Toast ───────────────────────────────────────────────────────────────────
  function showToast(msg, duration) {
    var el = byId('toast')
    if (!el) return
    el.textContent = msg
    el.className = 'toast show'
    clearTimeout(el._tid)
    el._tid = setTimeout(function () { el.className = 'toast' }, duration || 2500)
  }

  // ── Top tab switching ────────────────────────────────────────────────────────
  function switchTopTab(tab) {
    activeTopTab = tab
    qsa('.top-tab').forEach(function (t) {
      var on = t.getAttribute('data-ttab') === tab
      t.classList.toggle('active', on)
      t.setAttribute('aria-selected', on ? 'true' : 'false')
      t.setAttribute('tabindex', on ? '0' : '-1')
    })
    qsa('.ttab-content').forEach(function (p) {
      p.classList.toggle('active', p.id === 'ttab-' + tab)
    })
    if (tab === 'design') renderPipeline()
    if (tab === 'performance') renderPerfTab()
    if (tab === 'scaling') renderScalingTab()
    if (tab === 'run') renderRunTab()
  }

  // ── Bootstrap ────────────────────────────────────────────────────────────────
  function boot() {
    window.onerror = function (msg, _src, line) {
      document.body.innerHTML =
        '<div style="color:#f48771;padding:20px;font-family:monospace;white-space:pre">' +
        '<b>Canvas error (line ' + esc(String(line)) + '):</b>\n' + esc(String(msg)) + '</div>'
    }

    try { vscodeApi = acquireVsCodeApi() }
    catch (e) { document.body.innerHTML = '<div style="color:#f48771;padding:20px">acquireVsCodeApi failed: ' + esc(e.message) + '</div>'; return }

    // Load doc from embedded data
    var dataEl = byId('doc-data')
    if (dataEl && dataEl.dataset.json) {
      try { state.doc = JSON.parse(dataEl.dataset.json) } catch (_) { state.doc = {} }
    }
    if (!state.doc) state.doc = {}

    window.addEventListener('message', handleMessage)
    document.addEventListener('keydown', handleKeydown)

    setupTopTabs()
    setupMetaBar()
    render()
    wireUI()

    send('ready')
  }

  function handleMessage(e) {
    var m = e.data
    if (!m || !m.type) return
    switch (m.type) {
      case 'init':
        state.doc = m.doc || {}
        setDirty(false)
        render()
        break
      case 'update':
        // External file change (git checkout, raw edit) — refresh canvas with updated pipeline data
        if (m.doc) { state.doc = m.doc; setDirty(false); render() }
        break
      case 'runStarted':
        state.running = true
        state.runPort = m.port || null
        startMetricsPoll()
        updateRunStatus('running')
        if (activeTopTab === 'run') renderRunTab()
        break
      case 'stopped':
        state.running = false
        stopMetricsPoll()
        updateRunStatus('done')
        if (activeTopTab === 'run') renderRunTab()
        break
      case 'validationResult':
        showValidationPanel(m.issues || [])
        break
      case 'metricsResult':
        runMetrics = m.data || m.metrics || {}
        updateMetricsDisplay(runMetrics)
        break
      case 'exportDone':
        showToast('Exported to ' + (m.path || ''))
        break
      case 'runLog':
        appendRunLog(m.line || '')
        break
      case 'runError':
        state.running = false
        stopMetricsPoll()
        updateRunStatus('error')
        appendRunLog('[ERROR] ' + (m.message || 'Unknown error'))
        if (activeTopTab === 'run') renderRunTab()
        break
    }
  }

  function handleKeydown(e) {
    if ((e.ctrlKey || e.metaKey) && e.key === 's') {
      e.preventDefault()
      postUpdate()
      showToast('Saved')
    }
    if (e.key === 'Escape') { closePpPanel() }
  }

  // ── Full render ──────────────────────────────────────────────────────────────
  function render() {
    renderMetaBar()
    renderPipeline()
    renderProps()
    if (activeTopTab === 'performance') renderPerfTab()
    if (activeTopTab === 'scaling') renderScalingTab()
    if (activeTopTab === 'run') renderRunTab()
  }

  // ── Meta bar ─────────────────────────────────────────────────────────────────
  function renderMetaBar() {
    var doc = state.doc || {}
    var nameEl = byId('meta-name')
    var descEl = byId('meta-desc')
    var modeEl = byId('meta-mode')
    if (nameEl && nameEl !== document.activeElement) nameEl.value = doc.name || ''
    if (descEl && descEl !== document.activeElement) descEl.value = doc.description || ''
    if (modeEl) {
      var mode = doc.mode || 'etl'
      modeEl.textContent = mode.toUpperCase()
      modeEl.setAttribute('data-mode', mode)
      modeEl.setAttribute('aria-label', 'Pipeline mode: ' + mode.toUpperCase())
    }
  }

  function setupMetaBar() {
    var nameEl = byId('meta-name')
    var descEl = byId('meta-desc')
    var modeEl = byId('meta-mode')

    function saveMetaName() {
      if (!state.doc) return
      var v = nameEl ? nameEl.value.trim() : ''
      if (v) state.doc.name = v
      setDirty(true); postUpdate()
    }
    function saveMetaDesc() {
      if (!state.doc) return
      state.doc.description = descEl ? descEl.value : ''
      setDirty(true); postUpdate()
    }

    if (nameEl) {
      nameEl.addEventListener('blur', saveMetaName)
      nameEl.addEventListener('keydown', function (e) { if (e.key === 'Enter') { e.preventDefault(); nameEl.blur() } })
    }
    if (descEl) {
      descEl.addEventListener('blur', saveMetaDesc)
      descEl.addEventListener('keydown', function (e) { if (e.key === 'Enter') { e.preventDefault(); descEl.blur() } })
    }
    if (modeEl) {
      modeEl.addEventListener('click', cycleMode)
      modeEl.addEventListener('keydown', function (e) { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); cycleMode() } })
    }
  }

  function cycleMode() {
    if (!state.doc) return
    var modes = ['etl', 'elt', 'el']
    var cur = state.doc.mode || 'etl'
    var next = modes[(modes.indexOf(cur) + 1) % modes.length]
    state.doc.mode = next
    setDirty(true)
    renderMetaBar()
    renderPipeline()
    renderProps()
    postUpdate()
  }

  // ── Top tab setup ────────────────────────────────────────────────────────────
  function setupTopTabs() {
    var tabs = qsa('.top-tab')
    tabs.forEach(function (t, i) {
      t.addEventListener('click', function () { switchTopTab(t.getAttribute('data-ttab') || 'design') })
      t.addEventListener('keydown', function (e) {
        var k = e.key
        if (k === 'Enter' || k === ' ') { e.preventDefault(); switchTopTab(t.getAttribute('data-ttab') || 'design'); return }
        var next = -1
        if (k === 'ArrowRight' || k === 'ArrowDown') next = (i + 1) % tabs.length
        else if (k === 'ArrowLeft' || k === 'ArrowUp') next = (i - 1 + tabs.length) % tabs.length
        else if (k === 'Home') next = 0
        else if (k === 'End') next = tabs.length - 1
        if (next >= 0) { e.preventDefault(); var attr = tabs[next].getAttribute('data-ttab') || 'design'; switchTopTab(attr); tabs[next].focus() }
      })
    })
  }

  // ── Pipeline canvas render ────────────────────────────────────────────────────
  function renderPipeline() {
    var layer = byId('nodes-layer')
    var svg = byId('esvg')
    if (!layer || !svg) return
    layer.innerHTML = ''
    svg.innerHTML = ''

    var doc = state.doc || {}
    var mode = doc.mode || 'etl'
    var src = doc.source || {}
    var transforms = Array.isArray(doc.transforms) ? doc.transforms : []
    var sink = doc.sink || {}
    var dlq = doc.dlqSink || null

    // Source node
    var srcNode = makeNode('source', 0, src)
    layer.appendChild(srcNode)

    // Transform nodes (only shown in ETL and ELT modes)
    if (mode !== 'el') {
      transforms.forEach(function (tr, i) {
        var trNode = makeNode('transform', i, tr)
        layer.appendChild(trNode)
      })
      if (mode === 'elt') {
        var hint = document.createElement('div')
        hint.className = 'elt-hint'
        hint.textContent = 'Transforms run in target system'
        layer.appendChild(hint)
      }
    } else if (transforms.length > 0) {
      var warnBadge = document.createElement('div')
      warnBadge.className = 'el-warn-badge'
      warnBadge.textContent = '⚠ ' + transforms.length + ' transform(s) defined but mode is EL — they will be ignored'
      layer.appendChild(warnBadge)
    }

    // Sink node
    var sinkNode = makeNode('sink', 0, sink)
    layer.appendChild(sinkNode)

    // DLQ node
    if (dlq) {
      var dlqNode = makeNode('dlq', 0, dlq)
      layer.appendChild(dlqNode)
    }

    // Layout nodes
    layoutNodes(layer, mode, transforms.length, !!dlq)

    // Draw connectors (after layout so positions are set)
    requestAnimationFrame(function () { drawAllEdges(mode, transforms.length, !!dlq) })
  }

  function makeNode(kind, idx, obj) {
    var type = (obj && obj.type) || ''
    var icon = nodeIcon(kind, kind === 'dlq' ? (obj && obj.type || 'dlq') : type)
    var label = nodeLabel(kind, kind === 'dlq' ? (obj && obj.type || 'dlq') : type)
    var summary = nodeSummary(kind, obj)

    var div = document.createElement('div')
    div.className = 'node node-' + kind
    div.setAttribute('data-kind', kind)
    div.setAttribute('data-idx', String(idx))
    if (kind === 'dlq') div.classList.add('node-dlq-card')

    // Selected state
    var sel = state.selected
    if (sel && sel.kind === kind && (kind === 'source' || kind === 'sink' || kind === 'dlq' ? true : sel.index === idx)) {
      div.classList.add('selected')
    }

    var kindLabel = kind === 'source' ? 'Source' : kind === 'sink' ? 'Sink' : kind === 'dlq' ? 'DLQ' : 'Transform'
    var nhClass   = kind === 'source' ? 'nh-src' : kind === 'transform' ? 'nh-xform' : kind === 'sink' ? 'nh-sink' : 'nh-dlq'
    // Port dots: source gets right-side out, sink/dlq get left-side in, transforms get both
    var portsHtml = kind === 'source' ? '<div class="port-out"></div>'
      : (kind === 'sink' || kind === 'dlq') ? '<div class="port-in"></div>'
      : '<div class="port-in"></div><div class="port-out"></div>'

    div.innerHTML =
      '<div class="nh ' + nhClass + '">' +
        '<span class="nh-ico">' + icon + '</span>' +
        '<span class="nh-lbl">' + esc(kindLabel) + '</span>' +
        '<span class="nh-typ">' + esc(label) + '</span>' +
      '</div>' +
      (summary ? '<div class="nb"><div class="nb-row"><span class="nb-v" title="' + esc(summary) + '">' + esc(summary) + '</span></div></div>' : '') +
      portsHtml

    // Delete button — transforms and DLQ only (source/sink are required)
    if (kind === 'transform' || kind === 'dlq') {
      var delBtn = document.createElement('button')
      delBtn.className = 'node-del-btn'
      delBtn.title = 'Delete'
      delBtn.innerHTML = '&#10005;'
      delBtn.addEventListener('click', function (e) {
        e.stopPropagation()
        if (kind === 'transform') {
          deleteTransform(idx)
        } else if (kind === 'dlq') {
          deleteDlq()
        }
      })
      div.appendChild(delBtn)
    }

    // Universal drag — every node type is freely draggable (flogo3 pattern)
    div.addEventListener('mousedown', function (e) { onNodeDragStart(e, div, kind, idx) })

    div.addEventListener('click', function (e) {
      e.stopPropagation()
      selectNode(kind, kind === 'transform' ? idx : null)
    })

    return div
  }

  // ── Universal free-form node drag (flogo3: absolute position backed by doc.ui) ──
  function onNodeDragStart(e, div, kind, idx) {
    // Don't start drag from interactive elements or the delete button
    var tag = e.target.tagName
    if (tag === 'INPUT' || tag === 'BUTTON' || tag === 'TEXTAREA' || tag === 'SELECT' || tag === 'OPTION') return
    if (e.target.classList && e.target.classList.contains('node-del-btn')) return

    var startX = e.clientX
    var startY = e.clientY
    var startLeft = parseInt(div.style.left, 10) || 0
    var startTop  = parseInt(div.style.top, 10) || 0
    var dragging = false

    function onMove(ev) {
      var dx = ev.clientX - startX
      var dy = ev.clientY - startY
      if (!dragging && Math.sqrt(dx * dx + dy * dy) > 5) {
        dragging = true
        div.classList.add('dragging')
        e.preventDefault()
      }
      if (!dragging) return
      var newX = Math.max(0, startLeft + dx)
      var newY = Math.max(0, startTop  + dy)
      div.style.left = newX + 'px'
      div.style.top  = newY + 'px'
      // Expand canvas if node is near the edge
      var canvas = byId('canvas')
      if (canvas) {
        canvas.style.minWidth  = Math.max(parseInt(canvas.style.minWidth, 10)  || 0, newX + CARD_W + 60) + 'px'
        canvas.style.minHeight = Math.max(parseInt(canvas.style.minHeight, 10) || 0, newY + CARD_H + 60) + 'px'
      }
      requestAnimationFrame(drawAllEdges)
    }

    function onUp(ev) {
      document.removeEventListener('mousemove', onMove)
      document.removeEventListener('mouseup', onUp)
      if (!dragging) return  // was a click; click event fires selectNode
      div.classList.remove('dragging')
      var finalX = parseInt(div.style.left, 10) || 0
      var finalY = parseInt(div.style.top, 10) || 0
      setNodeUiPos(kind, idx, finalX, finalY)
      setDirty(true)
      postUpdate()
      drawAllEdges()
    }

    document.addEventListener('mousemove', onMove)
    document.addEventListener('mouseup', onUp)
  }

  // ── UI position helpers (flogo3 pattern: positions live in doc.ui, separate from pipeline config) ──
  var CARD_W = 160, CARD_H = 100, CARD_GAP = 28

  function getNodeUiPos(kind, idx) {
    var ui = (state.doc && state.doc.ui) || {}
    if (kind === 'source') return ui.source || null
    if (kind === 'sink') return ui.sink || null
    if (kind === 'dlq') return ui.dlqSink || null
    if (kind === 'transform') { var trs = ui.transforms || []; return trs[idx] || null }
    return null
  }

  function setNodeUiPos(kind, idx, x, y) {
    if (!state.doc) return
    if (!state.doc.ui) state.doc.ui = {}
    var ui = state.doc.ui
    var pos = { x: Math.round(x), y: Math.round(y) }
    if (kind === 'source') ui.source = pos
    else if (kind === 'sink') ui.sink = pos
    else if (kind === 'dlq') ui.dlqSink = pos
    else if (kind === 'transform') {
      if (!ui.transforms) ui.transforms = []
      while (ui.transforms.length <= idx) ui.transforms.push(null)
      ui.transforms[idx] = pos
    }
  }

  function layoutNodes(layer, mode, transformCount, hasDlq) {
    var CANVAS_PAD = 40
    var colSrc = CANVAS_PAD
    var colTr  = CANVAS_PAD + CARD_W + 80
    var colSink = colTr + (transformCount > 0 ? CARD_W + 80 : 0)

    var srcNodes = qsa('.node-source', layer)
    var trNodes  = qsa('.node-transform', layer)
    var sinkNodes = qsa('.node-sink', layer)
    var dlqNodes = qsa('.node-dlq-card', layer)

    // Ensure doc.ui exists so default positions are persisted
    if (!state.doc) return
    if (!state.doc.ui) state.doc.ui = {}
    var ui = state.doc.ui

    function applyPos(n, x, y) {
      n.style.position = 'absolute'
      n.style.left = x + 'px'
      n.style.top  = y + 'px'
      n.style.width = CARD_W + 'px'
    }

    // Source
    if (!ui.source) ui.source = { x: colSrc, y: CANVAS_PAD }
    srcNodes.forEach(function (n) { applyPos(n, ui.source.x, ui.source.y) })

    // Transforms — default to a vertical stack in the middle column
    if (!ui.transforms) ui.transforms = []
    trNodes.forEach(function (n, i) {
      if (!ui.transforms[i]) ui.transforms[i] = { x: colTr, y: CANVAS_PAD + i * (CARD_H + CARD_GAP) }
      applyPos(n, ui.transforms[i].x, ui.transforms[i].y)
    })

    // Sink
    if (!ui.sink) ui.sink = { x: colSink, y: CANVAS_PAD }
    sinkNodes.forEach(function (n) { applyPos(n, ui.sink.x, ui.sink.y) })

    // DLQ — default below sink
    if (hasDlq && !ui.dlqSink) ui.dlqSink = { x: colSink, y: CANVAS_PAD + CARD_H + 44 }
    dlqNodes.forEach(function (n) { if (ui.dlqSink) applyPos(n, ui.dlqSink.x, ui.dlqSink.y) })

    // Expand canvas to fit all nodes (for scroll)
    var allPos = [ui.source, ui.sink]
    ;(ui.transforms || []).forEach(function (p) { if (p) allPos.push(p) })
    if (hasDlq && ui.dlqSink) allPos.push(ui.dlqSink)
    var maxX = 0, maxY = 0
    allPos.forEach(function (p) {
      if (p) { maxX = Math.max(maxX, p.x); maxY = Math.max(maxY, p.y) }
    })
    var canvas = byId('canvas')
    if (canvas) {
      canvas.style.minWidth  = (maxX + CARD_W + 60) + 'px'
      canvas.style.minHeight = (maxY + CARD_H + 60) + 'px'
    }
  }

  function drawAllEdges(mode, transformCount, hasDlq) {
    var svg = byId('esvg')
    if (!svg) return
    svg.innerHTML = ''

    var layer = byId('nodes-layer')
    if (!layer) return

    var srcEl = qs('.node-source', layer)
    var sinkEl = qs('.node-sink', layer)
    var trEls = qsa('.node-transform', layer)
    var dlqEl = qs('.node-dlq-card', layer)

    if (!srcEl || !sinkEl) return

    if (mode !== 'el' && trEls.length > 0) {
      // Source -> first transform
      drawEdge(svg, srcEl, trEls[0], false)
      // Transform -> transform chain
      for (var i = 0; i < trEls.length - 1; i++) {
        drawEdge(svg, trEls[i], trEls[i + 1], false)
      }
      // Last transform -> sink
      drawEdge(svg, trEls[trEls.length - 1], sinkEl, false)
    } else {
      // Source -> sink direct
      drawEdge(svg, srcEl, sinkEl, false)
    }

    // Sink -> DLQ (dashed)
    if (dlqEl) {
      drawEdge(svg, sinkEl, dlqEl, true)
    }
  }

  function drawEdge(svg, fromEl, toEl, dashed) {
    var canvas = byId('canvas')
    if (!canvas) return
    var cr = canvas.getBoundingClientRect()
    var fr = fromEl.getBoundingClientRect()
    var to = toEl.getBoundingClientRect()

    var x1 = fr.right - cr.left
    var y1 = fr.top + fr.height / 2 - cr.top
    var x2 = to.left - cr.left
    var y2 = to.top + to.height / 2 - cr.top
    var cx = (x1 + x2) / 2

    var path = document.createElementNS('http://www.w3.org/2000/svg', 'path')
    path.setAttribute('d', 'M' + x1 + ',' + y1 + ' C' + cx + ',' + y1 + ' ' + cx + ',' + y2 + ' ' + x2 + ',' + y2)
    path.setAttribute('stroke', '#979797')
    path.setAttribute('stroke-width', '2')
    path.setAttribute('fill', 'none')
    if (dashed) path.setAttribute('stroke-dasharray', '6,4')

    // Arrow marker at end
    var arrow = document.createElementNS('http://www.w3.org/2000/svg', 'polygon')
    var ax = x2, ay = y2
    arrow.setAttribute('points', (ax) + ',' + ay + ' ' + (ax - 8) + ',' + (ay - 4) + ' ' + (ax - 8) + ',' + (ay + 4))
    arrow.setAttribute('fill', dashed ? '#cca700' : '#979797')

    svg.appendChild(path)
    svg.appendChild(arrow)
  }

  // ── Node selection ───────────────────────────────────────────────────────────
  function selectNode(kind, index) {
    state.selected = { kind: kind, index: index }
    qsa('.node').forEach(function (n) { n.classList.remove('selected') })
    var sel = kind === 'source'
      ? qs('.node-source')
      : kind === 'sink'
        ? qs('.node-sink')
        : kind === 'dlq'
          ? qs('.node-dlq-card')
          : qs('.node-transform[data-idx="' + index + '"]')
    if (sel) sel.classList.add('selected')

    // Snapshot current config for Discard support
    var doc = state.doc || {}
    if (kind === 'source') state.propsSnapshot = JSON.parse(JSON.stringify(doc.source || {}))
    else if (kind === 'sink') state.propsSnapshot = JSON.parse(JSON.stringify(doc.sink || {}))
    else if (kind === 'dlq') state.propsSnapshot = JSON.parse(JSON.stringify(doc.dlqSink || {}))
    else if (kind === 'transform') {
      var trs = doc.transforms || []
      state.propsSnapshot = trs[index] ? JSON.parse(JSON.stringify(trs[index])) : {}
    }

    // Update static header title — show kind + immutable type name
    var titleEl = byId('pp-hdr-title')
    if (titleEl) {
      var doc2 = state.doc || {}
      if (kind === 'source') {
        titleEl.textContent = 'Source — ' + nodeLabel('source', (doc2.source || {}).type || '')
      } else if (kind === 'sink') {
        titleEl.textContent = 'Sink — ' + nodeLabel('sink', (doc2.sink || {}).type || '')
      } else if (kind === 'dlq') {
        titleEl.textContent = 'DLQ — ' + nodeLabel('dlq', (doc2.dlqSink || {}).type || '')
      } else if (kind === 'transform') {
        var trNode = ((doc2.transforms || [])[index] || {})
        titleEl.textContent = 'Transform #' + (index + 1) + ' — ' + nodeLabel('transform', trNode.type || '')
      }
    }

    var ppEl = byId('pp')
    if (ppEl) ppEl.classList.add('pp-open')
    renderProps()
  }

  // Close the properties panel without saving (user was just reviewing)
  function closePpPanel() {
    state.selected = null
    state.propsSnapshot = null
    qsa('.node').forEach(function (n) { n.classList.remove('selected') })
    var ppEl = byId('pp')
    if (ppEl) {
      ppEl.style.height = ''  // clear resize-drag inline style so CSS class controls height
      ppEl.classList.remove('pp-open')
    }
    var titleEl = byId('pp-hdr-title')
    if (titleEl) titleEl.textContent = 'Properties'
    var pp = byId('pp-content')
    if (pp) pp.innerHTML = '<div class="pp-empty" id="pp-empty"><span class="pp-empty-ico">&#9670;</span><span>Select a node to configure it</span></div>'
  }

  // Discard edits — restore the snapshot taken when panel opened, then close
  function discardProps() {
    var sel = state.selected
    var snap = state.propsSnapshot
    if (sel && snap) {
      var doc = state.doc || {}
      if (sel.kind === 'source') doc.source = snap
      else if (sel.kind === 'sink') doc.sink = snap
      else if (sel.kind === 'dlq') doc.dlqSink = snap
      else if (sel.kind === 'transform') {
        var trs = doc.transforms || []
        if (sel.index != null && trs[sel.index]) trs[sel.index] = snap
      }
      postUpdate()
      renderPipeline()
    }
    state.selected = null
    state.propsSnapshot = null
    qsa('.node').forEach(function (n) { n.classList.remove('selected') })
    var ppEl = byId('pp')
    if (ppEl) {
      ppEl.style.height = ''
      ppEl.classList.remove('pp-open')
    }
    var titleEl = byId('pp-hdr-title')
    if (titleEl) titleEl.textContent = 'Properties'
    var pp = byId('pp-content')
    if (pp) pp.innerHTML = '<div class="pp-empty" id="pp-empty"><span class="pp-empty-ico">&#9670;</span><span>Select a node to configure it</span></div>'
  }

  // ── Properties panel ─────────────────────────────────────────────────────────
  function renderProps() {
    var pp = byId('pp-content')
    if (!pp) return
    var sel = state.selected
    if (!sel) {
      pp.innerHTML = '<div class="pp-empty">Select a stage on the canvas to configure it.</div>'
      return
    }
    if (sel.kind === 'source') renderSourceProps(pp)
    else if (sel.kind === 'transform') renderTransformProps(pp, sel.index)
    else if (sel.kind === 'sink') renderSinkProps(pp)
    else if (sel.kind === 'dlq') renderDlqProps(pp)
  }

  // ── Source props ──────────────────────────────────────────────────────────────
  function renderSourceProps(pp) {
    var doc = state.doc || {}
    var src = doc.source || {}
    var type = src.type || 'csv'

    var html = '<div class="pp-fields-wrap">'
    html += renderSourceTypeFields(type, src)
    html += '</div>'

    html += '<div class="pp-action-bar">' +
      '<button class="btn-p" id="btn-src-save">Save</button>' +
      '<button class="btn-g" id="btn-src-discard">Discard</button>' +
      '</div>'
    pp.innerHTML = html

    wireSourceFields(type, src)

    var saveBtn = byId('btn-src-save')
    if (saveBtn) saveBtn.addEventListener('click', function () { saveSourceProps(); closePpPanel() })
    var discardBtn = byId('btn-src-discard')
    if (discardBtn) discardBtn.addEventListener('click', discardProps)
  }

  function renderColGrid(id, columns) {
    var colDefs = Array.isArray(columns) ? columns : []
    var colTypes = ['string', 'int', 'float', 'bool', 'timestamp', 'bytes']
    var rows = colDefs.map(function (c) {
      var name = typeof c === 'string' ? c : (c.name || '')
      var colType = typeof c === 'object' ? (c.type || 'string') : 'string'
      var opts = colTypes.map(function (t) {
        return '<option value="' + t + '"' + (t === colType ? ' selected' : '') + '>' + t + '</option>'
      }).join('')
      return '<div class="col-row">' +
        '<input class="pp-input col-name" type="text" value="' + esc(name) + '" placeholder="column_name"/>' +
        '<select class="pp-input col-type">' + opts + '</select>' +
        '<button class="col-del-row" data-action="del-col-row" title="Remove">×</button>' +
        '</div>'
    }).join('')
    return '<div class="mapper-toolbar"><span class="mapper-toolbar-label">Columns</span>' +
      '<button class="btn-g" id="btn-add-col-row" style="font-size:11px;padding:2px 8px">+ Add column</button></div>' +
      '<div class="cols-grid" id="' + id + '">' +
        '<div class="mapper-col-hdr">Column Name</div>' +
        '<div class="mapper-col-hdr">Type</div>' +
        '<div></div>' +
        rows +
      '</div>'
  }

  function wireColEditor(editorId) {
    var editor = byId(editorId)
    var addBtn = byId('btn-add-col-row')
    var colTypes = ['string', 'int', 'float', 'bool', 'timestamp', 'bytes']
    if (editor) {
      editor.addEventListener('click', function (e) {
        if (tgt(e, '[data-action="del-col-row"]')) {
          var row = tgt(e).closest('.col-row')
          if (row) row.remove()
        }
      })
    }
    if (addBtn) {
      addBtn.addEventListener('click', function () {
        if (!editor) return
        var opts = colTypes.map(function (t) {
          return '<option value="' + t + '"' + (t === 'string' ? ' selected' : '') + '>' + t + '</option>'
        }).join('')
        var row = document.createElement('div')
        row.className = 'col-row'
        row.innerHTML =
          '<input class="pp-input col-name" type="text" value="" placeholder="column_name"/>' +
          '<select class="pp-input col-type">' + opts + '</select>' +
          '<button class="col-del-row" data-action="del-col-row" title="Remove">×</button>'
        editor.appendChild(row)
        var inp = qs('.col-name', row); if (inp) inp.focus()
      })
    }
  }

  function collectColRows(editorId) {
    var editor = byId(editorId)
    if (!editor) return undefined
    var cols = []
    qsa('.col-row', editor).forEach(function (row) {
      var nameEl = qs('.col-name', row), typeEl = qs('.col-type', row)
      var name = nameEl ? nameEl.value.trim() : ''
      var type = typeEl ? typeEl.value : 'string'
      if (name) cols.push({ name: name, type: type })
    })
    return cols.length ? cols : undefined
  }

  function renderSourceTypeFields(type, src) {
    var html = ''
    if (type === 'csv' || type === 'tsv') {
      html += ppField('Path', '<input class="pp-input" id="src-path" type="text" value="' + esc(src.path || '') + '" placeholder="/data/input.csv"/>')
      if (type === 'csv') html += ppField('Delimiter', '<input class="pp-input" id="src-delim" type="text" value="' + esc(src.delimiter || ',') + '" placeholder=","/>')
      html += ppField('Has Header', ppCheck('src-header', src.hasHeader !== false))
      html += renderColGrid('src-cols-editor', src.columns)
    } else if (type === 'jdbc') {
      html += ppField('Driver', ppSelect('src-driver', ['postgres', 'mysql', 'sqlite', 'mssql'], src.driver || 'postgres'))
      html += ppField('DSN / Connection String', '<input class="pp-input" id="src-dsn" type="text" value="' + esc(src.dsn || '') + '" placeholder="host=localhost dbname=mydb user=u password=p sslmode=disable"/>')
      html += ppField('Table', '<input class="pp-input" id="src-table" type="text" value="' + esc(src.table || '') + '" placeholder="my_table"/>')
      html += ppField('Query (optional)', '<textarea class="pp-textarea" id="src-query" rows="3" placeholder="SELECT * FROM my_table WHERE ...">' + esc(src.query || '') + '</textarea>')
      html += ppField('Key Column', '<input class="pp-input" id="src-keycol" type="text" value="' + esc(src.keyColumn || '') + '" placeholder="id"/>')
      html += ppField('Partitions', '<input class="pp-input" id="src-parts" type="number" min="1" max="64" value="' + esc(src.partitions || 1) + '"/>')
    } else if (type === 'parquet') {
      html += ppField('Path', '<input class="pp-input" id="src-path" type="text" value="' + esc(src.path || '') + '" placeholder="s3://bucket/prefix/*.parquet"/>')
      html += ppField('Columns (JSON array, optional)', '<textarea class="pp-textarea" id="src-cols" rows="3" placeholder=\'["col1","col2"]\'>' + esc(src.columns ? JSON.stringify(src.columns) : '') + '</textarea>')
      html += ppField('Row Group Parallelism (1–8)', '<input class="pp-input" id="src-rgp" type="number" min="1" max="8" value="' + esc(src.rowGroupParallelism || 4) + '"/>')
    } else if (type === 'fixedwidth') {
      html += ppField('Path', '<input class="pp-input" id="src-path" type="text" value="' + esc(src.path || '') + '" placeholder="/mnt/mainframe/file.dat"/>')
      html += ppField('Encoding', ppSelect('src-enc', ['utf-8', 'ebcdic-037', 'ebcdic-500'], src.encoding || 'utf-8'))
      html += ppField('Record Length', '<input class="pp-input" id="src-reclen" type="number" min="1" value="' + esc(src.recordLength || 80) + '"/>')
      html += ppField('Skip Lines', '<input class="pp-input" id="src-skip" type="number" min="0" value="' + esc(src.skipLines || 0) + '"/>')
      html += ppField('Columns (JSON)', '<textarea class="pp-textarea" id="src-cols" rows="5" placeholder=\'[{"name":"id","start":1,"length":10,"type":"string"}]\'>' + esc(src.columns ? JSON.stringify(src.columns, null, 2) : '') + '</textarea>')
    } else if (type === 'xml') {
      html += ppField('Path', '<input class="pp-input" id="src-path" type="text" value="' + esc(src.path || '') + '" placeholder="/data/records.xml"/>')
      html += ppField('Record Element', '<input class="pp-input" id="src-recel" type="text" value="' + esc(src.recordElement || '') + '" placeholder="record"/>')
      html += renderColGrid('src-cols-editor', src.columns)
    } else if (type === 'jsonlines' || type === 'ndjson') {
      html += ppField('Path', '<input class="pp-input" id="src-path" type="text" value="' + esc(src.path || '') + '" placeholder="/data/records.ndjson"/>')
      html += renderColGrid('src-cols-editor', src.columns)
    } else if (type === 'kafka') {
      var brokersVal = Array.isArray(src.brokers) ? src.brokers.join(', ') : (src.brokers || '')
      html += ppField('Brokers (comma-separated)', '<input class="pp-input" id="src-brokers" type="text" value="' + esc(brokersVal) + '" placeholder="kafka:9092"/>')
      html += ppField('Topic', '<input class="pp-input" id="src-topic" type="text" value="' + esc(src.topic || '') + '"/>')
      html += ppField('Group ID', '<input class="pp-input" id="src-groupid" type="text" value="' + esc(src.groupId || '') + '" placeholder="my-pipeline-consumer"/>')
      html += ppField('SASL', ppCheck('src-sasl', !!src.sasl))
      html += ppField('SCRAM Algorithm', ppSelect('src-scram', ['SHA-256', 'SHA-512'], src.scram || 'SHA-256'))
      html += ppField('Username Env Var', '<input class="pp-input" id="src-uenv" type="text" value="' + esc(src.usernameEnv || '') + '" placeholder="KAFKA_USERNAME"/>')
      html += ppField('Password Env Var', '<input class="pp-input" id="src-penv" type="text" value="' + esc(src.passwordEnv || '') + '" placeholder="KAFKA_PASSWORD"/>')
    } else if (type === 'objstore') {
      html += ppField('Provider', ppSelect('src-provider', ['s3', 'gcs', 'adls', 'azure-blob'], src.provider || 's3'))
      html += ppField('Bucket', '<input class="pp-input" id="src-bucket" type="text" value="' + esc(src.bucket || '') + '"/>')
      html += ppField('Prefix', '<input class="pp-input" id="src-prefix" type="text" value="' + esc(src.prefix || '') + '"/>')
      html += ppField('Pattern', '<input class="pp-input" id="src-pattern" type="text" value="' + esc(src.pattern || '*.csv') + '" placeholder="*.csv"/>')
      html += ppField('Format', ppSelect('src-fmt', ['csv', 'parquet', 'jsonlines'], src.format || 'csv'))
    }
    return html
  }

  function wireSourceFields(type, src) {
    if (type === 'csv' || type === 'tsv' || type === 'xml' || type === 'jsonlines' || type === 'ndjson') {
      wireColEditor('src-cols-editor')
    }
  }

  function saveSourceProps() {
    if (!state.doc) return
    if (!state.doc.source) state.doc.source = {}
    var s = state.doc.source
    var type = s.type || 'csv'

    var v = function (id) { var el = byId(id); return el ? el.value : '' }
    var checked = function (id) { var el = byId(id); return el ? el.checked : false }
    var jsonParse = function (id) {
      var el = byId(id); if (!el || !el.value.trim()) return undefined
      try { return JSON.parse(el.value) } catch (_) { showToast('Invalid JSON in ' + id); return undefined }
    }

    if (type === 'csv' || type === 'tsv') {
      s.path = v('src-path')
      if (type === 'csv') s.delimiter = v('src-delim') || ','
      s.hasHeader = checked('src-header')
      var cols = collectColRows('src-cols-editor'); if (cols !== undefined) s.columns = cols
    } else if (type === 'jdbc') {
      s.driver = v('src-driver')
      s.dsn = v('src-dsn')
      s.table = v('src-table')
      s.query = v('src-query') || undefined
      s.keyColumn = v('src-keycol') || undefined
      s.partitions = parseInt(v('src-parts')) || 1
    } else if (type === 'parquet') {
      s.path = v('src-path')
      var pcols = jsonParse('src-cols'); if (pcols !== undefined) s.columns = pcols
      s.rowGroupParallelism = parseInt(v('src-rgp')) || 4
    } else if (type === 'fixedwidth') {
      s.path = v('src-path')
      s.encoding = v('src-enc')
      s.recordLength = parseInt(v('src-reclen')) || 80
      s.skipLines = parseInt(v('src-skip')) || 0
      var fcols = jsonParse('src-cols'); if (fcols !== undefined) s.columns = fcols
    } else if (type === 'xml') {
      s.path = v('src-path')
      s.recordElement = v('src-recel')
      var xcols = collectColRows('src-cols-editor'); if (xcols !== undefined) s.columns = xcols
    } else if (type === 'jsonlines' || type === 'ndjson') {
      s.path = v('src-path')
      var jcols = collectColRows('src-cols-editor'); if (jcols !== undefined) s.columns = jcols
    } else if (type === 'kafka') {
      var bv = v('src-brokers')
      s.brokers = bv ? bv.split(',').map(function (x) { return x.trim() }) : []
      s.topic = v('src-topic')
      s.groupId = v('src-groupid')
      s.sasl = checked('src-sasl')
      s.scram = v('src-scram')
      s.usernameEnv = v('src-uenv') || undefined
      s.passwordEnv = v('src-penv') || undefined
    } else if (type === 'objstore') {
      s.provider = v('src-provider')
      s.bucket = v('src-bucket')
      s.prefix = v('src-prefix') || undefined
      s.pattern = v('src-pattern') || undefined
      s.format = v('src-fmt')
    }

    setDirty(true)
    renderPipeline()
    postUpdate()
    showToast('Source updated')
  }

  // ── Transform props ───────────────────────────────────────────────────────────
  function renderTransformProps(pp, idx) {
    var doc = state.doc || {}
    var transforms = Array.isArray(doc.transforms) ? doc.transforms : []
    var tr = transforms[idx] || {}
    var type = tr.type || 'filter'

    var html = '<div class="pp-fields-wrap">'
    html += renderTransformTypeFields(type, tr, idx)
    html += '</div>'

    html += '<div class="pp-action-bar">' +
      '<button class="btn-p" id="btn-tr-save">Save</button>' +
      '<button class="btn-g" id="btn-tr-discard">Discard</button>' +
      '<button class="btn-d" id="btn-tr-del" title="Delete this transform" style="margin-left:auto">Delete</button>' +
      '</div>'

    pp.innerHTML = html

    var delBtn = byId('btn-tr-del')
    if (delBtn) delBtn.addEventListener('click', function () { deleteTransform(idx) })

    var saveBtn = byId('btn-tr-save')
    if (saveBtn) saveBtn.addEventListener('click', function () { saveTransformProps(idx); closePpPanel() })
    var discardBtn = byId('btn-tr-discard')
    if (discardBtn) discardBtn.addEventListener('click', discardProps)

    // Map field editor wiring
    if (type === 'map' || type === 'mapper') wireMapEditor(tr)
  }

  function renderTransformTypeFields(type, tr, trIdx) {
    var html = ''
    if (type === 'filter') {
      html += ppField('Expression', '<textarea class="pp-textarea" id="tr-expr" rows="4" placeholder="quantity > 0 AND status != \'CANCELLED\'">' + esc(tr.expr || '') + '</textarea>')
      html += '<div class="pp-hint">Supported: field op value, AND / OR, IS NULL, IN (...), comparison operators (=, !=, &lt;, &gt;, &lt;=, &gt;=).</div>'
    } else if (type === 'map' || type === 'mapper') {
      // Build available-fields list from source columns + upstream transforms
      var doc2 = state.doc || {}
      var srcCols = (doc2.source && Array.isArray(doc2.source.columns)) ? doc2.source.columns : []
      var availHtml = ''
      if (srcCols.length) {
        availHtml += '<div class="avail-section">Source</div>'
        srcCols.forEach(function (c) {
          var n = typeof c === 'string' ? c : (c.name || '')
          var t = typeof c === 'object' ? (c.type || 'any') : 'any'
          if (n) availHtml += '<div class="avail-field" data-field="' + esc(n) + '" title="' + esc(t) + '">' +
            '<span class="avail-type">' + esc(t.substring(0,3)) + '</span>' +
            '<span class="avail-name">$' + esc(n) + '</span>' +
            '</div>'
        })
      }
      // Also show any output fields from preceding lookup transforms
      var transforms = Array.isArray(doc2.transforms) ? doc2.transforms : []
      transforms.slice(0, trIdx || 0).forEach(function (prev) {
        if (prev.type === 'lookup' && Array.isArray(prev.outputFields) && prev.outputFields.length) {
          availHtml += '<div class="avail-section">Lookup</div>'
          prev.outputFields.forEach(function (f) {
            availHtml += '<div class="avail-field" data-field="' + esc(f) + '" title="string">' +
              '<span class="avail-type">str</span>' +
              '<span class="avail-name">$' + esc(f) + '</span>' +
              '</div>'
          })
        }
      })
      if (!availHtml) availHtml = '<div class="avail-empty">No schema defined on Source</div>'

      html += '<div class="mapper-host">' +
        '<div class="mapper-avail">' +
          '<div class="mapper-panel-hdr">Available Fields</div>' +
          '<div class="avail-list" id="avail-list">' + availHtml + '</div>' +
        '</div>' +
        '<div class="mapper-main">' +
          '<div class="mapper-toolbar"><span class="mapper-toolbar-label">Mappings</span>' +
            '<button class="btn-g" id="btn-add-map-row" style="font-size:11px;padding:2px 8px">+ Add</button></div>' +
          '<div class="mapper-grid" id="map-fields-editor">' +
            '<div class="mapper-col-hdr">Output Field</div>' +
            '<div class="mapper-col-hdr">Expression</div>' +
            '<div></div>' +
            renderMapFieldRows(tr.fields || {}) +
          '</div>' +
        '</div>' +
      '</div>'
    } else if (type === 'validate') {
      html += ppField('Rules (JSON)', '<textarea class="pp-textarea" id="tr-rules" rows="5" placeholder=\'[{"field":"id","notNull":true}]\'>' + esc(tr.rules ? JSON.stringify(tr.rules, null, 2) : '') + '</textarea>')
      html += ppField('On Violation', ppSelect('tr-onviolation', ['route', 'drop', 'fail'], tr.onViolation || 'drop'))
      html += '<div id="tr-errsinkt-wrap">' + ppField('Error Sink Type', '<input class="pp-input" id="tr-errsinktype" type="text" value="' + esc(tr.errorSinkType || '') + '" placeholder="jdbc"/>') + '</div>'
    } else if (type === 'lookup') {
      html += ppField('URL', '<input class="pp-input" id="tr-url" type="text" value="' + esc(tr.url || '') + '" placeholder="http://service/lookup"/>')
      html += ppField('Key Field', '<input class="pp-input" id="tr-keyfield" type="text" value="' + esc(tr.keyField || '') + '"/>')
      html += ppField('Output Fields (comma-separated)', '<input class="pp-input" id="tr-outfields" type="text" value="' + esc(Array.isArray(tr.outputFields) ? tr.outputFields.join(', ') : (tr.outputFields || '')) + '"/>')
      html += ppField('Concurrency', '<input class="pp-input" id="tr-concur" type="number" min="1" max="128" value="' + esc(tr.concurrency || 8) + '"/>')
      html += ppField('Cache Size', '<input class="pp-input" id="tr-cache" type="number" min="0" value="' + esc(tr.cacheSize || 1000) + '"/>')
      html += ppField('Timeout (ms)', '<input class="pp-input" id="tr-timeout" type="number" min="0" value="' + esc(tr.timeoutMs || 5000) + '"/>')
      html += ppField('On Error', ppSelect('tr-onerr', ['null', 'skip', 'fail'], tr.onError || 'null'))
    } else if (type === 'hwm') {
      html += ppField('HWM Column', '<input class="pp-input" id="tr-hwmcol" type="text" value="' + esc(tr.column || '') + '" placeholder="updated_at"/>')
      html += ppField('Backend', ppSelect('tr-hwmbe', ['file', 'redis', 'postgres'], tr.backend || 'file'))
      html += ppField('Backend Config (JSON)', '<textarea class="pp-textarea" id="tr-hwmcfg" rows="3" placeholder=\'{"path":"/var/flogo/hwm"}\'>' + esc(tr.backendConfig ? JSON.stringify(tr.backendConfig, null, 2) : '') + '</textarea>')
    } else if (type === 'schemadrift') {
      html += ppField('Policy', ppSelect('tr-sdpol', ['additive', 'strict', 'drop', 'project'], tr.policy || 'additive'))
      html += '<div class="pp-hint">additive: new columns added; strict: reject drift; drop: silently drop rows with drift; project: strip unknown columns.</div>'
    } else if (type === 'dq') {
      html += ppField('Expectations (JSON)', '<textarea class="pp-textarea" id="tr-dqexp" rows="5" placeholder=\'[{"field":"id","notNull":true,"pattern":"^[A-Z0-9]+$"}]\'>' + esc(tr.expectations ? JSON.stringify(tr.expectations, null, 2) : '') + '</textarea>')
      html += ppField('Threshold (0–1)', '<div style="display:flex;align-items:center;gap:8px"><input type="range" id="tr-dqthr" min="0" max="1" step="0.01" value="' + esc(tr.threshold != null ? tr.threshold : 0.95) + '" style="flex:1"/><span id="tr-dqthr-val">' + esc(tr.threshold != null ? tr.threshold : 0.95) + '</span></div>')
      html += ppField('On Fail', ppSelect('tr-dqfail', ['abort', 'report', 'pass'], tr.onFail || 'report'))

      // Wire threshold slider live
      setTimeout(function () {
        var sl = byId('tr-dqthr'); var vl = byId('tr-dqthr-val')
        if (sl && vl) sl.addEventListener('input', function () { vl.textContent = sl.value })
      }, 0)
    }
    return html
  }

  function renderMapFieldRows(fields) {
    var html = ''
    Object.keys(fields).forEach(function (k) {
      html += '<div class="map-row">' +
        '<input class="pp-input map-out" type="text" value="' + esc(k) + '" placeholder="output_field"/>' +
        '<input class="pp-input map-expr" type="text" value="' + esc(fields[k]) + '" placeholder="=$source_field or =expr(...)"/>' +
        '<button class="map-del-row" data-action="del-map-row" title="Remove">×</button>' +
        '</div>'
    })
    return html
  }

  function makeMapRow() {
    var div = document.createElement('div')
    div.className = 'map-row'
    div.innerHTML =
      '<input class="pp-input map-out" type="text" value="" placeholder="output_field"/>' +
      '<input class="pp-input map-expr" type="text" value="" placeholder="=$source_field or =expr(...)"/>' +
      '<button class="map-del-row" data-action="del-map-row" title="Remove">×</button>'
    return div
  }

  function wireMapEditor(tr) {
    var editor = byId('map-fields-editor')
    var addBtn = byId('btn-add-map-row')
    var availList = byId('avail-list')
    var lastFocusedExpr = null  // track which expr input last had focus

    if (editor) {
      editor.addEventListener('click', function (e) {
        if (tgt(e, '[data-action="del-map-row"]')) {
          var row = tgt(e).closest('.map-row')
          if (row) row.remove()
        }
      })
      // Track last focused expression input for available-field insertion
      editor.addEventListener('focusin', function (e) {
        if (e.target && e.target.classList.contains('map-expr')) {
          lastFocusedExpr = e.target
        }
      })
    }

    if (addBtn) {
      addBtn.addEventListener('click', function () {
        if (!editor) return
        var row = makeMapRow()
        editor.appendChild(row)
        var inp = qs('.map-out', row); if (inp) inp.focus()
      })
    }

    // Clicking an available field: if an expression input is focused, append/set;
    // otherwise add a new row pre-filled with the field reference.
    if (availList) {
      availList.addEventListener('click', function (e) {
        var el = tgt(e, '.avail-field')
        if (!el) return
        var field = el.dataset.field
        if (!field) return
        var ref = '=$' + field
        if (lastFocusedExpr && document.contains(lastFocusedExpr)) {
          // Append at cursor or replace selection
          var start = lastFocusedExpr.selectionStart
          var end = lastFocusedExpr.selectionEnd
          var cur = lastFocusedExpr.value
          lastFocusedExpr.value = cur.substring(0, start) + ref + cur.substring(end)
          lastFocusedExpr.selectionStart = lastFocusedExpr.selectionEnd = start + ref.length
          lastFocusedExpr.focus()
        } else {
          // Add a new mapping row pre-filled with this field
          var row = makeMapRow()
          qs('.map-out', row).value = field
          qs('.map-expr', row).value = ref
          editor.appendChild(row)
          qs('.map-expr', row).focus()
        }
      })
    }
  }

  function collectMapFields() {
    var editor = byId('map-fields-editor')
    if (!editor) return {}
    var fields = {}
    qsa('.map-row', editor).forEach(function (row) {
      var kEl = qs('.map-out', row), vEl = qs('.map-expr', row)
      var k = kEl ? kEl.value : '', v = vEl ? vEl.value : ''
      if (k.trim()) fields[k.trim()] = v
    })
    return fields
  }

  function deleteTransform(idx) {
    if (!state.doc) return
    var transforms = state.doc.transforms || []
    transforms.splice(idx, 1)
    state.doc.transforms = transforms
    // Remove stored UI position for this transform and re-index remaining
    if (state.doc.ui && Array.isArray(state.doc.ui.transforms)) {
      state.doc.ui.transforms.splice(idx, 1)
    }
    closePpPanel()
    setDirty(true)
    render()
    postUpdate()
    showToast('Transform removed')
  }

  function deleteDlq() {
    if (!state.doc) return
    delete state.doc.dlqSink
    if (state.doc.ui) delete state.doc.ui.dlqSink
    closePpPanel()
    setDirty(true)
    render()
    postUpdate()
    showToast('Dead Letter Queue removed')
  }

  function saveTransformProps(idx) {
    if (!state.doc) return
    if (!state.doc.transforms) state.doc.transforms = []
    var trs = state.doc.transforms
    if (!trs[idx]) trs[idx] = {}
    var tr = trs[idx]
    var v = function (id) { var el = byId(id); return el ? el.value : '' }
    var checked = function (id) { var el = byId(id); return el ? el.checked : false }
    var jsonParse = function (id) {
      var el = byId(id); if (!el || !el.value.trim()) return undefined
      try { return JSON.parse(el.value) } catch (_) { showToast('Invalid JSON in ' + id); return undefined }
    }

    var type = tr.type || 'filter'

    if (type === 'filter') {
      tr.expr = v('tr-expr')
    } else if (type === 'map' || type === 'mapper') {
      tr.fields = collectMapFields()
    } else if (type === 'validate') {
      var rules = jsonParse('tr-rules'); if (rules !== undefined) tr.rules = rules
      tr.onViolation = v('tr-onviolation')
      if (tr.onViolation === 'route') tr.errorSinkType = v('tr-errsinktype') || undefined
      else delete tr.errorSinkType
    } else if (type === 'lookup') {
      tr.url = v('tr-url')
      tr.keyField = v('tr-keyfield')
      var of2 = v('tr-outfields')
      tr.outputFields = of2 ? of2.split(',').map(function (x) { return x.trim() }) : []
      tr.concurrency = parseInt(v('tr-concur')) || 8
      tr.cacheSize = parseInt(v('tr-cache')) || 0
      tr.timeoutMs = parseInt(v('tr-timeout')) || 5000
      tr.onError = v('tr-onerr')
    } else if (type === 'hwm') {
      tr.column = v('tr-hwmcol')
      tr.backend = v('tr-hwmbe')
      var hwmcfg = jsonParse('tr-hwmcfg'); if (hwmcfg !== undefined) tr.backendConfig = hwmcfg
    } else if (type === 'schemadrift') {
      tr.policy = v('tr-sdpol')
    } else if (type === 'dq') {
      var dqexp = jsonParse('tr-dqexp'); if (dqexp !== undefined) tr.expectations = dqexp
      var thrEl = byId('tr-dqthr'); if (thrEl) tr.threshold = parseFloat(thrEl.value)
      tr.onFail = v('tr-dqfail')
    }

    setDirty(true)
    renderPipeline()
    postUpdate()
    showToast('Transform updated')
  }

  // ── Sink props ────────────────────────────────────────────────────────────────
  function renderSinkProps(pp) {
    var doc = state.doc || {}
    var sink = doc.sink || {}
    var type = sink.type || 'jdbc'

    var html = '<div class="pp-fields-wrap">'
    html += renderSinkTypeFields(type, sink)
    html += '</div>'

    html += '<div class="pp-action-bar">' +
      '<button class="btn-p" id="btn-sink-save">Save</button>' +
      '<button class="btn-g" id="btn-sink-discard">Discard</button>' +
      '</div>'

    pp.innerHTML = html

    var saveBtn = byId('btn-sink-save')
    if (saveBtn) saveBtn.addEventListener('click', function () { saveSinkProps(); closePpPanel() })
    var discardBtn = byId('btn-sink-discard')
    if (discardBtn) discardBtn.addEventListener('click', discardProps)
  }

  function renderSinkTypeFields(type, sink) {
    var html = ''
    if (type === 'jdbc') {
      html += ppField('Driver', ppSelect('sink-driver', ['postgres', 'mysql', 'sqlite', 'mssql'], sink.driver || 'postgres'))
      html += ppField('DSN / Connection String', '<input class="pp-input" id="sink-dsn" type="text" value="' + esc(sink.dsn || '') + '" placeholder="host=localhost dbname=mydb user=u password=p"/>')
      html += ppField('Table', '<input class="pp-input" id="sink-table" type="text" value="' + esc(sink.table || '') + '"/>')
      html += ppField('Batch Size', '<input class="pp-input" id="sink-batch" type="number" min="1" value="' + esc(sink.batchSize || 1000) + '"/>')
      html += ppField('On Conflict', ppSelect('sink-conflict', ['insert', 'upsert', 'replace', 'ignore'], sink.onConflict || 'insert'))
      html += ppField('Conflict Key (comma-separated)', '<input class="pp-input" id="sink-confkey" type="text" value="' + esc(Array.isArray(sink.conflictKey) ? sink.conflictKey.join(', ') : (sink.conflictKey || '')) + '" placeholder="id"/>')
    } else if (type === 'parquet') {
      html += ppField('Path', '<input class="pp-input" id="sink-path" type="text" value="' + esc(sink.path || '') + '" placeholder="/data/output/"/>')
      html += ppField('Row Group Size', '<input class="pp-input" id="sink-rgs" type="number" min="1000" value="' + esc(sink.rowGroupSize || 100000) + '"/>')
    } else if (type === 'objstore') {
      html += ppField('Provider', ppSelect('sink-provider', ['s3', 'gcs', 'adls', 'azure-blob'], sink.provider || 's3'))
      html += ppField('Bucket', '<input class="pp-input" id="sink-bucket" type="text" value="' + esc(sink.bucket || '') + '"/>')
      html += ppField('Prefix', '<input class="pp-input" id="sink-prefix" type="text" value="' + esc(sink.prefix || '') + '"/>')
      html += ppField('Format', ppSelect('sink-fmt', ['csv', 'parquet', 'jsonlines'], sink.format || 'parquet'))
    } else if (type === 'kafka') {
      var brokersVal = Array.isArray(sink.brokers) ? sink.brokers.join(', ') : (sink.brokers || '')
      html += ppField('Brokers (comma-separated)', '<input class="pp-input" id="sink-brokers" type="text" value="' + esc(brokersVal) + '" placeholder="kafka:9092"/>')
      html += ppField('Topic', '<input class="pp-input" id="sink-topic" type="text" value="' + esc(sink.topic || '') + '"/>')
      html += ppField('SASL', ppCheck('sink-sasl', !!sink.sasl))
      html += ppField('SCRAM Algorithm', ppSelect('sink-scram', ['SHA-256', 'SHA-512'], sink.scram || 'SHA-256'))
      html += ppField('Username Env Var', '<input class="pp-input" id="sink-uenv" type="text" value="' + esc(sink.usernameEnv || '') + '"/>')
      html += ppField('Password Env Var', '<input class="pp-input" id="sink-penv" type="text" value="' + esc(sink.passwordEnv || '') + '"/>')
      html += ppField('Batch Size', '<input class="pp-input" id="sink-batch" type="number" min="1" value="' + esc(sink.batchSize || 500) + '"/>')
    } else if (type === 'dlq') {
      html += ppField('Inner Sink Type', ppSelect('sink-innertype', ['jdbc', 'parquet', 'objstore', 'kafka', 'null'], sink.innerType || 'jdbc'))
      html += ppField('Inner Options (JSON)', '<textarea class="pp-textarea" id="sink-inneropts" rows="4" placeholder=\'{"driver":"postgres","dsn":"...","table":"dlq"}\'>' + esc(sink.innerOptions ? JSON.stringify(sink.innerOptions, null, 2) : '') + '</textarea>')
    } else if (type === 'null') {
      html += '<div class="pp-hint" style="padding:12px">Discard all rows — useful for testing the source and transforms without writing to a real destination.</div>'
    }
    return html
  }

  function saveSinkProps() {
    if (!state.doc) return
    if (!state.doc.sink) state.doc.sink = {}
    var s = state.doc.sink
    var v = function (id) { var el = byId(id); return el ? el.value : '' }
    var checked = function (id) { var el = byId(id); return el ? el.checked : false }
    var jsonParse = function (id) {
      var el = byId(id); if (!el || !el.value.trim()) return undefined
      try { return JSON.parse(el.value) } catch (_) { showToast('Invalid JSON in ' + id); return undefined }
    }

    var type = s.type || 'jdbc'

    if (type === 'jdbc') {
      s.driver = v('sink-driver')
      s.dsn = v('sink-dsn')
      s.table = v('sink-table')
      s.batchSize = parseInt(v('sink-batch')) || 1000
      s.onConflict = v('sink-conflict')
      var ck = v('sink-confkey')
      s.conflictKey = ck ? ck.split(',').map(function (x) { return x.trim() }) : undefined
    } else if (type === 'parquet') {
      s.path = v('sink-path')
      s.rowGroupSize = parseInt(v('sink-rgs')) || 100000
    } else if (type === 'objstore') {
      s.provider = v('sink-provider')
      s.bucket = v('sink-bucket')
      s.prefix = v('sink-prefix') || undefined
      s.format = v('sink-fmt')
    } else if (type === 'kafka') {
      var bv = v('sink-brokers')
      s.brokers = bv ? bv.split(',').map(function (x) { return x.trim() }) : []
      s.topic = v('sink-topic')
      s.sasl = checked('sink-sasl')
      s.scram = v('sink-scram')
      s.usernameEnv = v('sink-uenv') || undefined
      s.passwordEnv = v('sink-penv') || undefined
      s.batchSize = parseInt(v('sink-batch')) || 500
    } else if (type === 'dlq') {
      s.innerType = v('sink-innertype')
      var io = jsonParse('sink-inneropts'); if (io !== undefined) s.innerOptions = io
    }

    setDirty(true)
    renderPipeline()
    postUpdate()
    showToast('Sink updated')
  }

  // ── DLQ props ─────────────────────────────────────────────────────────────────
  function renderDlqProps(pp) {
    var doc = state.doc || {}
    var dlq = doc.dlqSink || {}
    var type = dlq.type || 'jdbc'

    var html = '<div class="pp-fields-wrap">' +
      '<div class="pp-hint pp-field-full" style="margin-bottom:4px">Rows that fail validation or error in the sink are routed here.</div>'

    html += renderSinkTypeFields(type, dlq).replace(/id="sink-/g, 'id="dlq-').replace(/id="sink/g, 'id="dlq')
    html += '</div>'

    html += '<div class="pp-action-bar">' +
      '<button class="btn-p" id="btn-dlq-save">Save</button>' +
      '<button class="btn-g" id="btn-dlq-discard">Discard</button>' +
      '<button class="btn-d" id="btn-dlq-remove" style="margin-left:auto">Remove DLQ</button>' +
      '</div>'

    pp.innerHTML = html

    var saveBtn = byId('btn-dlq-save')
    if (saveBtn) saveBtn.addEventListener('click', function () { saveDlqProps(); closePpPanel() })
    var discardBtn = byId('btn-dlq-discard')
    if (discardBtn) discardBtn.addEventListener('click', discardProps)

    var removeBtn = byId('btn-dlq-remove')
    if (removeBtn) removeBtn.addEventListener('click', deleteDlq)
  }

  function saveDlqProps() {
    if (!state.doc) return
    if (!state.doc.dlqSink) state.doc.dlqSink = {}
    var s = state.doc.dlqSink
    var v = function (id) { var el = byId(id); return el ? el.value : '' }
    var checked = function (id) { var el = byId(id); return el ? el.checked : false }

    var type = s.type || 'jdbc'

    if (type === 'jdbc') {
      s.driver = v('dlq-driver')
      s.dsn = v('dlq-dsn')
      s.table = v('dlq-table')
      s.batchSize = parseInt(v('dlq-batch')) || 200
    } else if (type === 'parquet') {
      s.path = v('dlq-path')
    } else if (type === 'objstore') {
      s.provider = v('dlq-provider')
      s.bucket = v('dlq-bucket')
      s.prefix = v('dlq-prefix') || undefined
      s.format = v('dlq-fmt')
    } else if (type === 'kafka') {
      var bv = v('dlq-brokers')
      s.brokers = bv ? bv.split(',').map(function (x) { return x.trim() }) : []
      s.topic = v('dlq-topic')
    }

    setDirty(true)
    renderPipeline()
    postUpdate()
    showToast('DLQ sink updated')
  }

  // ── PP helper builders ────────────────────────────────────────────────────────
  function ppField(label, inputHtml) {
    return '<div class="pp-field"><label class="pp-label">' + esc(label) + '</label>' + inputHtml + '</div>'
  }

  function ppSelect(id, options, selected) {
    var opts = options.map(function (o) {
      return '<option value="' + esc(o) + '"' + (o === selected ? ' selected' : '') + '>' + esc(o) + '</option>'
    }).join('')
    return '<select class="pp-select" id="' + id + '">' + opts + '</select>'
  }

  function ppCheck(id, checked) {
    return '<label class="pp-check-wrap"><input type="checkbox" id="' + id + '"' + (checked ? ' checked' : '') + '/><span class="pp-check-label">' + (checked ? 'Yes' : 'No') + '</span></label>'
  }

  // ── Performance tab ───────────────────────────────────────────────────────────
  // Wires the static sliders already present in the HTML into state.doc.performance.
  // Run once when the tab is first shown (or on re-render); idempotent due to
  // addEventListener being additive only when needed — we guard with a flag.
  function renderPerfTab() {
    var doc = state.doc || {}
    var perf = doc.performance || {}

    // number input ID → doc.performance key
    var fieldMap = [
      { id: 'perf-batchsize', key: 'batchSize' },
      { id: 'perf-workers',   key: 'transformWorkers' },
      { id: 'perf-memory',    key: 'maxMemoryMB' },
      { id: 'perf-src-buf',   key: 'sourceBufferBatches' },
      { id: 'perf-snk-buf',   key: 'sinkBufferBatches' },
    ]

    fieldMap.forEach(function (f) {
      var el = byId(f.id)
      if (!el) return
      // Populate from doc
      if (perf[f.key] != null) el.value = String(perf[f.key])
      // Wire live update once
      if (!el.dataset.wired) {
        el.dataset.wired = '1'
        el.addEventListener('change', function () {
          var n = parseInt(el.value)
          if (isNaN(n)) return
          if (!state.doc) return
          if (!state.doc.performance) state.doc.performance = {}
          state.doc.performance[f.key] = n
          setDirty(true)
          postUpdate()
        })
      }
    })

    // Wire the Save button
    var saveBtn = byId('btn-perf-save')
    if (saveBtn && !saveBtn.dataset.wired) {
      saveBtn.dataset.wired = '1'
      saveBtn.addEventListener('click', function () {
        setDirty(true)
        postUpdate()
        showToast('Performance config saved')
      })
    }
  }

  // ── Scaling tab ───────────────────────────────────────────────────────────────
  // Populates the static DOM already rendered by the provider HTML and wires
  // event listeners (guarded with data-wired to stay idempotent).
  function renderScalingTab() {
    var doc = state.doc || {}
    var sc = doc.scaling || {}
    var mode = sc.mode || 'single'

    // Populate scaling mode radios
    qsa('[name="scale-mode"]').forEach(function (r) {
      r.checked = (r.value === mode)
    })

    // Show/hide sharded / distributed sections
    var shEl = byId('scale-instance-section')
    var dsEl = byId('scale-coordinator-section')
    if (shEl) shEl.style.display = (mode === 'sharded' || mode === 'distributed') ? '' : 'none'
    if (dsEl) dsEl.style.display = mode === 'distributed' ? '' : 'none'

    // Populate instance count
    var instEl = byId('scale-instance-count')
    if (instEl) instEl.value = String(sc.instanceCount || 4)

    // Coordinator
    var coord = sc.coordinator || {}
    var coordTypeSel = byId('scale-coordinator-type')
    if (coordTypeSel) coordTypeSel.value = coord.type || 'postgres'
    var coordDsnEl = byId('scale-coordinator-dsn')
    if (coordDsnEl) coordDsnEl.value = coord.dsn || ''
    var leaseEl = byId('scale-lease-seconds')
    if (leaseEl) leaseEl.value = String(coord.leaseSeconds || 60)

    // Wire event listeners (once)
    var scaleModeEl = byId('scale-mode')
    if (scaleModeEl && !scaleModeEl.dataset.wired) {
      scaleModeEl.dataset.wired = '1'
      scaleModeEl.addEventListener('change', function (e) {
        var m = e.target && e.target.value
        if (!m) return
        if (shEl) shEl.style.display = (m === 'sharded' || m === 'distributed') ? '' : 'none'
        if (dsEl) dsEl.style.display = m === 'distributed' ? '' : 'none'
      })
    }

    // Wire Save button
    var saveBtn = byId('btn-scale-save')
    if (saveBtn && !saveBtn.dataset.wired) {
      saveBtn.dataset.wired = '1'
      saveBtn.addEventListener('click', saveScalingProps)
    }
  }

  function saveScalingProps() {
    if (!state.doc) return
    var v = function (id) { var el = byId(id); return el ? el.value : '' }

    var modeEl = qs('[name="scale-mode"]:checked')
    var mode = modeEl ? modeEl.value : 'single'

    var scaling = { mode: mode }
    if (mode === 'sharded' || mode === 'distributed') {
      scaling.instanceCount = parseInt(v('scale-instance-count')) || 4
    }
    if (mode === 'distributed') {
      var coordType = v('scale-coordinator-type')
      var coord = { type: coordType }
      coord.dsn = v('scale-coordinator-dsn') || undefined
      coord.leaseSeconds = parseInt(v('scale-lease-seconds')) || 60
      scaling.coordinator = coord
    }

    state.doc.scaling = scaling
    setDirty(true)
    postUpdate()
    showToast('Scaling config saved')
  }

  // ── Run tab ───────────────────────────────────────────────────────────────────
  var runStatus = 'idle'  // idle | starting | running | done | error

  // Wire the static Run tab DOM once (called from wireUI after boot).
  function wireRunTab() {
    var startBtn = byId('btn-run-start')
    var stopBtn = byId('btn-run-stop')
    var clearBtn = byId('btn-run-clear')
    var refreshBtn = byId('btn-metrics-refresh')
    var scrollBtn = byId('btn-log-scroll')

    if (startBtn) startBtn.addEventListener('click', function () {
      var issues = validateDoc(state.doc)
      var errors = issues.filter(function (i) { return i.sev === 'error' })
      if (errors.length) {
        showValidationPanel(issues)
        showToast(errors.length + ' error(s) — fix before running')
        return
      }
      runStatus = 'starting'
      runLog = []
      runMetrics = {}
      updateRunStatus('starting')
      send('run')
    })

    if (stopBtn) stopBtn.addEventListener('click', function () {
      send('stop')
    })

    if (clearBtn) clearBtn.addEventListener('click', function () {
      runLog = []
      runMetrics = {}
      runStatus = 'idle'
      updateRunStatus('idle')
      var logEl = byId('run-log')
      if (logEl) logEl.innerHTML = ''
      updateMetricsDisplay({})
    })

    if (refreshBtn) refreshBtn.addEventListener('click', function () {
      send('metrics')
    })

    if (scrollBtn) scrollBtn.addEventListener('click', function () {
      var logEl = byId('run-log')
      if (logEl) logEl.scrollTop = logEl.scrollHeight
    })
  }

  // renderRunTab: update the existing static DOM in place — no innerHTML clobber.
  function renderRunTab() {
    updateRunStatus(runStatus)
    updateMetricsDisplay(runMetrics || {})
    var logEl = byId('run-log')
    if (logEl && runLog.length) {
      logEl.textContent = runLog.join('\n')
      logEl.scrollTop = logEl.scrollHeight
    }
  }

  function updateRunStatus(status) {
    runStatus = status
    var statusLabel = { idle: 'idle', starting: 'starting', running: 'running', done: 'done', error: 'error' }

    // Static badge (#run-status) — matches provider HTML id
    var badge = byId('run-status')
    if (badge) {
      badge.textContent = statusLabel[status] || status
      badge.className = 'run-status-badge ' + (status || 'idle')
    }
    // Animated dot
    var dot = byId('run-status-dot')
    if (dot) {
      dot.className = 'run-status-dot ' + (status || '')
    }
    // Progress bar fill (#run-progress — matches provider HTML)
    var bar = byId('run-progress')
    if (bar) {
      if (status === 'running') {
        bar.style.width = '60%'
        bar.style.transition = 'none'
      } else if (status === 'done' || status === 'error') {
        bar.style.width = '100%'
      } else {
        bar.style.width = '0%'
      }
    }
    // Progress pct text
    var pctEl = byId('run-progress-pct')
    if (pctEl) pctEl.textContent = status === 'running' ? '…' : status === 'done' ? '100%' : status === 'error' ? 'error' : '—'

    // Run/stop buttons
    var startBtn = byId('btn-run-start')
    var stopBtn = byId('btn-run-stop')
    if (startBtn) {
      startBtn.disabled = status === 'running' || status === 'starting'
      startBtn.style.display = (status === 'running' || status === 'starting') ? 'none' : ''
    }
    if (stopBtn) {
      stopBtn.style.display = (status === 'running' || status === 'starting') ? '' : 'none'
    }
  }

  function appendRunLog(line) {
    runLog.push(line)
    if (runLog.length > 500) runLog = runLog.slice(-500)
    var logEl = byId('run-log')
    if (logEl) {
      logEl.textContent = runLog.join('\n')
      logEl.scrollTop = logEl.scrollHeight
    }
  }

  function updateMetricsDisplay(m) {
    var $ = function (id, val) { var el = byId(id); if (el) el.textContent = val }
    // ID names match the static tbody in pipelineEditorProvider.js HTML
    $('m-rows-processed', m.rowsProcessed != null ? String(m.rowsProcessed) : '—')
    $('m-rows-written', m.rowsWritten != null ? String(m.rowsWritten) : '—')
    $('m-rows-failed', m.rowsFailed != null ? String(m.rowsFailed) : '—')
    var durMs = m.durationMs
    $('m-duration', durMs != null ? durMs + ' ms' : '—')
    var throughput = (m.rowsProcessed != null && durMs && durMs > 0)
      ? Math.round(m.rowsProcessed / (durMs / 1000)) + ' rows/sec' : '—'
    $('m-throughput', throughput)
    $('m-memory', m.memoryRSS != null ? m.memoryRSS + ' MB' : '—')
  }

  function startMetricsPoll() {
    stopMetricsPoll()
    state.metricsInterval = setInterval(function () {
      if (state.running && state.runPort) {
        send('metrics', { port: state.runPort })
      }
    }, 1000)
  }

  function stopMetricsPoll() {
    if (state.metricsInterval) {
      clearInterval(state.metricsInterval)
      state.metricsInterval = null
    }
  }

  // ── Validation ────────────────────────────────────────────────────────────────
  function validateDoc(doc) {
    var issues = []
    if (!doc) return [{ sev: 'error', msg: 'No pipeline document loaded.' }]
    var src = doc.source || {}
    var sink = doc.sink || {}
    var transforms = Array.isArray(doc.transforms) ? doc.transforms : []
    var mode = doc.mode || 'etl'

    if (!src.type) issues.push({ sev: 'error', msg: 'Source type is required.' })
    if (!sink.type) issues.push({ sev: 'error', msg: 'Sink type is required.' })
    if (mode === 'etl' && transforms.length === 0) issues.push({ sev: 'error', msg: 'ETL mode requires at least one transform. Switch to EL mode or add a transform.' })
    if (mode === 'el' && transforms.length > 0) issues.push({ sev: 'warn', msg: 'EL mode: ' + transforms.length + ' transform(s) defined will be ignored.' })

    if (src.type === 'csv' && !src.path) issues.push({ sev: 'error', msg: 'Source: path is required for CSV source.' })
    if (src.type === 'tsv' && !src.path) issues.push({ sev: 'error', msg: 'Source: path is required for TSV source.' })
    if (src.type === 'parquet' && !src.path) issues.push({ sev: 'error', msg: 'Source: path is required for Parquet source.' })
    if (src.type === 'fixedwidth' && !src.path) issues.push({ sev: 'error', msg: 'Source: path is required for Fixed-Width source.' })
    if (src.type === 'xml' && !src.path) issues.push({ sev: 'error', msg: 'Source: path is required for XML source.' })
    if (src.type === 'jsonlines' && !src.path) issues.push({ sev: 'error', msg: 'Source: path is required for JSON Lines source.' })
    if (src.type === 'jdbc' && (!src.dsn || !src.table)) issues.push({ sev: 'error', msg: 'Source: JDBC source requires DSN and table.' })
    if (src.type === 'kafka' && !src.groupId) issues.push({ sev: 'warn', msg: 'Source: Kafka source should have a groupId.' })
    if (src.type === 'kafka' && (!src.brokers || (Array.isArray(src.brokers) && !src.brokers.length))) issues.push({ sev: 'error', msg: 'Source: Kafka source requires at least one broker.' })
    if (src.type === 'objstore' && !src.bucket) issues.push({ sev: 'error', msg: 'Source: Object store source requires a bucket.' })

    if (sink.type === 'jdbc' && (!sink.dsn || !sink.table)) issues.push({ sev: 'error', msg: 'Sink: JDBC sink requires DSN and table.' })
    if (sink.type === 'parquet' && !sink.path) issues.push({ sev: 'error', msg: 'Sink: Parquet sink requires a path.' })
    if (sink.type === 'kafka' && (!sink.brokers || (Array.isArray(sink.brokers) && !sink.brokers.length))) issues.push({ sev: 'error', msg: 'Sink: Kafka sink requires at least one broker.' })
    if (sink.type === 'objstore' && !sink.bucket) issues.push({ sev: 'error', msg: 'Sink: Object store sink requires a bucket.' })

    return issues
  }

  function showValidationPanel(issues) {
    // ID matches the static val-panel div in pipelineEditorProvider.js HTML
    var panel = byId('val-panel')
    if (!panel) return
    if (!issues || !issues.length) {
      panel.style.display = 'none'
      return
    }
    panel.style.display = ''

    // Populate val-list (the scrollable list inside val-panel)
    var list = byId('val-list')
    var title = byId('val-title')
    var errorCount = issues.filter(function (i) { return i.sev === 'error' }).length
    var warnCount = issues.filter(function (i) { return i.sev === 'warn' }).length
    if (title) title.textContent = 'Validation — ' + errorCount + ' error(s), ' + warnCount + ' warning(s)'

    if (list) {
      list.innerHTML = issues.map(function (iss) {
        var sevClass = iss.sev === 'error' ? 'val-error' : iss.sev === 'warn' ? 'val-warn' : 'val-info'
        var sevLabel = (iss.sev || 'info').toUpperCase()
        return '<div class="val-row ' + sevClass + '">' +
          '<span class="val-sev val-sev-' + (iss.sev || 'info') + '">' + esc(sevLabel) + '</span>' +
          (iss.label ? '<span class="val-label">' + esc(iss.label) + '</span>' : '') +
          '<span class="val-msg">' + esc(iss.msg || '') + '</span>' +
          '</div>'
      }).join('')
    }

    // Wire close button
    var closeBtn = byId('btn-val-close')
    if (closeBtn && !closeBtn.dataset.wired) {
      closeBtn.dataset.wired = '1'
      closeBtn.addEventListener('click', function () { panel.style.display = 'none' })
    }
  }

  // ── Palette (sidebar) ──────────────────────────────────────────────────────────
  var PALETTE_SOURCES = [
    { label: 'CSV', icon: '📄', type: 'csv' },
    { label: 'TSV', icon: '📄', type: 'tsv' },
    { label: 'JDBC (Database)', icon: '🗄', type: 'jdbc' },
    { label: 'Parquet', icon: '📦', type: 'parquet' },
    { label: 'Fixed-Width / EBCDIC', icon: '📃', type: 'fixedwidth' },
    { label: 'XML', icon: '📰', type: 'xml' },
    { label: 'JSON Lines', icon: '📑', type: 'jsonlines' },
    { label: 'Kafka', icon: '⚡', type: 'kafka' },
    { label: 'Cloud Object Store', icon: '☁', type: 'objstore' },
  ]

  var PALETTE_TRANSFORMS = [
    { label: 'Filter', icon: '🔍', type: 'filter' },
    { label: 'Map', icon: '🔀', type: 'map' },
    { label: 'Validate', icon: '✅', type: 'validate' },
    { label: 'Lookup', icon: '🔎', type: 'lookup' },
    { label: 'Incremental / HWM', icon: '📈', type: 'hwm' },
    { label: 'Schema Drift', icon: '🔄', type: 'schemadrift' },
    { label: 'Data Quality', icon: '🧪', type: 'dq' },
  ]

  var PALETTE_SINKS = [
    { label: 'JDBC (Database)', icon: '🗄', type: 'jdbc' },
    { label: 'Parquet', icon: '📦', type: 'parquet' },
    { label: 'Cloud Object Store', icon: '☁', type: 'objstore' },
    { label: 'Kafka', icon: '⚡', type: 'kafka' },
    { label: 'Dead Letter Queue', icon: '⚠', type: 'dlq' },
    { label: 'Null (discard)', icon: '🚫', type: 'null' },
  ]

  // Wire the static palette items already rendered in the provider HTML.
  // Each .pal-item has data-node-type and data-source-type / data-xform-type / data-sink-type.
  function renderPalette() {
    // Wire rail buttons (toggle sidebar panel)
    qsa('.rail-btn').forEach(function (btn) {
      if (btn.dataset.wired) return
      btn.dataset.wired = '1'
      btn.addEventListener('click', function () {
        var panelName = btn.getAttribute('data-panel')
        var panel = byId('sidebar-panel')
        // Toggle: if already open on same panel, close it
        var current = panel && panel.dataset.open
        if (current === panelName) {
          if (panel) { panel.style.width = '0'; panel.dataset.open = '' }
          qsa('.rail-btn').forEach(function (b) { b.classList.remove('active') })
          return
        }
        // Show the requested spane
        qsa('.spane').forEach(function (s) { s.classList.remove('active') })
        var spane = byId('spane-' + panelName)
        if (spane) spane.classList.add('active')
        if (panel) { panel.style.width = '200px'; panel.dataset.open = panelName }
        qsa('.rail-btn').forEach(function (b) { b.classList.remove('active') })
        btn.classList.add('active')
      })
    })

    // Wire palette item clicks — add/set stage from palette
    qsa('.pal-item[data-node-type]').forEach(function (item) {
      if (item.dataset.wired) return
      item.dataset.wired = '1'

      item.addEventListener('click', function () {
        var nodeType = item.getAttribute('data-node-type')
        if (nodeType === 'source') {
          var t = item.getAttribute('data-source-type') || 'csv'
          handlePaletteAction('set-source', t)
        } else if (nodeType === 'transform') {
          var t2 = item.getAttribute('data-xform-type') || 'map'
          handlePaletteAction('add-transform', t2)
        } else if (nodeType === 'sink') {
          var t3 = item.getAttribute('data-sink-type') || 'jdbc'
          handlePaletteAction('set-sink', t3)
        }
      })

      // Drag from palette onto canvas
      item.addEventListener('dragstart', function (e) {
        var nodeType2 = item.getAttribute('data-node-type')
        var subType = item.getAttribute('data-source-type') ||
          item.getAttribute('data-xform-type') ||
          item.getAttribute('data-sink-type') || ''
        e.dataTransfer.setData('text/plain', JSON.stringify({ nodeType: nodeType2, subType: subType }))
      })
    })

    // Wire canvas drop
    var cwrap = byId('cwrap')
    if (cwrap && !cwrap.dataset.dropWired) {
      cwrap.dataset.dropWired = '1'
      cwrap.addEventListener('dragover', function (e) { e.preventDefault() })
      cwrap.addEventListener('drop', function (e) {
        e.preventDefault()
        try {
          var data = JSON.parse(e.dataTransfer.getData('text/plain') || '{}')
          if (data.nodeType === 'source') handlePaletteAction('set-source', data.subType)
          else if (data.nodeType === 'transform') handlePaletteAction('add-transform', data.subType)
          else if (data.nodeType === 'sink') handlePaletteAction('set-sink', data.subType)
        } catch (_) { }
      })
    }
  }

  function handlePaletteAction(action, type) {
    if (!state.doc) return
    if (action === 'set-source') {
      state.doc.source = { type: type }
      state.selected = { kind: 'source', index: null }
      setDirty(true)
      render()
      postUpdate()
      showToast('Source set to ' + nodeLabel('source', type))
    } else if (action === 'add-transform') {
      if (!state.doc.transforms) state.doc.transforms = []
      var newTr = { type: type }
      state.doc.transforms.push(newTr)
      var newIdx = state.doc.transforms.length - 1
      state.selected = { kind: 'transform', index: newIdx }
      setDirty(true)
      render()
      postUpdate()
      showToast('Added ' + nodeLabel('transform', type) + ' transform')
    } else if (action === 'set-sink') {
      state.doc.sink = { type: type }
      state.selected = { kind: 'sink', index: null }
      setDirty(true)
      render()
      postUpdate()
      showToast('Sink set to ' + nodeLabel('sink', type))
    } else if (action === 'add-dlq') {
      state.doc.dlqSink = { type: 'jdbc' }
      state.selected = { kind: 'dlq', index: null }
      setDirty(true)
      render()
      postUpdate()
      showToast('DLQ sink added')
    }
  }

  // ── Validate button ───────────────────────────────────────────────────────────
  function wireValidateButton() {
    var btn = byId('btn-validate')
    if (!btn) return
    btn.addEventListener('click', function () {
      var issues = validateDoc(state.doc)
      showValidationPanel(issues)
      if (!issues.length) showToast('No issues found ✓')
    })
  }

  // ── Canvas click deselect ─────────────────────────────────────────────────────
  function wireCanvasDeselect() {
    var canvas = byId('canvas')
    if (canvas) {
      canvas.addEventListener('click', function (e) {
        if (e.target === canvas || e.target.id === 'nodes-layer' || e.target.id === 'esvg') {
          closePpPanel()
        }
      })
    }
  }

  // ── Properties panel resize (drag top edge to change height) ──────────────────
  function wirePpResize() {
    var resizeEl = byId('pp-resize')
    var ppEl = byId('pp')
    if (!resizeEl || !ppEl) return

    resizeEl.addEventListener('mousedown', function (e) {
      e.preventDefault()
      var startY = e.clientY
      var startH = ppEl.getBoundingClientRect().height
      resizeEl.classList.add('dragging')

      function onMove(ev) {
        // Dragging up increases height; dragging down decreases it
        var newH = Math.max(160, Math.min(600, startH - (ev.clientY - startY)))
        ppEl.style.height = newH + 'px'
      }
      function onUp() {
        document.removeEventListener('mousemove', onMove)
        document.removeEventListener('mouseup', onUp)
        resizeEl.classList.remove('dragging')
      }
      document.addEventListener('mousemove', onMove)
      document.addEventListener('mouseup', onUp)
    })
  }

  // ── Wire all UI ───────────────────────────────────────────────────────────────
  function wireUI() {
    renderPalette()
    wireValidateButton()
    wireCanvasDeselect()
    wirePpResize()
    wireRunTab()

    // Properties panel header close button (X) — just closes, no save/discard
    var ppCloseBtn = byId('btn-pp-close')
    if (ppCloseBtn) ppCloseBtn.addEventListener('click', closePpPanel)

    // "Run Pipeline" button in the meta bar (id="btn-run") switches to Run tab and starts
    var runBtn = byId('btn-run')
    if (runBtn) runBtn.addEventListener('click', function () {
      switchTopTab('run')
      // Small delay so the tab activates before we auto-click start
      setTimeout(function () {
        var startBtn = byId('btn-run-start')
        if (startBtn && !startBtn.disabled) startBtn.click()
      }, 50)
    })

    // "{ }" raw JSON button
    var rawBtn = byId('btn-raw')
    if (rawBtn) rawBtn.addEventListener('click', function () { send('openRaw') })

    // Canvas fit button
    var fitBtn = byId('btn-fit')
    if (fitBtn) fitBtn.addEventListener('click', function () { renderPipeline() })
  }

  // ── Entry point ────────────────────────────────────────────────────────────────
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', boot)
  } else {
    boot()
  }

})()
