// @ts-check
'use strict'

const { spawn, exec, execSync, execFile } = require('child_process')
const http = require('http')
const net = require('net')
const path = require('path')
const fs = require('fs')
const vscode = require('vscode')

// Readiness probe: the pipeline engine exposes /health on the same port it is
// bound to. start() waits ~300 ms for early crashes before polling, so a 200
// here means the process survived config load and the pipeline started.
const HEALTH_ENDPOINT = '/v1/health'
const POLL_INTERVAL_MS = 500
const STARTUP_TIMEOUT_S = 15
const PORT_RETRY_MAX = 10
const DEFAULT_PORT = 9080

// Returns the platform-specific binary filename bundled in bin/.
// Naming convention mirrors flogo-agent (matches a build script):
//   Windows arm64 → flogo-datapipeline-windows-arm64.exe
//   Windows x64   → flogo-datapipeline-windows-amd64.exe
//   macOS  arm64  → flogo-datapipeline-darwin-arm64
//   macOS  x64    → flogo-datapipeline-darwin-amd64
//   Linux  arm64  → flogo-datapipeline-linux-arm64
//   Linux  x64    → flogo-datapipeline-linux-amd64
function _platformBinaryName() {
  const goArch = process.arch === 'x64' ? 'amd64' : process.arch  // x64→amd64, arm64 stays
  const baseName = 'flogo-datapipeline'
  if (process.platform === 'win32') return `${baseName}-windows-${goArch}.exe`
  return `${baseName}-${process.platform}-${goArch}`
}

class BinaryManager {
  constructor(extensionPath) {
    this._extensionPath = extensionPath
    this._process = null
    this._port = null
    this._crashCb = null
    this._externalStop = null  // teardown for an attached external engine
    this._channel = vscode.window.createOutputChannel('Flogo Data Pipeline')
    this._startingPromise = null  // in-flight start() deduplicator
  }

  get port() { return this._port }
  get channel() { return this._channel }

  // Callback invoked if the binary exits unexpectedly after a successful start.
  onCrash(cb) { this._crashCb = cb }

  // ── Public API ────────────────────────────────────────────────────────────

  // Start the flogo-datapipeline engine for the given pipeline file on the
  // given port (default: DEFAULT_PORT). Tries up to PORT_RETRY_MAX consecutive
  // ports until one binds cleanly. Resolves with the bound port.
  /** @returns {Promise<number>} */
  async start(pipelineFile, startPort) {
    if (this._externalStop) this.stop()   // tear down any attached external engine first
    if (this._process) return this._port  // already running
    if (this._startingPromise) return this._startingPromise  // concurrent start() — share the in-flight promise
    this._startingPromise = this._doStart(pipelineFile, startPort)
    try {
      return await this._startingPromise
    } finally {
      this._startingPromise = null
    }
  }

  /** @returns {Promise<number>} */
  async _doStart(pipelineFile, startPort) {

    const binary = this._resolveBinary()
    this._channel.show(true)

    const basePort = startPort || DEFAULT_PORT
    let port = basePort
    let proc = null

    for (let attempt = 0; attempt < PORT_RETRY_MAX; attempt++) {
      const free = await this._isPortFree(port)
      if (!free) {
        this._channel.appendLine(`[manager] port ${port} busy, trying ${port + 1}…`)
        port++
        continue
      }

      proc = this._spawn(binary, port, pipelineFile)

      // Give the process 2 s to crash (EADDRINUSE surfaces quickly).
      const crashed = await this._waitForEarlyExit(proc, 2000)
      if (!crashed) break  // still running → assume port was free

      if (attempt + 1 < PORT_RETRY_MAX) {
        port++
        this._channel.appendLine(`[manager] port busy (early exit), trying ${port}…`)
      } else {
        throw new Error(
          `Could not bind to any port in ${basePort}–${port}. Check for conflicting processes.`)
      }
    }

    if (!proc) throw new Error('flogo-datapipeline process was not spawned')

    // Wait for the health endpoint to respond before advertising the port.
    try {
      await this._waitForHealth(port)
    } catch (err) {
      proc.kill()
      throw err
    }

    this._process = proc
    this._port = port
    this._channel.appendLine(`[manager] flogo-datapipeline ready on port ${port}`)

    // Watch for unexpected exits after a successful startup.
    proc.once('exit', (code, signal) => {
      if (this._process !== proc) return  // intentional stop()
      this._channel.appendLine(
        `[manager] engine exited (code=${code} signal=${signal}) — unexpected!`)
      this._process = null
      this._port = null
      if (this._crashCb) this._crashCb()
    })

    return port
  }

  stop() {
    if (this._externalStop) {
      const fn = this._externalStop
      this._externalStop = null
      this._port = null
      try { fn() } catch (_) { }
      this._channel.appendLine('[manager] external engine stopped')
      return
    }
    if (!this._process) return
    const proc = this._process
    this._process = null
    this._port = null

    if (process.platform === 'win32') {
      // Use async exec() so the VS Code extension host thread is not blocked
      // while taskkill terminates the process tree (same pattern as flogo-agent).
      exec(`taskkill /PID ${proc.pid} /F /T`, () => { })
    } else {
      proc.kill('SIGTERM')
    }
    this._channel.appendLine('[manager] engine stopped')
  }

  // Find a free TCP port at/above startPort. Falls back to startPort when none
  // found in the search window — the port retry loop in start() handles the rest.
  async findFreePort(startPort) {
    for (let p = startPort || DEFAULT_PORT; p < (startPort || DEFAULT_PORT) + 50; p++) {
      if (await this._isPortFree(p)) return p
    }
    return startPort || DEFAULT_PORT
  }

  // Run the engine in a visible integrated terminal (user can see stdout), then
  // attach the manager by health-checking `port`. `onStop` runs on stop() before
  // the terminal is disposed. On startup failure the terminal is left open so the
  // user can read the error.
  /** @returns {Promise<number>} */
  async runInTerminal({ name, cwd, commandLine, port, onStop }) {
    if (this._process || this._externalStop) this.stop()
    const term = vscode.window.createTerminal(cwd ? { name, cwd } : { name })
    term.show(true)
    term.sendText(commandLine)
    // Register teardown up-front so stop() can dispose the terminal even on failure.
    this._externalStop = () => {
      try { if (onStop) onStop() } catch (_) { }
      try { term.dispose() } catch (_) { }
    }
    // Health-check before advertising the port; this._port stays null on failure so
    // the UI never targets an unconfirmed engine.
    await this._waitForHealth(port)
    this._port = port
    this._channel.appendLine(`[manager] engine running in terminal "${name}" on port ${port}`)
    return port
  }

  // Build the flogo-datapipeline binary for a specific platform.
  // Runs `go build` in the source directory and writes the binary to `out`.
  /** @returns {Promise<void>} */
  buildBinary({ sourcePath, out, goos, goarch, pipelinePath }) {
    return new Promise((resolve, reject) => {
      if (!sourcePath) {
        return reject(new Error('buildBinary: sourcePath is required'))
      }
      if (!out) {
        return reject(new Error('buildBinary: out (output path) is required'))
      }
      const env = {
        ...process.env,
        GOOS: goos || process.platform.replace('win32', 'windows'),
        GOARCH: goarch || (process.arch === 'x64' ? 'amd64' : process.arch),
        CGO_ENABLED: '0',
      }
      const cmd = `go build -o "${out}" ./cmd/flogo-datapipeline/`
      this._channel.appendLine(`[build] ${cmd} (cwd: ${sourcePath})`)
      exec(cmd, { cwd: sourcePath, env }, (err, stdout, stderr) => {
        if (stdout) this._channel.append(stdout)
        if (stderr) this._channel.append(stderr)
        if (err) {
          return reject(new Error(`go build failed: ${stderr || err.message}`))
        }
        this._channel.appendLine(`[build] binary written to ${out}`)
        resolve()
      })
    })
  }

  // Returns the build command string for display (tooltips / setup screens).
  buildCommand() {
    const name = _platformBinaryName()
    const out = path.join(this._extensionPath, 'bin', name)
    return `go build -o "${out}" ./cmd/flogo-datapipeline/`
  }

  // ── Private ───────────────────────────────────────────────────────────────

  // Resolve the binary path, searching in order:
  //   1. extensionPath/bin/<platform-name>  (bundled in VSIX)
  //   2. extensionPath/<platform-name>      (dev drop-in)
  //   3. PATH (system-installed build)
  _resolveBinary() {
    const name = _platformBinaryName()

    const inBin = path.join(this._extensionPath, 'bin', name)
    if (fs.existsSync(inBin)) return inBin

    const inRoot = path.join(this._extensionPath, name)
    if (fs.existsSync(inRoot)) return inRoot

    // Try PATH — execSync will throw if not found.
    try {
      const which = process.platform === 'win32' ? 'where' : 'which'
      const found = execSync(`${which} ${name}`, { stdio: ['ignore', 'pipe', 'ignore'] })
        .toString().trim().split(/\r?\n/)[0]
      if (found && fs.existsSync(found)) return found
    } catch (_) { }

    // Also try the bare executable name on PATH (lets `flogo-datapipeline` without
    // the platform suffix work when the user has built it locally).
    const bareName = process.platform === 'win32' ? 'flogo-datapipeline.exe' : 'flogo-datapipeline'
    try {
      const which = process.platform === 'win32' ? 'where' : 'which'
      const found = execSync(`${which} ${bareName}`, { stdio: ['ignore', 'pipe', 'ignore'] })
        .toString().trim().split(/\r?\n/)[0]
      if (found && fs.existsSync(found)) return found
    } catch (_) { }

    // Structured error so callers can show a setup prompt.
    const err = /** @type {Error & { code?: string, buildCmd?: string }} */ (
      new Error(`flogo-datapipeline binary not found (${name}). Run:\n  ${this.buildCommand()}`)
    )
    err.code = 'NO_BINARY'
    err.buildCmd = this.buildCommand()
    throw err
  }

  // Strip macOS quarantine attribute and ensure execute bit — required for
  // bundled binaries on macOS that aren't notarized.
  _prepBinary(binary) {
    if (process.platform === 'win32') return
    try { execSync(`chmod +x "${binary}"`, { stdio: 'ignore' }) } catch (_) { }
    if (process.platform === 'darwin') {
      try {
        execSync(`xattr -dr com.apple.quarantine "${binary}"`, { stdio: 'ignore' })
        this._channel.appendLine('[manager] quarantine attribute removed from binary')
      } catch (_) { }
    }
  }

  // Spawn the engine process. The pipeline file and port are passed via both
  // environment variables (PIPELINE_FILE / PORT) and CLI flags (-port / -f) so
  // either convention works regardless of how the binary was built.
  _spawn(binary, port, pipelineFile) {
    this._prepBinary(binary)

    const args = ['-port', String(port)]
    if (pipelineFile) args.push('-f', pipelineFile)

    const env = { ...process.env, PORT: String(port) }
    if (pipelineFile) env.PIPELINE_FILE = pipelineFile

    this._channel.appendLine(`[manager] spawning: ${binary} ${args.join(' ')}`)
    const proc = spawn(binary, args, {
      stdio: ['ignore', 'pipe', 'pipe'],
      shell: false,   // always false — same as flogo-agent; avoids quoting issues on Win32
      env,
    })

    // Guard against ENOENT / EACCES after the pre-flight so Node doesn't re-throw
    // as an uncaught exception that would crash the extension host.
    proc.once('error', err =>
      this._channel.appendLine('[manager] engine process error: ' + err.message))
    proc.stdout.on('data', d => this._channel.append(d.toString()))
    proc.stderr.on('data', d => this._channel.append(d.toString()))
    return proc
  }

  _isPortFree(port) {
    return new Promise(resolve => {
      const srv = net.createServer()
      srv.once('error', () => resolve(false))
      srv.once('listening', () => srv.close(() => resolve(true)))
      srv.listen(port, '127.0.0.1')
    })
  }

  _waitForEarlyExit(proc, ms) {
    return new Promise(resolve => {
      const onExit = () => { clearTimeout(timer); resolve(true) }
      const timer = setTimeout(() => { proc.off('exit', onExit); resolve(false) }, ms)
      proc.once('exit', onExit)
    })
  }

  /** @returns {Promise<void>} */
  _waitForHealth(port) {
    return new Promise((resolve, reject) => {
      const deadline = Date.now() + STARTUP_TIMEOUT_S * 1000
      const poll = () => {
        if (Date.now() > deadline) {
          return reject(new Error(
            `Engine did not start within ${STARTUP_TIMEOUT_S}s on port ${port}.`))
        }
        const req = http.get(`http://127.0.0.1:${port}${HEALTH_ENDPOINT}`, res => {
          req.setTimeout(0)  // cancel destroy timer — response arrived
          res.resume()
          if (res.statusCode === 200) { resolve() }
          else { setTimeout(poll, POLL_INTERVAL_MS) }
        })
        req.on('error', () => setTimeout(poll, POLL_INTERVAL_MS))
        req.setTimeout(POLL_INTERVAL_MS, () => req.destroy())
      }
      setTimeout(poll, 300)  // small delay so the process can bind
    })
  }
}

module.exports = { BinaryManager }
