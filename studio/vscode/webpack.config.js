//@ts-check
'use strict';

const path = require('path');

/** @type {import('webpack').Configuration} */
const extensionConfig = {
  name: 'extension',
  target: 'node',
  mode: 'none',
  entry: './src/extension.ts',
  output: {
    path: path.resolve(__dirname, 'dist'),
    filename: 'extension.js',
    libraryTarget: 'commonjs2',
  },
  externals: {
    vscode: 'commonjs vscode',
  },
  resolve: {
    extensions: ['.ts', '.js'],
  },
  module: {
    rules: [
      {
        test: /\.ts$/,
        exclude: /node_modules/,
        use: [{ loader: 'ts-loader' }],
      },
    ],
  },
  optimization: {
    // Disable Terser minification. For a VS Code extension the size win is
    // negligible (it is installed locally, not shipped over a network), and
    // webpack's built-in Terser runs in jest-worker CHILD processes that do NOT
    // inherit the parent's --max-old-space-size. On memory-constrained machines
    // / CI those workers OOM with:
    //   "Worker terminated due to reaching memory limit: JS heap out of memory".
    // Skipping minification removes the worker entirely.
    minimize: false,
    // Belt-and-suspenders: even if `--mode production` (or a future change) flips
    // minimize back on, an EMPTY minimizer list guarantees no jest-worker child
    // process is ever spawned, so the OOM cannot recur.
    minimizer: [],
  },
  performance: { hints: false },
  devtool: 'nosources-source-map',
  infrastructureLogging: { level: 'log' },
};

/** @type {import('webpack').Configuration} */
const webviewConfig = {
  name: 'webview',
  target: 'web',
  mode: 'none',
  entry: './webview/main.ts',
  output: {
    path: path.resolve(__dirname, 'dist'),
    filename: 'webview.js',
    libraryTarget: 'window',
  },
  resolve: {
    extensions: ['.ts', '.js'],
  },
  module: {
    rules: [
      {
        test: /\.ts$/,
        exclude: /node_modules/,
        use: [{ loader: 'ts-loader', options: { compilerOptions: { module: 'esnext' } } }],
      },
    ],
  },
  optimization: {
    // See extensionConfig: disable the Terser minifier so the production build
    // does not spawn jest-worker child processes that OOM ("Worker terminated
    // due to reaching memory limit") on constrained machines / CI.
    minimize: false,
    // Belt-and-suspenders (see extensionConfig): an empty minimizer list means no
    // jest-worker child process is ever spawned, even under `--mode production`.
    minimizer: [],
  },
  performance: { hints: false },
  devtool: 'nosources-source-map',
};

module.exports = [extensionConfig, webviewConfig];
