'use strict'

// ESLint flat config for the Flogo Data Pipeline Studio VS Code extension.
//
// Two execution environments live in this package:
//   - the Node "host" (extension entry point, providers, build scripts), and
//   - the browser "webview" (pipelineCanvas.js, loaded as an external <script>).
// They need different globals, so each gets its own block below.

const js = require('@eslint/js')
const globals = require('globals')

module.exports = [
  {
    ignores: ['node_modules/**', 'bin/**', 'resources/**', '**/*.vsix'],
  },

  js.configs.recommended,

  {
    // Node host code: the extension activation point, custom-editor provider,
    // sidebar, binary manager, starter templates, and build scripts.
    files: [
      'src/extension.js',
      'src/pipelineEditorProvider.js',
      'src/extensionScanner.js',
      'src/templates.js',
      'src/binaryManager.js',
      'src/sidebarProvider.js',
      'scripts/**/*.js',
      'eslint.config.js',
    ],
    languageOptions: {
      ecmaVersion: 2022,
      sourceType: 'commonjs',
      globals: { ...globals.node },
    },
    rules: {
      // Unused catch bindings and intentionally-unused args (leading _) are fine.
      'no-unused-vars': ['error', { argsIgnorePattern: '^_', caughtErrors: 'none' }],
      // Deliberate best-effort empty catches are allowed; empty if/for/etc. are not.
      'no-empty': ['error', { allowEmptyCatch: true }],
    },
  },

  {
    // Webview canvas: a browser script (IIFE), talks to the host via the
    // VS Code webview API exposed as acquireVsCodeApi().
    files: ['src/pipelineCanvas.js'],
    languageOptions: {
      ecmaVersion: 2022,
      sourceType: 'script',
      globals: {
        ...globals.browser,
        acquireVsCodeApi: 'readonly',
      },
    },
    rules: {
      'no-unused-vars': ['error', { argsIgnorePattern: '^_', caughtErrors: 'none' }],
      'no-empty': ['error', { allowEmptyCatch: true }],
    },
  },
]
