import * as vscode from 'vscode';

export interface BuildBinaryOpts {
  sourcePath: string;
  out: string;
  goos?: string;
  goarch?: string;
  pipelinePath?: string;
}

export interface RunInTerminalOpts {
  name: string;
  cwd?: string;
  commandLine: string;
  port: number;
  onStop?: () => void;
}

export class BinaryManager {
  constructor(extensionPath: string);
  readonly port: number | null;
  readonly channel: vscode.OutputChannel;
  setOverrideBinaryPath(p: string | null): void;
  onCrash(cb: () => void): void;
  onLog(cb: ((text: string) => void) | null): void;
  start(pipelineFile: string, startPort?: number): Promise<number>;
  stop(): void;
  findFreePort(startPort?: number): Promise<number>;
  runInTerminal(opts: RunInTerminalOpts): Promise<number>;
  buildBinary(opts: BuildBinaryOpts): Promise<void>;
  buildCommand(): string;
}
