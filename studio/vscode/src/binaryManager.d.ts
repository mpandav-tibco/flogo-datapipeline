import * as vscode from 'vscode';

export interface LeanBuildOpts {
  pipelineFile: string;
  out: string;
  goos?: string;
  goarch?: string;
  workRoot?: string;
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
  start(pipelineFile: string, startPort?: number, workingDir?: string): Promise<number>;
  stop(): void;
  findFreePort(startPort?: number): Promise<number>;
  runInTerminal(opts: RunInTerminalOpts): Promise<number>;
  hasGoToolchain(): boolean;
  buildLeanBinary(opts: LeanBuildOpts): Promise<void>;
}
