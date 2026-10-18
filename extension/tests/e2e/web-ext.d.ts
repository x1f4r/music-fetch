// The part of web-ext's Node API the Firefox smoke test uses (the package ships no types).
declare module 'web-ext' {
  interface RunOptions {
    sourceDir: string;
    target: ('firefox-desktop' | 'firefox-android' | 'chromium')[];
    firefox?: string;
    args?: string[];
    pref?: Record<string, string | number | boolean>;
    startUrl?: string[];
    noReload?: boolean;
    noInput?: boolean;
    browserConsole?: boolean;
  }
  interface Runner {
    exit(): Promise<void>;
  }
  const webExt: {
    cmd: { run(options: RunOptions, settings?: { shouldExitProgram?: boolean }): Promise<Runner> };
  };
  export default webExt;
}
