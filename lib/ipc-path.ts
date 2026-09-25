import { posix } from "node:path";

// Preserve the 2.2.0 format: usernames from the launch environment are not
// identities and must not affect peer validation. Windows isolation depends on
// registration-file ACLs and the bearer-token handshake, not the pipe name.
export function socketPathFor(platform: NodeJS.Platform, runtimeDir: string, namespace: string, instanceId: string): string {
  if (platform === "win32") return `\\\\.\\pipe\\pi-peer-${namespace}-${instanceId}`;
  return posix.join(runtimeDir, `${instanceId}.sock`);
}
