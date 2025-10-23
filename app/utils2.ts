import { exec } from 'child_process';
import { promisify } from 'util';
import * as fs from 'fs';
import * as path from 'path';

const execAsync = promisify(exec);

export async function runCommandAndParseJSON(cmd : string) : Promise<any> {
  try {
    const { stdout } = await execAsync(cmd);

    const data = JSON.parse(stdout);
    // console.log('Parsed JSON:', data);

    return data;
  } catch (err: any) {
    console.error('Command or parse error:', err.message);
    throw err;
  }
}

export function listFilesSync(dir: string): string[] {
  const entries = fs.readdirSync(dir, { withFileTypes: true });
  return entries
    .filter(entry => entry.isFile())
    .map(entry => path.join(dir, entry.name));
}
