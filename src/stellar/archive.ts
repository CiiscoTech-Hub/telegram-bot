import { open, mkdir, readdir, unlink, chmod } from "node:fs/promises";
import path from "node:path";
import { scanJsonReplacer } from "./events.js";
import type { DecodedEvent } from "./decode.js";
import {
  DEFAULT_JSONL_CHUNK_BYTES,
  DEFAULT_MAX_JSONL_LINE_BYTES,
  JsonlLineTooLongError,
  parseJsonlStream,
} from "./scan-jsonl.js";

export {
  DEFAULT_JSONL_CHUNK_BYTES,
  DEFAULT_MAX_JSONL_LINE_BYTES,
  JsonlLineTooLongError,
};

export interface EventArchiveOptions {
  dir: string;
  maxBytes?: number;
  maxFiles?: number;
}

export interface ReadArchiveOptions {
  /**
   * Maximum allowed byte length of a single JSONL record (excluding line ending).
   * Oversized records are rejected before UTF-8 decoding or `JSON.parse`.
   * Defaults to {@link DEFAULT_MAX_JSONL_LINE_BYTES} (64 KiB).
   */
  maxLineBytes?: number;
  /**
   * Fixed read buffer size in bytes for streaming each archive file.
   * Defaults to {@link DEFAULT_JSONL_CHUNK_BYTES} (64 KiB).
   */
  chunkBytes?: number;
}

export interface EventArchive {
  append(events: readonly DecodedEvent[]): Promise<void>;
  close(): Promise<void>;
}

const DEFAULT_MAX_BYTES = 10 * 1024 * 1024; // 10 MiB
const DEFAULT_MAX_FILES = 10;
const FILE_MODE = 0o600;

function formatArchiveFilename(index: number, timestamp: number): string {
  const ts = new Date(timestamp).toISOString().replace(/[:.]/g, "-");
  const idx = String(index).padStart(6, "0");
  return `events-${ts}-${idx}.jsonl`;
}

export function createEventArchive(options: EventArchiveOptions): EventArchive {
  const { dir } = options;
  const maxBytes = Math.max(1, options.maxBytes ?? DEFAULT_MAX_BYTES);
  const maxFiles = Math.max(1, options.maxFiles ?? DEFAULT_MAX_FILES);

  let currentHandle: any = null;
  let currentFilePath: string | null = null;
  let currentSize = 0;
  let fileIndex = 0;
  let closed = false;

  let queue: Promise<void> = Promise.resolve();

  async function rotateIfNeeded(additionalBytes: number): Promise<void> {
    if (currentHandle && currentSize + additionalBytes <= maxBytes) {
      return;
    }

    if (currentHandle) {
      await currentHandle.sync();
      await currentHandle.close();
      currentHandle = null;
      currentFilePath = null;
    }

    await mkdir(dir, { recursive: true });

    // Enforce maxFiles by deleting older files if needed
    const existing = await listArchiveFiles(dir);
    // If we're creating a new file, we'll have existing.length + 1 files.
    // If existing.length + 1 > maxFiles, remove oldest so we stay within maxFiles.
    const toRemoveCount = Math.max(0, existing.length + 1 - maxFiles);
    for (let i = 0; i < toRemoveCount; i++) {
      const target = existing[i];
      if (!target) continue;
      try {
        await unlink(path.join(dir, target));
      } catch {
        // ignore
      }
    }

    fileIndex += 1;
    const filename = formatArchiveFilename(fileIndex, Date.now());
    currentFilePath = path.join(dir, filename);

    // Open with 'a+' flag (append and reading), 0o600 permissions
    currentHandle = await open(currentFilePath, "a+", FILE_MODE);
    try {
      await chmod(currentFilePath, FILE_MODE);
    } catch {
      // chmod might not be fully supported on some OS/filesystems, safe to ignore
    }

    const st = await currentHandle.stat();
    currentSize = st.size;
  }

  async function doAppend(events: readonly DecodedEvent[]): Promise<void> {
    if (closed) {
      throw new Error("Event archive is closed");
    }
    if (!events || events.length === 0) return;

    for (const event of events) {
      const line = JSON.stringify(event, scanJsonReplacer) + "\n";
      const buffer = Buffer.from(line, "utf8");
      const bytes = buffer.byteLength;

      // Rotate if adding this line exceeds maxBytes, or if no file opened yet
      if (!currentHandle || currentSize + bytes > maxBytes) {
        await rotateIfNeeded(bytes);
      }

      await currentHandle.write(buffer);
      await currentHandle.sync();
      currentSize += bytes;
    }
  }

  async function doClose(): Promise<void> {
    if (closed) return;
    closed = true;
    if (currentHandle) {
      await currentHandle.sync();
      await currentHandle.close();
      currentHandle = null;
    }
  }

  return {
    async append(events: readonly DecodedEvent[]): Promise<void> {
      queue = queue.then(() => doAppend(events));
      return queue;
    },
    async close(): Promise<void> {
      queue = queue.then(() => doClose());
      return queue;
    },
  };
}

async function listArchiveFiles(dir: string): Promise<string[]> {
  try {
    const entries = await readdir(dir);
    return entries
      .filter((name) => name.startsWith("events-") && name.endsWith(".jsonl"))
      .sort();
  } catch {
    return [];
  }
}

function validateChunkBytes(chunkBytes: number): number {
  if (!Number.isFinite(chunkBytes) || !Number.isInteger(chunkBytes) || chunkBytes < 1) {
    throw new RangeError("chunkBytes must be a positive finite integer");
  }
  return chunkBytes;
}

async function* readFileChunks(
  filePath: string,
  chunkBytes: number,
): AsyncGenerator<Buffer, void, unknown> {
  const handle = await open(filePath, "r");
  try {
    const buffer = Buffer.allocUnsafe(chunkBytes);
    for (;;) {
      const { bytesRead } = await handle.read(buffer, 0, chunkBytes, null);
      if (bytesRead === 0) break;
      yield buffer.subarray(0, bytesRead);
    }
  } finally {
    await handle.close();
  }
}

/**
 * Stream decoded events from a single JSONL archive file using a fixed-size
 * read buffer and a bounded line buffer. Oversized records are rejected with
 * {@link JsonlLineTooLongError} before parsing.
 */
export async function* readArchiveFile(
  filePath: string,
  options: ReadArchiveOptions = {},
): AsyncGenerator<DecodedEvent, void, unknown> {
  const chunkBytes = validateChunkBytes(options.chunkBytes ?? DEFAULT_JSONL_CHUNK_BYTES);
  const maxLineBytes = options.maxLineBytes ?? DEFAULT_MAX_JSONL_LINE_BYTES;
  const source = path.basename(filePath);

  yield* parseJsonlStream<DecodedEvent>(readFileChunks(filePath, chunkBytes), {
    maxLineBytes,
    source,
  });
}

/**
 * Stream decoded events across all rotating JSONL archive files in `dir` in
 * chronological filename order, using a bounded line buffer so memory stays
 * independent of total archive size.
 */
export async function* readArchive(
  dir: string,
  options: ReadArchiveOptions = {},
): AsyncGenerator<DecodedEvent, void, unknown> {
  const files = await listArchiveFiles(dir);
  for (const file of files) {
    yield* readArchiveFile(path.join(dir, file), options);
  }
}
