// Exercise the distributed WASM and its JavaScript glue, not just the native crate.
import assert from 'node:assert/strict';
import {readFileSync, readdirSync} from 'node:fs';
import {fileURLToPath} from 'node:url';
import {join} from 'node:path';
import init, {Viewer} from '../docs/pkg/tickvault_viewer.js';

await init({module_or_path: readFileSync(new URL('../docs/pkg/tickvault_viewer_bg.wasm', import.meta.url))});
const root = fileURLToPath(new URL('../docs/data/kraken/', import.meta.url));
function parquet(directory) {
  return readdirSync(directory, {withFileTypes: true}).flatMap(entry => {
    const path = join(directory, entry.name);
    return entry.isDirectory() ? parquet(path) : entry.name.endsWith('.parquet') ? [path] : [];
  });
}
const viewer = new Viewer('BTC-USD', 2, 10);
try {
  for (const path of parquet(root).sort()) viewer.add_file(readFileSync(path));
  viewer.seal();
  assert(viewer.messages() > 0 && viewer.rows() > 0);
  const count = viewer.messages();
  const book = JSON.parse(viewer.book_after(count, 10));
  assert(book.bids.length > 0 && book.asks.length > 0);
  assert.deepEqual(JSON.parse(viewer.book_at(viewer.message_at(count - 1), 10)), book);
  viewer.book_after(1, 10);
  assert.deepEqual(JSON.parse(viewer.book_after(count, 10)), book);
  console.log('Distributed WASM verified: Parquet load, timestamp query, backward/forward replay');
} finally {
  viewer.free();
}
