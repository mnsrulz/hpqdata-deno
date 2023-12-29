// @deno-types="https://esm.sh/@duckdb/duckdb-wasm@1.28.0/dist/duckdb-browser-blocking.d.ts"
import { createDuckDB, getJsDelivrBundles, ConsoleLogger, DEFAULT_RUNTIME } from 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.28.0/dist/duckdb-browser-blocking.mjs/+esm';
const logger = new ConsoleLogger();
const JSDELIVR_BUNDLES = getJsDelivrBundles();
const db = await createDuckDB(JSDELIVR_BUNDLES, logger, DEFAULT_RUNTIME);
await db.instantiate(() => { });

const arrayBuffer = await fetch('https://github.com/mnsrulz/hpqdata/releases/download/v1.1/db.parquet')    //let's initialize the data set in memory
    .then(r => r.arrayBuffer());
db.registerFileBuffer('db.parquet', new Uint8Array(arrayBuffer));

//HTTP paths are not supported due to xhr not available in deno.
//db.registerFileURL('db.parquet', 'https://github.com/mnsrulz/hpqdata/releases/download/v1.1/db.parquet', DuckDBDataProtocol.HTTP, false);

export const conn = db.connect();


