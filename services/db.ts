// @deno-types="https://esm.sh/@duckdb/duckdb-wasm@1.28.0/dist/duckdb-browser-blocking.d.ts"
import { createDuckDB, getJsDelivrBundles, ConsoleLogger, DEFAULT_RUNTIME } from 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.28.0/dist/duckdb-browser-blocking.mjs/+esm';
const logger = new ConsoleLogger();
const JSDELIVR_BUNDLES = getJsDelivrBundles();

const initialize = async ()=>{
    const db = await createDuckDB(JSDELIVR_BUNDLES, logger, DEFAULT_RUNTIME);
    await db.instantiate(() => { });
    //const arrayBuffer = await fetch('https://github.com/mnsrulz/hpqdata/releases/download/v1.1/db.parquet')    //let's initialize the data set in memory
    //    .then(r => r.arrayBuffer());
    //db.registerFileBuffer('db.parquet', new Uint8Array(arrayBuffer));

    // db.registerFileHandle('db.parquet', '')
    const dbFileHandle = await Deno.open("./assets/db.parquet");
    await db.registerFileHandle('db.parquet', dbFileHandle, 1, true);
    return db;
}

const dbPromise = initialize();

//HTTP paths are not supported due to xhr not available in deno.
//db.registerFileURL('db.parquet', 'https://github.com/mnsrulz/hpqdata/releases/download/v1.1/db.parquet', DuckDBDataProtocol.HTTP, false);


export const getConnection = async ()=> {
    const dbPromiseVal = await dbPromise;
    return dbPromiseVal.connect();
}


