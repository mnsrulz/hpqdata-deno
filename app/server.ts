import { Application, Router, isHttpError } from "https://deno.land/x/oak@v12.6.1/mod.ts";
import { getQuery } from "https://deno.land/x/oak@v12.6.1/helpers.ts";
import { hash } from '../services/hash.ts'

// @deno-types="https://esm.sh/@duckdb/duckdb-wasm@1.28.0/dist/duckdb-browser-blocking.d.ts"
import { createDuckDB, getJsDelivrBundles, ConsoleLogger, DEFAULT_RUNTIME } from 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.28.0/dist/duckdb-browser-blocking.mjs/+esm';
const logger = new ConsoleLogger();
const JSDELIVR_BUNDLES = getJsDelivrBundles();
const db = await createDuckDB(JSDELIVR_BUNDLES, logger, DEFAULT_RUNTIME);
await db.instantiate(() => { });

//const FILE_URL = new URL("./assets/db.parquet", import.meta.url).href;
const FILE_URL = 'https://github.com/mnsrulz/hpqdata/releases/download/v1.1/db.parquet';
const arrayBuffer = await fetch(FILE_URL)    //let's initialize the data set in memory
        .then(r => r.arrayBuffer());
db.registerFileBuffer('db.parquet', new Uint8Array(arrayBuffer));

const ttlTimeMs = 5 * 60 * 1000;  //5 minutes of cache
BigInt.prototype.toJSON = function () { return Number(this); }    //to keep them as numbers. Numbers have good range.

const instanceId = crypto.randomUUID();
const router = new Router();
router
  .get("/ls", async (context) => {
    const d = [];
    for await (const dirEntry of Deno.readDir("./assets/")) {
      d.push(dirEntry.name);
    }
    context.response.body = d;
  })
  .get("/testopenfile", async (context) => {
    await Deno.open("./assets/db.parquet");
  })
  .get("/count", async (context) => {
    const q = `SELECT COUNT(1) C FROM 'db.parquet'`
    
    // const dbFileHandle = await Deno.open("./assets/db.parquet");
    // await db.registerFileHandle('db.parquet', dbFileHandle, 3, true);

    const conn = await db.connect();
    const arrowResult = await conn.send(q);
    const result = JSON.stringify(arrowResult.readAll()[0].toArray().map((row) => row.toJSON()));
    context.response.body = result;
    conn.close();
  })
  // .get("/raw", async (context) => {
  //   const { q } = getQuery(context);
  //   if (!q) throw new Error(`empty query provided. Use with ?q=YOUR_QUERY`)
  //   const kv = await Deno.openKv();
  //   const hashKey = await hash(q);
  //   const key = ["queryresult", hashKey];

  //   const { value } = await kv.get(key);
  //   if (value) {
  //     context.response.body = value;
  //     context.response.headers.set("x-read-from", 'cache');
  //   } else {
  //     const conn = await getConnection();
  //     const arrowResult = await conn.send(q);
  //     const result = JSON.stringify(arrowResult.readAll()[0].toArray().map((row) => row.toJSON()));
  //     await kv.set(key, result, { expireIn: ttlTimeMs });
  //     context.response.body = result;
  //     conn.close();
  //   }
  //   context.response.headers.set("x-instance-id", instanceId);
  //   context.response.type = "application/json";
  // })
  ;

const app = new Application();

app.use(async (context, next) => {
  try {
    context.response.headers.set('Access-Control-Allow-Origin', '*')
    await next();
  } catch (err) {
    if (isHttpError(err)) {
      context.response.status = err.status;
    } else {
      context.response.status = 500;
    }
    context.response.body = { error: err.message };
    context.response.type = "json";
  }
});


app.use(router.routes());
app.use(router.allowedMethods());
await app.listen({ port: 8000 });