import { Application, Router, isHttpError } from "https://deno.land/x/oak@v12.6.1/mod.ts";
import { getQuery } from "https://deno.land/x/oak@v12.6.1/helpers.ts";
import { hash } from '../services/hash.ts'
import { getConnection } from '../services/db.ts'
const ttlTimeMs = 5 * 24 * 60 * 60 * 1000;  //5 days of cache
BigInt.prototype.toJSON = function () { return Number(this); }    //to keep them as numbers. Numbers have good range.
const kv = await Deno.openKv();
type RawSqlRequest = { q: string, requestId: string };
const instanceId = crypto.randomUUID();

kv.listenQueue(async (message) => {
  if (!message) return;
  const { requestId, q } = JSON.parse(`${message}`) as RawSqlRequest;
  const key = ["queryresult", requestId];
  console.log(`new message received! ${requestId}. Processing in instanceId: ${instanceId}`);
  const conn = await getConnection();
  const arrowResult = await conn.send(q);
  const result = JSON.stringify(arrowResult.readAll()[0].toArray().map((row) => row.toJSON()));
  await kv.set(key, result, { expireIn: ttlTimeMs });
});


const router = new Router();
router
  .get("/raw_org", async (context) => {
    const { q } = getQuery(context);
    if (!q) throw new Error(`empty query provided. Use with ?q=YOUR_QUERY`)
    const hashKey = await hash(q);
    const key = ["queryresult", hashKey];

    const { value } = await kv.get(key);
    if (value) {
      context.response.body = value;
      context.response.headers.set("x-read-from", 'cache');
    } else {
      const conn = await getConnection();
      const arrowResult = await conn.send(q);
      const result = JSON.stringify(arrowResult.readAll()[0].toArray().map((row) => row.toJSON()));
      await kv.set(key, result, { expireIn: ttlTimeMs });
      context.response.body = result;
      conn.close();
    }
    context.response.headers.set("x-instance-id", instanceId);
    context.response.type = "application/json";
  })
  .get("/raw", async (context) => {
    const { q } = getQuery(context);
    if (!q) throw new Error(`empty query provided. Use with ?q=YOUR_QUERY`)
    const requestId = await hash(q);
    const key = ["queryresult", requestId];

    const { value } = await kv.get(key);
    if (value) {
      context.response.body = value;
      context.response.headers.set("x-read-from", 'cache');
    } else {
      // const conn = await getConnection();
      // const arrowResult = await conn.send(q);
      // const result = JSON.stringify(arrowResult.readAll()[0].toArray().map((row) => row.toJSON()));
      // await kv.set(key, result, { expireIn: ttlTimeMs });

      await kv.enqueue(JSON.stringify({
        requestId,
        q
      }))
      let result = null;
      for await (const iterator of kv.watch<{ result: string }[]>([key])) {
        for (const item of iterator) {
          if (item.value) {
            result = item.value
          }
        }
        if (result) break;
      }

      context.response.body = result;
      // conn.close();
    }
    context.response.headers.set("x-instance-id", instanceId);
    context.response.type = "application/json";
  });

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
