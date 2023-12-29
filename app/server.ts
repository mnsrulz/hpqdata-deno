import { Application, Router, isHttpError } from "https://deno.land/x/oak@v12.6.1/mod.ts";
import { getQuery } from "https://deno.land/x/oak@v12.6.1/helpers.ts";
import { hash } from '../services/hash.ts'
import { conn } from '../services/db.ts'

BigInt.prototype.toJSON = function () { return Number(this); }    //to keep them as numbers. Numbers have good range.

const instanceId = crypto.randomUUID();
const router = new Router();
router
  .get("/raw", async (context) => {
    const { q } = getQuery(context);
    if (!q) throw new Error(`empty query provided. Use with ?q=YOUR_QUERY`)
    const kv = await Deno.openKv();
    const hashKey = await hash(q);
    const key = ["queryresult", hashKey];

    const { value } = await kv.get(key);
    if (value) {
      context.response.body = value;
      context.response.headers.set("x-read-from", 'cache');
    } else {
      const arrowResult = conn.query(q);
      const result = arrowResult.toArray().map((row) => row.toJSON());
      await kv.set(key, result, { expireIn: 60 * 1000 }); //60 seconds expiration
      context.response.body = result;
    }
    context.response.headers.set("x-instance-id", instanceId);
  });

const app = new Application();

app.use(async (context, next) => {
  try {
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