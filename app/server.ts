import { Application, Router } from "https://deno.land/x/oak@v12.6.1/mod.ts";
import { getQuery } from "https://deno.land/x/oak@v12.6.1/helpers.ts";

import { conn } from '../services/db'

const router = new Router();
router
  .get("/raw", (context) => {
    const {q} = getQuery(context);
    if(!q) throw new Error(`empty query provided. Use with ?q=YOUR_QUERY`)
    
    const arrowResult = conn.query(q);
    context.response.body = arrowResult.toArray().map((row) => row.toJSON());
  });

const app = new Application();
app.use(router.routes());
app.use(router.allowedMethods());
await app.listen({ port: 8000 });