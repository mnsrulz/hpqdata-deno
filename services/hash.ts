import { encodeHex } from "https://deno.land/std@0.207.0/encoding/hex.ts";

export const hash = async message =>{
    const messageBuffer = new TextEncoder().encode(message);
    const hashBuffer = await crypto.subtle.digest("SHA-256", messageBuffer);
    return encodeHex(hashBuffer);
}