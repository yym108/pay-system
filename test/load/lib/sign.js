import crypto from 'k6/crypto';

const NONCE_CHARS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';

// 为每次请求生成 24 位 [A-Za-z0-9] 随机 nonce，避免同 VU 或跨 VU 重复。
export function generateNonce(vuId, iter) {
  const prefix = `${vuId}x${iter}x`.replace(/[^A-Za-z0-9]/g, '0');
  let out = prefix;
  while (out.length < 24) {
    const idx = Math.floor(Math.random() * NONCE_CHARS.length);
    out += NONCE_CHARS[idx];
  }
  return out.slice(0, 24);
}

// 按 docs/05-api.md §3.1 计算商户请求签名。
export function signMerchantHeaders({ method, requestUri, body, appId, appSecret, timestamp, nonce }) {
  const rawBody = body || '';
  const bodyHash = crypto.sha256(rawBody, 'hex');
  const canonical = `${method.toUpperCase()}\n${requestUri}\n${timestamp}\n${nonce}\n${bodyHash}`;
  const signature = crypto.hmac('sha256', appSecret, canonical, 'base64');
  return {
    'Content-Type': 'application/json',
    'X-App-Id': appId,
    'X-Timestamp': String(timestamp),
    'X-Nonce': nonce,
    'X-Signature': signature,
  };
}
