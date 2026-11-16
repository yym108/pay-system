import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';
import exec from 'k6/execution';
import { generateNonce, signMerchantHeaders } from './lib/sign.js';

const GATEWAY_URL = (__ENV.GATEWAY_URL || 'http://127.0.0.1:8080').replace(/\/+$/, '');
const SCENARIO = (__ENV.SCENARIO || 'S1').toUpperCase();
const SECRET_FMT = __ENV.DEMO_APP_SECRET_FORMAT || 'demo_secret_%s_0123456789ab';
const WARMUP_DURATION = __ENV.WARMUP_DURATION || '1m';
const STEADY_DURATION = __ENV.STEADY_DURATION || '5m';
const RUN_ID = __ENV.RUN_ID || String(Date.now());

const DEFAULT_VUS = {
  S1: 50,
  S2: 50,
  S3: 40,
  S4: 30,
  S5: 40,
  S6: 40,
  S7: 50,
};
const VUS = Number(__ENV.VUS || DEFAULT_VUS[SCENARIO] || 40);

export const steadyLatency = new Trend('steady_tx_duration', true);
export const steadyOps = new Counter('steady_ops_total');
export const steadyErrRate = new Rate('steady_err_rate');

export const options = {
  setupTimeout: '120s',
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)'],
  scenarios: {
    warmup: {
      executor: 'constant-vus',
      vus: VUS,
      duration: WARMUP_DURATION,
      tags: { phase: 'warmup', scenario: SCENARIO },
    },
    steady: {
      executor: 'constant-vus',
      vus: VUS,
      duration: STEADY_DURATION,
      startTime: WARMUP_DURATION,
      tags: { phase: 'steady', scenario: SCENARIO },
    },
  },
};

function merchantIdByIdx(idx) {
  return `M${200000 + idx}`;
}

function appSecretFor(merchantId) {
  return SECRET_FMT.replace('%s', merchantId);
}

function appIdFor(merchantId) {
  return `app_${merchantId.toLowerCase()}`;
}

let localIter = 0;

function nextNonce() {
  localIter += 1;
  const vuId = exec.vu ? exec.vu.idInTest : 0;
  return generateNonce(vuId, localIter);
}

function apiCall(method, path, merchantId, bodyObj, timeoutSec = 10) {
  const bodyStr = bodyObj ? JSON.stringify(bodyObj) : '';
  const ts = Math.floor(Date.now() / 1000);
  const nonce = nextNonce();
  const headers = signMerchantHeaders({
    method,
    requestUri: path,
    body: bodyStr,
    appId: appIdFor(merchantId),
    appSecret: appSecretFor(merchantId),
    timestamp: ts,
    nonce,
  });
  const url = `${GATEWAY_URL}${path}`;
  const params = {
    headers,
    timeout: `${timeoutSec}s`,
    tags: { name: `${method} ${path.replace(/\/[A-Z]{2}\d{19}/g, '/:no')}` },
  };
  if (method === 'GET') {
    return http.get(url, params);
  }
  return http.post(url, bodyStr, params);
}

function parseJsonSafe(res) {
  try {
    return res.json();
  } catch (_) {
    return null;
  }
}

function isSteady() {
  return exec.scenario && exec.scenario.name === 'steady';
}

function recordOp(startMs, ok) {
  if (isSteady()) {
    steadyOps.add(1);
    steadyLatency.add(Date.now() - startMs);
    steadyErrRate.add(!ok);
  }
}

// 为 S4 和 S7 准备 100 笔已入账大额订单，供压测期间发起部分退款。
export function setup() {
  if (SCENARIO !== 'S4' && SCENARIO !== 'S7') {
    return { refundOrders: [] };
  }
  const orders = [];
  for (let i = 0; i < 100; i += 1) {
    const mId = merchantIdByIdx(i);
    const outTradeNo = `REF_BASE_${RUN_ID}_${i}`;
    const createRes = apiCall('POST', '/v1/payments', mId, {
      out_trade_no: outTradeNo,
      amount: 10000000,
      subject: `refund base ${mId}`,
    });
    const createBody = parseJsonSafe(createRes);
    if (createRes.status !== 200 || !createBody || createBody.code !== 0) {
      throw new Error(`setup create payment failed for ${mId}: ${createRes.status} ${createRes.body}`);
    }
    const payNo = createBody.data.pay_no;
    const payRes = apiCall('POST', `/v1/payments/${payNo}/pay`, mId, { channel: 'mockpay' });
    const payBody = parseJsonSafe(payRes);
    if (payRes.status !== 200 || !payBody || payBody.code !== 0) {
      throw new Error(`setup pay failed for ${payNo}: ${payRes.status} ${payRes.body}`);
    }
    orders.push({ merchantId: mId, payNo });
  }

  // 等待全部 100 笔订单变为 SUCCESS，再多等 1s 让 Kafka 完成入账。
  const deadline = Date.now() + 60000;
  const pending = new Set(orders.map((_, i) => i));
  while (pending.size > 0 && Date.now() < deadline) {
    for (const idx of Array.from(pending)) {
      const o = orders[idx];
      const qRes = apiCall('GET', `/v1/payments/${o.payNo}`, o.merchantId, null);
      const qBody = parseJsonSafe(qRes);
      if (qRes.status === 200 && qBody && qBody.code === 0 && qBody.data.status === 'SUCCESS') {
        pending.delete(idx);
      }
    }
    if (pending.size > 0) {
      sleep(0.2);
    }
  }
  if (pending.size > 0) {
    throw new Error(`setup refund base orders did not finish in 60s: remaining=${pending.size}`);
  }
  sleep(2);
  return { refundOrders: orders };
}

function uniqueNo(prefix) {
  const vuId = exec.vu ? exec.vu.idInTest : 0;
  localIter += 1;
  return `${prefix}_${RUN_ID}_${vuId}_${localIter}`;
}

function pickMerchantIdx() {
  const vuId = exec.vu ? exec.vu.idInTest : 1;
  return (vuId + localIter) % 100;
}

function runS1() {
  const mId = merchantIdByIdx(pickMerchantIdx());
  const start = Date.now();
  const res = apiCall('POST', '/v1/payments', mId, {
    out_trade_no: uniqueNo('S1'),
    amount: 1000,
    subject: 'load s1 create',
  });
  const body = parseJsonSafe(res);
  const ok = check(res, {
    'S1 create status 200': (r) => r.status === 200,
    'S1 create code 0': () => body !== null && body.code === 0,
  });
  recordOp(start, ok);
}

function runS2() {
  const mId = merchantIdByIdx(pickMerchantIdx());
  const startCreate = Date.now();
  const cRes = apiCall('POST', '/v1/payments', mId, {
    out_trade_no: uniqueNo('S2'),
    amount: 1000,
    subject: 'load s2 create',
  });
  const cBody = parseJsonSafe(cRes);
  const cOk = check(cRes, {
    'S2 create ok': (r) => r.status === 200 && cBody !== null && cBody.code === 0,
  });
  recordOp(startCreate, cOk);
  if (!cOk) {
    return;
  }
  const payNo = cBody.data.pay_no;
  for (let i = 0; i < 3; i += 1) {
    const startQ = Date.now();
    const qRes = apiCall('GET', `/v1/payments/${payNo}`, mId, null);
    const qBody = parseJsonSafe(qRes);
    const qOk = check(qRes, {
      'S2 query ok': (r) => r.status === 200 && qBody !== null && qBody.code === 0,
    });
    recordOp(startQ, qOk);
  }
}

function runS3() {
  const mId = merchantIdByIdx(pickMerchantIdx());
  const start = Date.now();
  const cRes = apiCall('POST', '/v1/payments', mId, {
    out_trade_no: uniqueNo('S3'),
    amount: 1000,
    subject: 'load s3 pay',
  });
  const cBody = parseJsonSafe(cRes);
  if (cRes.status !== 200 || !cBody || cBody.code !== 0) {
    check(cRes, { 'S3 create ok': () => false });
    recordOp(start, false);
    return;
  }
  const payNo = cBody.data.pay_no;
  const pRes = apiCall('POST', `/v1/payments/${payNo}/pay`, mId, { channel: 'mockpay' });
  const pBody = parseJsonSafe(pRes);
  if (pRes.status !== 200 || !pBody || pBody.code !== 0) {
    check(pRes, { 'S3 pay ok': () => false });
    recordOp(start, false);
    return;
  }
  // 轮询直到终态（channel-mock 延迟 50-200ms）。
  let done = false;
  const deadline = Date.now() + 10000;
  while (Date.now() < deadline) {
    const qRes = apiCall('GET', `/v1/payments/${payNo}`, mId, null);
    const qBody = parseJsonSafe(qRes);
    if (qRes.status === 200 && qBody && qBody.code === 0) {
      const st = qBody.data.status;
      if (st === 'SUCCESS') {
        done = true;
        break;
      }
      if (st === 'FAILED' || st === 'CLOSED') {
        break;
      }
    }
    sleep(0.05);
  }
  check(null, { 'S3 settled SUCCESS': () => done });
  recordOp(start, done);
}

function runS4(data) {
  const orders = data.refundOrders || [];
  if (orders.length === 0) {
    recordOp(Date.now(), false);
    return;
  }
  // 按 VU 分配不同订单，避免多个 VU 同时锁同一笔 payment_order。
  const vuId = exec.vu ? exec.vu.idInTest : 1;
  const idx = (vuId - 1) % orders.length;
  const target = orders[idx];
  const start = Date.now();
  const res = apiCall('POST', '/v1/refunds', target.merchantId, {
    pay_no: target.payNo,
    out_refund_no: uniqueNo('S4'),
    amount: 100,
    reason: 'load s4 partial refund',
  });
  const body = parseJsonSafe(res);
  const ok = check(res, {
    'S4 refund ok': (r) => r.status === 200 && body !== null && body.code === 0,
  });
  recordOp(start, ok);
}

function runS5() {
  // 同分片转账：偶数->偶数 或 奇数->奇数。
  const vuId = exec.vu ? exec.vu.idInTest : 1;
  localIter += 1;
  const shard = (vuId + localIter) % 2;
  const payerIdx = ((vuId * 2 + shard) % 100);
  const payeeIdx = ((payerIdx + 2) % 100);
  const payer = merchantIdByIdx(payerIdx);
  const payee = merchantIdByIdx(payeeIdx);
  const start = Date.now();
  const res = apiCall('POST', '/v1/transfers', payer, {
    out_transfer_no: uniqueNo('S5'),
    payee_merchant_id: payee,
    amount: 100,
    remark: 'load s5 local transfer',
  });
  const body = parseJsonSafe(res);
  const ok = check(res, {
    'S5 transfer ok': (r) => r.status === 200 && body !== null && body.code === 0 && body.data.status === 'SUCCESS',
  });
  recordOp(start, ok);
}

function runS6() {
  // 跨分片转账：偶数->奇数 或 奇数->偶数。
  const vuId = exec.vu ? exec.vu.idInTest : 1;
  localIter += 1;
  const payerIdx = (vuId + localIter) % 100;
  const payeeIdx = (payerIdx + 1) % 100;
  const payer = merchantIdByIdx(payerIdx);
  const payee = merchantIdByIdx(payeeIdx);
  const start = Date.now();
  const res = apiCall('POST', '/v1/transfers', payer, {
    out_transfer_no: uniqueNo('S6'),
    payee_merchant_id: payee,
    amount: 100,
    remark: 'load s6 tcc transfer',
  });
  const body = parseJsonSafe(res);
  const ok = check(res, {
    'S6 transfer ok': (r) => r.status === 200 && body !== null && body.code === 0 && body.data.status === 'SUCCESS',
  });
  recordOp(start, ok);
}

function runS7(data) {
  // 混合：S3 : S4 : S5 : S6 = 5 : 1 : 2 : 2
  const r = Math.random() * 10;
  if (r < 5) {
    runS3();
  } else if (r < 6) {
    runS4(data);
  } else if (r < 8) {
    runS5();
  } else {
    runS6();
  }
}

export default function (data) {
  switch (SCENARIO) {
    case 'S1':
      runS1();
      break;
    case 'S2':
      runS2();
      break;
    case 'S3':
      runS3();
      break;
    case 'S4':
      runS4(data);
      break;
    case 'S5':
      runS5();
      break;
    case 'S6':
      runS6();
      break;
    case 'S7':
      runS7(data);
      break;
    default:
      throw new Error(`unknown scenario: ${SCENARIO}`);
  }
}
