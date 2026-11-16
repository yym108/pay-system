import { check } from 'k6';
import { signMerchantHeaders } from './sign.js';

// 验证 docs/05-api.md §3.7 测试向量 V1 与 V2。
export default function () {
  const secret = 'demo_secret_M100001_0123456789ab';
  const ts = '1791500000';
  const nonce = 'AbCdEf0123456789';

  const v1 = signMerchantHeaders({
    method: 'POST',
    requestUri: '/v1/payments',
    body: '{"out_trade_no":"T20261009001","amount":10000,"subject":"demo"}',
    appId: 'app_m100001',
    appSecret: secret,
    timestamp: ts,
    nonce: nonce,
  });

  const v2 = signMerchantHeaders({
    method: 'GET',
    requestUri: '/v1/payments?out_trade_no=T20261009001',
    body: '',
    appId: 'app_m100001',
    appSecret: secret,
    timestamp: ts,
    nonce: nonce,
  });

  const ok = check(null, {
    'V1 signature matches docs/05-api.md 3.7': () =>
      v1['X-Signature'] === 'hFYKid0+k7q9vCvqh/i6YnvoaAJYzf2niNuvSkMCwoQ=',
    'V2 signature matches docs/05-api.md 3.7': () =>
      v2['X-Signature'] === 'D1t81263+AFhLO6+FPSoVQqNFANBAKpi0akuyE9gVu4=',
  });

  if (!ok) {
    throw new Error(`signature vector mismatch: V1=${v1['X-Signature']} V2=${v2['X-Signature']}`);
  }
}
