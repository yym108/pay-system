// 公共类型。时间是 RFC3339 毫秒字符串，例如 2026-10-12T10:00:00.000+08:00。
// 业务码是 i32，取值与约定 §5.2 一致。字段只增不删；以后新增字段必须 optional。

namespace go base

// Timestamp 是带数字时区的 RFC3339 毫秒时间。未发生的时间在各 IDL 里标 optional。
typedef string Timestamp

// BizCode 是业务码。0 表示成功。
// 只有必须连同业务对象返回的错误放进响应；其他业务错误走 Kitex BizStatusError。
typedef i32 BizCode
