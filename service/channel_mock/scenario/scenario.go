// Package scenario 定义模拟渠道的确定性故障场景（需求 CH-06）。
//
// 场景由支付金额（分）末两位决定，受理时写入 mock_txn.scenario，之后各功能点按场景名分支。
// 本包只描述场景本身对自动完成（CH-01）的影响；回调、关单、账单等行为由各自功能点实现。
package scenario

// Scenario 是 mock_txn.scenario 列的取值。
type Scenario string

// CH-06 场景表。值与 mock_txn.scenario 列一致。
const (
	Normal         Scenario = "NORMAL"
	Fail           Scenario = "FAIL"
	DropNotify     Scenario = "DROP_NOTIFY"
	DupNotify      Scenario = "DUP_NOTIFY"
	Slow           Scenario = "SLOW"
	AcceptTimeout  Scenario = "ACCEPT_TIMEOUT"
	BillMissing    Scenario = "BILL_MISSING"
	BillAmountDiff Scenario = "BILL_AMOUNT_DIFF"
	NeverFinish    Scenario = "NEVER_FINISH"
	CloseRace      Scenario = "CLOSE_RACE"
)

// byLastTwoDigits 按金额末两位（00–09）查场景，10–99 为 NORMAL。
var byLastTwoDigits = [...]Scenario{
	Normal, Fail, DropNotify, DupNotify, Slow,
	AcceptTimeout, BillMissing, BillAmountDiff, NeverFinish, CloseRace,
}

// ForPay 返回支付金额对应的场景。amount 不是正数时返回 NORMAL；金额合法性由受理校验负责。
func ForPay(amount int64) Scenario {
	if amount <= 0 {
		return Normal
	}
	tail := amount % 100
	if tail < int64(len(byLastTwoDigits)) {
		return byLastTwoDigits[tail]
	}
	return Normal
}

// String 返回列值。
func (s Scenario) String() string { return string(s) }

// AutoFinishes 报告受理后是否按延迟自动完成。只有 NEVER_FINISH 不自动完成。
func (s Scenario) AutoFinishes() bool { return s != NeverFinish }

// Fails 报告自动完成时是否置为 FAILED。只有 FAIL 置 FAILED，其余置 SUCCESS。
func (s Scenario) Fails() bool { return s == Fail }

// InBill 报告成功后是否写 bill_date（进入当天账单）。
// FAIL 不会成功，NEVER_FINISH 不会完成，BILL_MISSING 成功但故意不进账单；未登记的值按 NORMAL。
func (s Scenario) InBill() bool {
	switch s {
	case Fail, BillMissing, NeverFinish:
		return false
	default:
		return true
	}
}
