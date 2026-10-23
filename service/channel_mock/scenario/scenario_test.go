package scenario

import "testing"

// CH-01 规则 3：scenario 由支付金额（分）末两位决定（CH-06 表），其他末两位为 NORMAL。
func TestForPay(t *testing.T) {
	cases := []struct {
		amount int64
		want   Scenario
	}{
		{10000, Normal},
		{10001, Fail},
		{10002, DropNotify},
		{10003, DupNotify},
		{10004, Slow},
		{10005, AcceptTimeout},
		{10006, BillMissing},
		{10007, BillAmountDiff},
		{10008, NeverFinish},
		{10009, CloseRace},
		{10010, Normal},
		{10099, Normal},
		{1, Fail},
		{9, CloseRace},
		{100, Normal},
		// 非法金额不在这里拒绝，由受理校验返回 400；这里只保证不会解析出故障场景。
		{0, Normal},
		{-1, Normal},
		{-101, Normal},
	}
	for _, c := range cases {
		if got := ForPay(c.amount); got != c.want {
			t.Errorf("ForPay(%d) = %s, want %s", c.amount, got, c.want)
		}
	}
}

// CH-01 规则 4：只有 FAIL 置 FAILED，NEVER_FINISH 不自动完成，BILL_MISSING 不进账单。
func TestOutcomeRules(t *testing.T) {
	cases := []struct {
		s        Scenario
		finishes bool
		fails    bool
		inBill   bool
	}{
		{Normal, true, false, true},
		{Fail, true, true, false},
		{DropNotify, true, false, true},
		{DupNotify, true, false, true},
		{Slow, true, false, true},
		{AcceptTimeout, true, false, true},
		{BillMissing, true, false, false},
		{BillAmountDiff, true, false, true},
		{NeverFinish, false, false, false},
		{CloseRace, true, false, true},
		// 库里出现未登记的值时按 NORMAL 处理，不会让交易永远停在 PROCESSING。
		{Scenario("UNKNOWN"), true, false, true},
	}
	for _, c := range cases {
		if got := c.s.AutoFinishes(); got != c.finishes {
			t.Errorf("%s AutoFinishes = %v", c.s, got)
		}
		if got := c.s.Fails(); got != c.fails {
			t.Errorf("%s Fails = %v", c.s, got)
		}
		if got := c.s.InBill(); got != c.inBill {
			t.Errorf("%s InBill = %v", c.s, got)
		}
	}
}

func TestStringMatchesColumnValue(t *testing.T) {
	if Normal.String() != "NORMAL" || BillMissing.String() != "BILL_MISSING" {
		t.Fatal("scenario names must match mock_txn.scenario values")
	}
}
