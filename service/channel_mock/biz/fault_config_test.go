package biz

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/service/channel_mock/dal"
)

type faultMemNotifyStore struct {
	*memStore
}

func (s *faultMemNotifyStore) AddNotifySent(_ context.Context, id uint64, expected int) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.rows {
		if r.ID == id {
			if r.NotifySentCount == expected && (r.Status == dal.StatusSuccess || r.Status == dal.StatusFailed) {
				r.NotifySentCount++
				return true, nil
			}
			return false, nil
		}
	}
	return false, nil
}

func (s *faultMemNotifyStore) AddDupNotifySent(_ context.Context, id uint64, limit int) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.rows {
		if r.ID == id {
			if r.NotifySentCount < limit && (r.Status == dal.StatusSuccess || r.Status == dal.StatusFailed) {
				r.NotifySentCount++
				return true, nil
			}
			return false, nil
		}
	}
	return false, nil
}

func runAllDue(sched *manualSched, now time.Time) {
	for sched.RunDue(now) > 0 {
	}
}

// CH-07：全局故障配置默认值、更新校验及重置（模拟重启恢复默认值）。
func TestFaultConfigDefaultsUpdateAndReset(t *testing.T) {
	store := NewFaultConfigStore(200, 2000)
	got := store.Get()
	if got.DelayMinMS != 200 || got.DelayMaxMS != 2000 ||
		got.DupNotifyRate != 0 || got.DropNotifyRate != 0 ||
		got.AcceptErrorRate != 0 || got.AcceptTimeoutRate != 0 {
		t.Fatalf("defaults = %+v", got)
	}

	minMS := 50
	maxMS := 200
	dropRate := 0.5
	updated, err := store.Update(FaultConfigPatch{
		DelayMinMS:     &minMS,
		DelayMaxMS:     &maxMS,
		DropNotifyRate: &dropRate,
	})
	if err != nil {
		t.Fatalf("Update valid: %v", err)
	}
	if updated.DelayMinMS != 50 || updated.DelayMaxMS != 200 || updated.DropNotifyRate != 0.5 || updated.DupNotifyRate != 0 {
		t.Fatalf("updated = %+v", updated)
	}

	// 校验非法参数：负数延迟、min > max、概率越界、NaN/Inf。
	neg := -1
	badHigh := 1.01
	badLow := -0.01
	nan := math.NaN()
	cases := []struct {
		name  string
		patch FaultConfigPatch
	}{
		{name: "negative delay_min_ms", patch: FaultConfigPatch{DelayMinMS: &neg}},
		{name: "negative delay_max_ms", patch: FaultConfigPatch{DelayMaxMS: &neg}},
		{name: "min greater than max", patch: func() FaultConfigPatch {
			lo, hi := 300, 200
			return FaultConfigPatch{DelayMinMS: &lo, DelayMaxMS: &hi}
		}()},
		{name: "dup_notify_rate > 1", patch: FaultConfigPatch{DupNotifyRate: &badHigh}},
		{name: "drop_notify_rate < 0", patch: FaultConfigPatch{DropNotifyRate: &badLow}},
		{name: "accept_error_rate > 1", patch: FaultConfigPatch{AcceptErrorRate: &badHigh}},
		{name: "accept_timeout_rate NaN", patch: FaultConfigPatch{AcceptTimeoutRate: &nan}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := store.Update(tc.patch); err == nil {
				t.Fatalf("expected error for %s", tc.name)
			}
			// 失败不破坏已有配置。
			cur := store.Get()
			if cur.DelayMinMS != 50 || cur.DelayMaxMS != 200 || cur.DropNotifyRate != 0.5 {
				t.Fatalf("config mutated on error: %+v", cur)
			}
		})
	}

	// CH-07：重启（新建实例）后恢复默认值。
	restarted := NewFaultConfigStore(200, 2000)
	if def := restarted.Get(); def != (FaultConfig{DelayMinMS: 200, DelayMaxMS: 2000}) {
		t.Fatalf("after restart = %+v", def)
	}
}

// CH-07：完成延迟范围作用于 NORMAL 场景；非 NORMAL 场景保持启动默认值。
func TestServiceGlobalDelayRangeForNormal(t *testing.T) {
	e := newEnv(t)
	minMS, maxMS := 50, 150
	if _, err := e.svc.FaultConfig().Update(FaultConfigPatch{
		DelayMinMS: &minMS,
		DelayMaxMS: &maxMS,
	}); err != nil {
		t.Fatal(err)
	}

	// NORMAL 交易（末两位 00）：使用全局配置 50–150ms。
	if _, err := e.svc.Pay(context.Background(), payReq("PA-NORM-DELAY", 10000)); err != nil {
		t.Fatal(err)
	}
	// FAIL 交易（末两位 01）：仍使用启动默认值 200–2000ms。
	if _, err := e.svc.Pay(context.Background(), payReq("PA-FAIL-DELAY", 10001)); err != nil {
		t.Fatal(err)
	}

	dues := e.sched.dues()
	if len(dues) != 2 {
		t.Fatalf("dues len = %d", len(dues))
	}
	wantStart := baseTime.Truncate(time.Millisecond)
	wantNormDue := wantStart.Add(Delay("PA-NORM-DELAY", 50, 150))
	wantFailDue := wantStart.Add(Delay("PA-FAIL-DELAY", 200, 2000))
	if !dues[0].Equal(wantNormDue) {
		t.Fatalf("normal due = %v, want %v", dues[0], wantNormDue)
	}
	if !dues[1].Equal(wantFailDue) {
		t.Fatalf("fail due = %v, want %v", dues[1], wantFailDue)
	}
}

// CH-07 验收 1（单测）：丢回调概率设为 1 时，所有 NORMAL 交易都不回调；非 NORMAL（如 FAIL）仍照常回调。
func TestNotifierGlobalDropNotifyRate(t *testing.T) {
	store := &faultMemNotifyStore{memStore: newMemStore()}
	clk := &clock.Fake{}
	clk.Set(baseTime)
	sched := &manualSched{}
	ids := &seqIDs{}

	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		_, _ = io.WriteString(w, notifyAck)
	}))
	defer srv.Close()

	fc := NewFaultConfigStore(200, 2000)
	dropOne := 1.0
	if _, err := fc.Update(FaultConfigPatch{DropNotifyRate: &dropOne}); err != nil {
		t.Fatal(err)
	}

	notifier := NewNotifier(store, ids, clk, sched, []byte("test-secret"))
	notifier.SetFaultConfig(fc)

	svc := New(store, ids, clk, sched, Options{
		DelayMinMS: 200,
		DelayMaxMS: 2000,
		OnFinished: notifier.Handle,
	})

	// 发起 3 笔 NORMAL 交易与 1 笔 FAIL 交易。
	for i := range 3 {
		if _, err := svc.Pay(context.Background(), PayRequest{
			ReqNo:     fmt.Sprintf("PA-DROP-%d", i),
			Amount:    10000,
			Subject:   "demo",
			NotifyURL: srv.URL,
		}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := svc.Pay(context.Background(), PayRequest{
		ReqNo:     "PA-FAIL-STILL-NOTIFY",
		Amount:    10001,
		Subject:   "demo",
		NotifyURL: srv.URL,
	}); err != nil {
		t.Fatal(err)
	}

	clk.Advance(5 * time.Second)
	runAllDue(sched, clk.Now())

	// 3 笔 NORMAL 均应为 SUCCESS 且不回调（notify_sent_count = 0）。
	for i := range 3 {
		row, ok := store.row(fmt.Sprintf("PA-DROP-%d", i))
		if !ok || row.Status != dal.StatusSuccess || row.NotifySentCount != 0 {
			t.Fatalf("normal row %d = %+v", i, row)
		}
	}
	// FAIL 交易仍应回调 1 次。
	failRow, ok := store.row("PA-FAIL-STILL-NOTIFY")
	if !ok || failRow.Status != dal.StatusFailed || failRow.NotifySentCount != 1 {
		t.Fatalf("fail row = %+v", failRow)
	}
	if hits.Load() != 1 {
		t.Fatalf("http hits = %d, want 1", hits.Load())
	}
}

// CH-07：回调重复概率设为 1 时，NORMAL 交易按 DUP_NOTIFY 发送 10 次（并发 5 次 + 每秒 1 次再发 5 次）。
func TestNotifierGlobalDupNotifyRate(t *testing.T) {
	store := &faultMemNotifyStore{memStore: newMemStore()}
	clk := &clock.Fake{}
	clk.Set(baseTime)
	sched := &manualSched{}
	ids := &seqIDs{}

	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var p notifyPayload
		if err := json.NewDecoder(r.Body).Decode(&p); err == nil {
			hits.Add(1)
		}
		_, _ = io.WriteString(w, notifyAck)
	}))
	defer srv.Close()

	fc := NewFaultConfigStore(200, 2000)
	dupOne := 1.0
	if _, err := fc.Update(FaultConfigPatch{DupNotifyRate: &dupOne}); err != nil {
		t.Fatal(err)
	}

	notifier := NewNotifier(store, ids, clk, sched, []byte("test-secret"))
	notifier.SetFaultConfig(fc)
	svc := New(store, ids, clk, sched, Options{
		DelayMinMS: 200,
		DelayMaxMS: 2000,
		OnFinished: notifier.Handle,
	})

	if _, err := svc.Pay(context.Background(), PayRequest{
		ReqNo:     "PA-DUP-CFG",
		Amount:    10000,
		Subject:   "demo",
		NotifyURL: srv.URL,
	}); err != nil {
		t.Fatal(err)
	}

	// 自动完成并触发第一波 5 次。
	clk.Advance(3 * time.Second)
	runAllDue(sched, clk.Now())
	if hits.Load() != 5 {
		t.Fatalf("wave1 hits = %d, want 5", hits.Load())
	}
	// 推进 5 秒触发后 5 次。
	for range 5 {
		clk.Advance(time.Second)
		runAllDue(sched, clk.Now())
	}
	if hits.Load() != 10 {
		t.Fatalf("total hits = %d, want 10", hits.Load())
	}
	row, ok := store.row("PA-DUP-CFG")
	if !ok || row.NotifySentCount != 10 {
		t.Fatalf("row = %+v", row)
	}
}

// CH-07：受理接口错误率（500）与超时概率（阻塞 5 秒）作用于 NORMAL，不影响非 NORMAL 场景。
func TestServiceGlobalAcceptErrorAndTimeoutRate(t *testing.T) {
	t.Run("accept_error_rate=1", func(t *testing.T) {
		e := newEnv(t)
		errOne := 1.0
		if _, err := e.svc.FaultConfig().Update(FaultConfigPatch{AcceptErrorRate: &errOne}); err != nil {
			t.Fatal(err)
		}
		// NORMAL 返回错误（handler 映射为 500），不写库。
		if _, err := e.svc.Pay(context.Background(), payReq("PA-ERR-NORM", 10000)); err == nil {
			t.Fatal("expected error for NORMAL when accept_error_rate=1")
		}
		if _, ok := e.store.row("PA-ERR-NORM"); ok {
			t.Fatal("should not insert row on accept error")
		}
		// 非 NORMAL（FAIL 10001）不受影响。
		res, err := e.svc.Pay(context.Background(), payReq("PA-ERR-FAIL", 10001))
		if err != nil || res.Result != ResultOK {
			t.Fatalf("FAIL pay got res=%+v err=%v", res, err)
		}
	})

	t.Run("accept_timeout_rate=1", func(t *testing.T) {
		e := newEnv(t)
		toOne := 1.0
		if _, err := e.svc.FaultConfig().Update(FaultConfigPatch{AcceptTimeoutRate: &toOne}); err != nil {
			t.Fatal(err)
		}
		type payOut struct {
			res PayResult
			err error
		}
		ch := make(chan payOut, 1)
		go func() {
			res, err := e.svc.Pay(context.Background(), payReq("PA-TO-NORM", 10000))
			ch <- payOut{res: res, err: err}
		}()

		// 等待调度器出现 5 秒 hold 任务，且未推进时钟前不返回。
		timer := time.NewTimer(time.Second)
		defer timer.Stop()
		for len(e.sched.dues()) < 2 {
			select {
			case <-timer.C:
				t.Fatalf("expected 2 scheduled tasks (finish + hold), got %d", len(e.sched.dues()))
			default:
				time.Sleep(time.Millisecond)
			}
		}
		select {
		case out := <-ch:
			t.Fatalf("returned before 5s hold: %+v", out)
		default:
		}
		// 推进 5 秒后返回 OK，且行已存在并完成。
		e.clk.Advance(5 * time.Second)
		runAllDue(e.sched, e.clk.Now())
		out := <-ch
		if out.err != nil || out.res.Result != ResultOK {
			t.Fatalf("pay after 5s = %+v, err=%v", out.res, out.err)
		}
	})
}

// CH-07：高并发读写全局故障配置与处理 NORMAL 交易无数据竞争。
func TestFaultConfigConcurrentAccess(t *testing.T) {
	e := newEnv(t)
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := range 50 {
				minMS := 10 + (i % 50)
				maxMS := 100 + (i % 100)
				rate := float64(i%10) / 10.0 //nolint:forbidigo // 概率配置值，非金额
				_, _ = e.svc.FaultConfig().Update(FaultConfigPatch{
					DelayMinMS:     &minMS,
					DelayMaxMS:     &maxMS,
					DropNotifyRate: &rate,
				})
				_ = e.svc.FaultConfig().Get()
				_, _ = e.svc.Pay(context.Background(), payReq(fmt.Sprintf("PA-CONC-%d-%d", gid, i), 10000))
			}
		}(g)
	}
	wg.Wait()
}
