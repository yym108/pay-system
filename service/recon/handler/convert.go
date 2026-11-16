package handler

import (
	"time"

	"github.com/yym108/pay-system/kitex_gen/base"
	"github.com/yym108/pay-system/kitex_gen/recon"
	"github.com/yym108/pay-system/service/recon/dal"
)

const timestampLayout = "2006-01-02T15:04:05.000Z07:00"

var shanghai = time.FixedZone("Asia/Shanghai", 8*3600)

func toBatch(b dal.Batch) *recon.ReconBatch {
	out := &recon.ReconBatch{
		BatchNo:       b.BatchNo,
		Channel:       b.Channel,
		BillDate:      b.BillDate,
		Status:        b.Status,
		ChannelCount:  b.ChannelCount,
		ChannelAmount: b.ChannelAmount,
		LocalCount:    b.LocalCount,
		LocalAmount:   b.LocalAmount,
		MatchedCount:  b.MatchedCount,
		DiffCount:     b.DiffCount,
		ErrorMsg:      derefString(b.ErrorMsg),
	}
	if b.StartedAt != nil {
		ts := formatTimestamp(*b.StartedAt)
		out.StartedAt = &ts
	}
	if b.FinishedAt != nil {
		ts := formatTimestamp(*b.FinishedAt)
		out.FinishedAt = &ts
	}
	return out
}

func toDiffs(list []dal.Diff) []*recon.ReconDiff {
	out := make([]*recon.ReconDiff, 0, len(list))
	for _, d := range list {
		item := &recon.ReconDiff{
			DiffNo:        d.DiffNo,
			BatchNo:       d.BatchNo,
			Channel:       d.Channel,
			BillDate:      d.BillDate,
			DiffType:      d.DiffType,
			TxnType:       d.TxnType,
			BizNo:         d.BizNo,
			ChannelTxnNo:  d.ChannelTxnNo,
			LocalAmount:   d.LocalAmount,
			ChannelAmount: d.ChannelAmount,
			LocalStatus:   d.LocalStatus,
			ChannelStatus: d.ChannelStatus,
			Status:        d.Status,
			HandleAction:  d.HandleAction,
			Handler:       derefString(d.Handler),
			HandleNote:    derefString(d.HandleNote),
			CreatedAt:     formatTimestamp(d.CreatedAt),
		}
		if d.ResolvedAt != nil {
			ts := formatTimestamp(*d.ResolvedAt)
			item.ResolvedAt = &ts
		}
		out = append(out, item)
	}
	return out
}

func derefString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func formatTimestamp(t time.Time) base.Timestamp {
	return base.Timestamp(t.Truncate(time.Millisecond).In(shanghai).Format(timestampLayout))
}
