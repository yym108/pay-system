package logx

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/cloudwego/hertz/pkg/common/hlog"
	"github.com/cloudwego/kitex/pkg/klog"
)

// hlog 和 klog 的级别都是从 Trace 起的 iota，数值一样，但和 slog.Level 不是同一套。
const (
	levelTrace  = slog.LevelDebug - 4
	levelDebug  = slog.LevelDebug
	levelInfo   = slog.LevelInfo
	levelNotice = slog.LevelInfo + 1
	levelWarn   = slog.LevelWarn
	levelError  = slog.LevelError
	levelFatal  = slog.LevelError + 4
)

func fromForeignLevel(lv int) slog.Level {
	switch lv {
	case 0:
		return levelTrace
	case 1:
		return levelDebug
	case 2:
		return levelInfo
	case 3:
		return levelNotice
	case 4:
		return levelWarn
	case 5:
		return levelError
	case 6:
		return levelFatal
	default:
		return levelInfo
	}
}

// bridge 把框架日志写进 SetDefault 创建的那一个 slog。
type bridge struct {
	logger *slog.Logger
	level  *slog.LevelVar
}

func (b *bridge) write(ctx context.Context, level slog.Level, msg string) {
	// slog 自己把 nil context 换成 Background，这里不再新造一个。
	b.logger.Log(ctx, level, msg)
}

func (b *bridge) exit(ctx context.Context, msg string) {
	b.write(ctx, levelFatal, msg)
	os.Exit(1)
}

// SetOutput 不改 writer。输出端属于共用的 handler，两边框架不能各自换一份。
type hertzLogger struct{ *bridge }

func (l *hertzLogger) SetOutput(io.Writer) {}

func (l *hertzLogger) SetLevel(lv hlog.Level) { l.level.Set(fromForeignLevel(int(lv))) }

func (l *hertzLogger) Trace(v ...any) { l.write(context.Background(), levelTrace, fmt.Sprint(v...)) }
func (l *hertzLogger) Debug(v ...any) { l.write(context.Background(), levelDebug, fmt.Sprint(v...)) }
func (l *hertzLogger) Info(v ...any)  { l.write(context.Background(), levelInfo, fmt.Sprint(v...)) }
func (l *hertzLogger) Notice(v ...any) {
	l.write(context.Background(), levelNotice, fmt.Sprint(v...))
}
func (l *hertzLogger) Warn(v ...any)  { l.write(context.Background(), levelWarn, fmt.Sprint(v...)) }
func (l *hertzLogger) Error(v ...any) { l.write(context.Background(), levelError, fmt.Sprint(v...)) }
func (l *hertzLogger) Fatal(v ...any) { l.exit(context.Background(), fmt.Sprint(v...)) }

func (l *hertzLogger) Tracef(format string, v ...any) {
	l.write(context.Background(), levelTrace, fmt.Sprintf(format, v...))
}
func (l *hertzLogger) Debugf(format string, v ...any) {
	l.write(context.Background(), levelDebug, fmt.Sprintf(format, v...))
}
func (l *hertzLogger) Infof(format string, v ...any) {
	l.write(context.Background(), levelInfo, fmt.Sprintf(format, v...))
}
func (l *hertzLogger) Noticef(format string, v ...any) {
	l.write(context.Background(), levelNotice, fmt.Sprintf(format, v...))
}
func (l *hertzLogger) Warnf(format string, v ...any) {
	l.write(context.Background(), levelWarn, fmt.Sprintf(format, v...))
}
func (l *hertzLogger) Errorf(format string, v ...any) {
	l.write(context.Background(), levelError, fmt.Sprintf(format, v...))
}
func (l *hertzLogger) Fatalf(format string, v ...any) {
	l.exit(context.Background(), fmt.Sprintf(format, v...))
}

func (l *hertzLogger) CtxTracef(ctx context.Context, format string, v ...any) {
	l.write(ctx, levelTrace, fmt.Sprintf(format, v...))
}
func (l *hertzLogger) CtxDebugf(ctx context.Context, format string, v ...any) {
	l.write(ctx, levelDebug, fmt.Sprintf(format, v...))
}
func (l *hertzLogger) CtxInfof(ctx context.Context, format string, v ...any) {
	l.write(ctx, levelInfo, fmt.Sprintf(format, v...))
}
func (l *hertzLogger) CtxNoticef(ctx context.Context, format string, v ...any) {
	l.write(ctx, levelNotice, fmt.Sprintf(format, v...))
}
func (l *hertzLogger) CtxWarnf(ctx context.Context, format string, v ...any) {
	l.write(ctx, levelWarn, fmt.Sprintf(format, v...))
}
func (l *hertzLogger) CtxErrorf(ctx context.Context, format string, v ...any) {
	l.write(ctx, levelError, fmt.Sprintf(format, v...))
}
func (l *hertzLogger) CtxFatalf(ctx context.Context, format string, v ...any) {
	l.exit(ctx, fmt.Sprintf(format, v...))
}

type kitexLogger struct{ *bridge }

func (l *kitexLogger) SetOutput(io.Writer) {}

func (l *kitexLogger) SetLevel(lv klog.Level) { l.level.Set(fromForeignLevel(int(lv))) }

func (l *kitexLogger) Trace(v ...any) { l.write(context.Background(), levelTrace, fmt.Sprint(v...)) }
func (l *kitexLogger) Debug(v ...any) { l.write(context.Background(), levelDebug, fmt.Sprint(v...)) }
func (l *kitexLogger) Info(v ...any)  { l.write(context.Background(), levelInfo, fmt.Sprint(v...)) }
func (l *kitexLogger) Notice(v ...any) {
	l.write(context.Background(), levelNotice, fmt.Sprint(v...))
}
func (l *kitexLogger) Warn(v ...any)  { l.write(context.Background(), levelWarn, fmt.Sprint(v...)) }
func (l *kitexLogger) Error(v ...any) { l.write(context.Background(), levelError, fmt.Sprint(v...)) }
func (l *kitexLogger) Fatal(v ...any) { l.exit(context.Background(), fmt.Sprint(v...)) }

func (l *kitexLogger) Tracef(format string, v ...any) {
	l.write(context.Background(), levelTrace, fmt.Sprintf(format, v...))
}
func (l *kitexLogger) Debugf(format string, v ...any) {
	l.write(context.Background(), levelDebug, fmt.Sprintf(format, v...))
}
func (l *kitexLogger) Infof(format string, v ...any) {
	l.write(context.Background(), levelInfo, fmt.Sprintf(format, v...))
}
func (l *kitexLogger) Noticef(format string, v ...any) {
	l.write(context.Background(), levelNotice, fmt.Sprintf(format, v...))
}
func (l *kitexLogger) Warnf(format string, v ...any) {
	l.write(context.Background(), levelWarn, fmt.Sprintf(format, v...))
}
func (l *kitexLogger) Errorf(format string, v ...any) {
	l.write(context.Background(), levelError, fmt.Sprintf(format, v...))
}
func (l *kitexLogger) Fatalf(format string, v ...any) {
	l.exit(context.Background(), fmt.Sprintf(format, v...))
}

func (l *kitexLogger) CtxTracef(ctx context.Context, format string, v ...any) {
	l.write(ctx, levelTrace, fmt.Sprintf(format, v...))
}
func (l *kitexLogger) CtxDebugf(ctx context.Context, format string, v ...any) {
	l.write(ctx, levelDebug, fmt.Sprintf(format, v...))
}
func (l *kitexLogger) CtxInfof(ctx context.Context, format string, v ...any) {
	l.write(ctx, levelInfo, fmt.Sprintf(format, v...))
}
func (l *kitexLogger) CtxNoticef(ctx context.Context, format string, v ...any) {
	l.write(ctx, levelNotice, fmt.Sprintf(format, v...))
}
func (l *kitexLogger) CtxWarnf(ctx context.Context, format string, v ...any) {
	l.write(ctx, levelWarn, fmt.Sprintf(format, v...))
}
func (l *kitexLogger) CtxErrorf(ctx context.Context, format string, v ...any) {
	l.write(ctx, levelError, fmt.Sprintf(format, v...))
}
func (l *kitexLogger) CtxFatalf(ctx context.Context, format string, v ...any) {
	l.exit(ctx, fmt.Sprintf(format, v...))
}

var (
	_ hlog.FullLogger = (*hertzLogger)(nil)
	_ klog.FullLogger = (*kitexLogger)(nil)
)
