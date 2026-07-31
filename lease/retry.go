package lease

import (
	"context"
	"errors"
	"net"
	"time"
)

// isTransientNetErrBase 判断是否为瞬态网络错误（可重试）。
// 逻辑错误（ErrNotAcquired / ErrLeaseExpired）不在此列。
// 各后端可在其基础上增加特有的网络错误检查。
func isTransientNetErrBase(err error) bool {
	if err == nil {
		return false
	}
	// 逻辑错误不重试
	if errors.Is(err, ErrNotAcquired) || errors.Is(err, ErrLeaseExpired) {
		return false
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return netErr.Timeout() || netErr.Temporary()
	}
	var dnsErr *net.DNSError
	return errors.As(err, &dnsErr)
}

// sleepBackoff 指数退避等待，最大 2 秒。
func sleepBackoff(ctx context.Context, attempt int) {
	const base = 50 * time.Millisecond
	const max = 2 * time.Second
	d := base * (1 << attempt)
	if d > max {
		d = max
	}
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}
