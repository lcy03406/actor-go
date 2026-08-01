// Package grain 提供带租约管理的持久化 Actor 工具。详见 lifecycle.go。
package grain

import "errors"

// ErrNoDriver 表示 PersistenceManager 未配置 Driver。
var ErrNoDriver = errors.New("grain: no driver configured in PersistenceManager")
