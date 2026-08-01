package grain

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// DefaultLeaseTimeout 默认租约超时时间。
const DefaultLeaseTimeout = 10 * time.Minute

// LeaseInfo 是 Load 返回的租约信息。
// 激活时 Load 同时完成 Acquire，Load 成功意味着租约已获取。
type LeaseInfo struct {
	Key        string
	Owner      string
	Generation int64
}

// ErrLeaseTaken 表示租约已被其他节点持有。
// 调用方可通过字段判断是否转发请求、探测持有者或强制释放。
type ErrLeaseTaken struct {
	Key        string
	Owner      string
	Generation int64
}

func (e *ErrLeaseTaken) Error() string {
	return fmt.Sprintf("grain: lease %s taken by %s (generation=%d)", e.Key, e.Owner, e.Generation)
}

// Driver 是持久化驱动接口，内置租约管理。
// 框架通过 actorType + id 定位数据，src/dst 是 *S（快照指针）。
//
// Load 同时完成"加载快照 + 获取租约"，是原子操作：
//   - 文档不存在 → 创建文档，设置 owner，返回 ErrNotFound + LeaseInfo（首次激活，用零值）
//   - 文档存在且无主或租约已过期 → 抢占，返回数据 + 新的 LeaseInfo
//   - 文档存在且被其他节点持有且未过期 → 返回 ErrLeaseTaken（含持有者信息）
//
// Save 同时完成"保存快照 + 续租"：
//   - generation 匹配 → 写入数据 + 续租 TTL
//   - generation 不匹配 → 租约已被抢占，拒绝写入
type Driver interface {
	// Load 加载快照并获取租约。
	// 返回的 lease 非 nil 表示成功获取租约。
	// 若数据不存在，返回 ErrNotFound，但 lease 仍然有效（首次激活）。
	Load(ctx context.Context, actorType string, id string, owner string, dst any) (*LeaseInfo, error)

	// Save 保存快照并续租。
	// gen 和 owner 为当前持有的租约信息，用于 fencing 校验。
	Save(ctx context.Context, actorType string, id string, owner string, src any, gen int64) error

	// Release 释放租约（清空 owner），不删除数据。
	// gen 和 owner 匹配时才释放。
	Release(ctx context.Context, actorType string, id string, owner string, gen int64) error

	// ForceRelease 强制释放租约，不检查 owner 和 generation。
	// 用于租约持有者不可达时的强制接管，不删除数据。
	// 成功释放后返回新的 generation，调用方可直接以新 generation 抢占。
	ForceRelease(ctx context.Context, actorType string, id string) (newGeneration int64, err error)
}

// ErrNotFound 表示数据不存在，Load 返回此错误时框架使用零值初始化。
var ErrNotFound = errors.New("grain: snapshot not found")

// ─── JSON Driver ───

// JsonDriver 基于本地 JSON 文件的持久化驱动。
// 每个 actor 对应一个文件：{dir}/{actorType}/{id}.json
// 本地驱动无分布式租约竞争，owner 固定为当前进程，generation 简单递增。
type JsonDriver struct {
	dir string
}

// NewJsonDriver 创建 JSON 文件驱动。dir 为存储根目录。
func NewJsonDriver(dir string) *JsonDriver {
	return &JsonDriver{dir: dir}
}

func (d *JsonDriver) Load(_ context.Context, actorType string, id string, owner string, dst any) (*LeaseInfo, error) {
	path := d.filePath(actorType, id)
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return &LeaseInfo{Key: id, Owner: owner, Generation: 1}, ErrNotFound
		}
		return nil, err
	}
	defer func() { _ = f.Close() }()

	type fileDoc struct {
		Generation int64           `json:"generation"`
		Data       json.RawMessage `json:"data"`
	}
	var doc fileDoc
	if err := json.NewDecoder(f).Decode(&doc); err != nil {
		return nil, err
	}

	// 解码快照数据
	if len(doc.Data) > 0 {
		if err := json.Unmarshal(doc.Data, dst); err != nil {
			return nil, err
		}
	}

	return &LeaseInfo{Key: id, Owner: owner, Generation: doc.Generation + 1}, nil
}

func (d *JsonDriver) Save(_ context.Context, actorType string, id string, _ string, src any, gen int64) error {
	path := d.filePath(actorType, id)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	// 写入 generation 和快照数据
	type doc struct {
		Generation int64 `json:"generation"`
		Data       any   `json:"data"`
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(doc{Generation: gen, Data: src})
}

func (d *JsonDriver) Release(_ context.Context, actorType string, id string, _ string, _ int64) error {
	path := d.filePath(actorType, id)
	// 本地驱动 Release 不删除文件，保持数据持久化
	_ = path
	return nil
}

func (d *JsonDriver) ForceRelease(_ context.Context, actorType string, id string) (int64, error) {
	// 本地驱动：递增 generation 并写回文件，模拟租约强制释放，保留数据
	path := d.filePath(actorType, id)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return 0, err
	}

	type fileDoc struct {
		Generation int64           `json:"generation"`
		Data       json.RawMessage `json:"data"`
	}

	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 1, d.writeForceReleaseDoc(path, 1, nil)
		}
		return 0, err
	}

	var doc fileDoc
	decodeErr := json.NewDecoder(f).Decode(&doc)
	_ = f.Close()
	if decodeErr != nil {
		return 0, decodeErr
	}

	newGen := doc.Generation + 1
	return newGen, d.writeForceReleaseDoc(path, newGen, doc.Data)
}

func (d *JsonDriver) writeForceReleaseDoc(path string, gen int64, data json.RawMessage) error {
	type doc struct {
		Generation int64           `json:"generation"`
		Data       json.RawMessage `json:"data"`
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(doc{Generation: gen, Data: data})
}

func (d *JsonDriver) filePath(actorType string, id string) string {
	return filepath.Join(d.dir, actorType, id+".json")
}
