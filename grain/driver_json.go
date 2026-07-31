package grain

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
)

// Driver 是持久化驱动接口，负责数据的实际存取。
// 框架通过 actorType + id 定位数据，src/dst 是 *S（快照指针）。
type Driver interface {
	// Load 加载快照。若数据不存在，返回 nil（首次激活使用零值 D）。
	Load(ctx context.Context, actorType string, id string, dst any) error

	// Save 保存快照。gen 是 fencing token（monotonically increasing generation）。
	// 实现应使用 gen 防止过期写入：仅当存储中的 generation <= gen 时才写入。
	Save(ctx context.Context, actorType string, id string, src any, gen int64) error
}

// ErrNotFound 表示数据不存在，Load 返回此错误时框架使用零值初始化。
var ErrNotFound = errors.New("grain: snapshot not found")

// ─── JSON Driver ───

// JsonDriver 基于本地 JSON 文件的持久化驱动。
// 每个 actor 对应一个文件：{dir}/{actorType}/{id}.json
type JsonDriver struct {
	dir string
}

// NewJsonDriver 创建 JSON 文件驱动。dir 为存储根目录。
func NewJsonDriver(dir string) *JsonDriver {
	return &JsonDriver{dir: dir}
}

func (d *JsonDriver) Load(_ context.Context, actorType string, id string, dst any) error {
	path := d.filePath(actorType, id)
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return ErrNotFound
		}
		return err
	}
	defer f.Close()
	return json.NewDecoder(f).Decode(dst)
}

func (d *JsonDriver) Save(_ context.Context, actorType string, id string, src any, _ int64) error {
	path := d.filePath(actorType, id)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(src)
}

func (d *JsonDriver) filePath(actorType string, id string) string {
	return filepath.Join(d.dir, actorType, id+".json")
}
