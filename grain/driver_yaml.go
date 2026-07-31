package grain

import (
	"context"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// YamlDriver 基于本地 YAML 文件的持久化驱动。
// 每个 actor 对应一个文件：{dir}/{actorType}/{id}.yaml
type YamlDriver struct {
	dir string
}

// NewYamlDriver 创建 YAML 文件驱动。dir 为存储根目录。
func NewYamlDriver(dir string) *YamlDriver {
	return &YamlDriver{dir: dir}
}

func (d *YamlDriver) Load(_ context.Context, actorType string, id string, dst any) error {
	path := d.filePath(actorType, id)
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return ErrNotFound
		}
		return err
	}
	defer func() { _ = f.Close() }()
	return yaml.NewDecoder(f).Decode(dst)
}

func (d *YamlDriver) Save(_ context.Context, actorType string, id string, src any, _ int64) error {
	path := d.filePath(actorType, id)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	return yaml.NewEncoder(f).Encode(src)
}

func (d *YamlDriver) filePath(actorType string, id string) string {
	return filepath.Join(d.dir, actorType, id+".yaml")
}
