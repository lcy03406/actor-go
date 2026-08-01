package grain

import (
	"context"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// YamlDriver 基于本地 YAML 文件的持久化驱动。
// 每个 actor 对应一个文件：{dir}/{actorType}/{id}.yaml
// 本地驱动无分布式租约竞争，owner 固定为当前进程，generation 简单递增。
type YamlDriver struct {
	dir string
}

// NewYamlDriver 创建 YAML 文件驱动。dir 为存储根目录。
func NewYamlDriver(dir string) *YamlDriver {
	return &YamlDriver{dir: dir}
}

func (d *YamlDriver) Load(_ context.Context, actorType string, id string, owner string, dst any) (*LeaseInfo, error) {
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
		Generation int64  `yaml:"generation"`
		Data       any    `yaml:"data"`
	}
	var doc fileDoc
	doc.Data = dst
	if err := yaml.NewDecoder(f).Decode(&doc); err != nil {
		return nil, err
	}

	return &LeaseInfo{Key: id, Owner: owner, Generation: doc.Generation + 1}, nil
}

func (d *YamlDriver) Save(_ context.Context, actorType string, id string, _ string, src any, gen int64) error {
	path := d.filePath(actorType, id)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	type doc struct {
		Generation int64 `yaml:"generation"`
		Data       any   `yaml:"data"`
	}
	return yaml.NewEncoder(f).Encode(doc{Generation: gen, Data: src})
}

func (d *YamlDriver) Release(_ context.Context, actorType string, id string, _ string, _ int64) error {
	// 本地驱动 Release 不删除文件
	_ = d.filePath(actorType, id)
	return nil
}

func (d *YamlDriver) ForceRelease(_ context.Context, actorType string, id string) (int64, error) {
	// 本地驱动：递增 generation 并写回文件，模拟租约强制释放，保留数据
	path := d.filePath(actorType, id)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return 0, err
	}

	type fileDoc struct {
		Generation int64 `yaml:"generation"`
		Data       any   `yaml:"data"`
	}

	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 1, d.writeForceReleaseYaml(path, 1, nil)
		}
		return 0, err
	}

	var doc fileDoc
	decodeErr := yaml.NewDecoder(f).Decode(&doc)
	_ = f.Close()
	if decodeErr != nil {
		return 0, decodeErr
	}

	newGen := doc.Generation + 1
	return newGen, d.writeForceReleaseYaml(path, newGen, doc.Data)
}

func (d *YamlDriver) writeForceReleaseYaml(path string, gen int64, data any) error {
	type doc struct {
		Generation int64 `yaml:"generation"`
		Data       any   `yaml:"data"`
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	return yaml.NewEncoder(f).Encode(doc{Generation: gen, Data: data})
}

func (d *YamlDriver) filePath(actorType string, id string) string {
	return filepath.Join(d.dir, actorType, id+".yaml")
}
