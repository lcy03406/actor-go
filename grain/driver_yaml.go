package grain

import (
	"context"
	"fmt"
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

	// 先用 yaml.Node 接收 data 字段，避免 any 解码时忽略已有类型而创建 map[string]interface{}
	type fileDoc struct {
		Generation int64     `yaml:"generation"`
		Data       yaml.Node `yaml:"data"`
	}
	var doc fileDoc
	if err := yaml.NewDecoder(f).Decode(&doc); err != nil {
		return nil, err
	}

	// 将 data 节点解码到目标类型
	if err := doc.Data.Decode(dst); err != nil {
		return nil, err
	}

	return &LeaseInfo{Key: id, Owner: owner, Generation: doc.Generation + 1}, nil
}

func (d *YamlDriver) Save(_ context.Context, actorType string, id string, _ string, src any, gen int64) error {
	path := d.filePath(actorType, id)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}

	// src 为 nil：仅续租（更新 generation），保留原 data 字段。
	if src == nil {
		return d.renewOnlyYaml(path, gen)
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

// renewOnlyYaml 在 src 为 nil 时仅更新 generation 字段并续租，
// 通过读取整棵 yaml.Node 文档树、原地修改 generation 子节点后整体重写，
// 从而完整保留原 data 字段（含其类型结构）。
func (d *YamlDriver) renewOnlyYaml(path string, gen int64) error {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			// 文件不存在（首次激活前）：直接创建含 generation 的空文档。
			return d.writeRenewDoc(path, gen, nil)
		}
		return err
	}
	var root yaml.Node
	if err := yaml.NewDecoder(f).Decode(&root); err != nil {
		_ = f.Close()
		return err
	}
	_ = f.Close()

	// root 可能是 DocumentNode 包裹的 MappingNode，定位到真正的 Mapping 节点。
	mapping := &root
	if root.Kind == yaml.DocumentNode && len(root.Content) > 0 {
		mapping = root.Content[0]
	}
	if mapping.Kind == yaml.MappingNode {
		for i := 0; i+1 < len(mapping.Content); i += 2 {
			key := mapping.Content[i]
			val := mapping.Content[i+1]
			if key.Value == "generation" {
				val.Value = fmt.Sprintf("%d", gen)
				val.Tag = "!!int"
			}
		}
	}

	out, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() { _ = out.Close() }()
	return yaml.NewEncoder(out).Encode(&root)
}

// writeRenewDoc 创建/重写仅含 generation 的续租文档。
func (d *YamlDriver) writeRenewDoc(path string, gen int64, data *yaml.Node) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	type doc struct {
		Generation int64      `yaml:"generation"`
		Data       *yaml.Node `yaml:"data"`
	}
	return yaml.NewEncoder(f).Encode(doc{Generation: gen, Data: data})
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

	// 用 yaml.Node 保持原始 YAML 结构，避免 any 解码丢失类型信息
	type fileDoc struct {
		Generation int64     `yaml:"generation"`
		Data       yaml.Node `yaml:"data"`
	}

	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 1, d.writeForceReleaseYaml(path, 1, yaml.Node{})
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

func (d *YamlDriver) writeForceReleaseYaml(path string, gen int64, data yaml.Node) error {
	type doc struct {
		Generation int64     `yaml:"generation"`
		Data       yaml.Node `yaml:"data"`
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
