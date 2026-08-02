package grain

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestYamlDriver_LoadSave(t *testing.T) {
	dir := t.TempDir()
	d := NewYamlDriver(dir)

	// 首次 Load：文件不存在，返回 ErrNotFound
	var loaded TestGrainSnapshot
	lease, err := d.Load(context.Background(), "test", "actor-1", "node-1", &loaded)
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("first Load: want ErrNotFound, got %v", err)
	}
	if lease == nil || lease.Generation != 1 {
		t.Errorf("first Load: want generation=1, got %v", lease)
	}

	// Save 数据
	snapshot := &TestGrainSnapshot{Value: 100}
	err = d.Save(context.Background(), "test", "actor-1", "node-1", snapshot, 1)
	if err != nil {
		t.Fatalf("save failed: %v", err)
	}

	// 再次 Load：应返回数据
	var loaded2 TestGrainSnapshot
	lease2, err := d.Load(context.Background(), "test", "actor-1", "node-1", &loaded2)
	if err != nil {
		t.Fatalf("second Load failed: %v", err)
	}
	if lease2 == nil {
		t.Fatal("lease should not be nil")
	}
	if loaded2.Value != 100 {
		t.Errorf("want 100, got %d", loaded2.Value)
	}
}

func TestYamlDriver_LoadNotFound(t *testing.T) {
	dir := t.TempDir()
	d := NewYamlDriver(dir)

	var loaded TestGrainSnapshot
	lease, err := d.Load(context.Background(), "test", "no-such-actor", "node-1", &loaded)
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("want ErrNotFound, got %v", err)
	}
	if lease == nil {
		t.Error("lease should not be nil even on ErrNotFound (first activation)")
	}
}

func TestYamlDriver_Overwrite(t *testing.T) {
	dir := t.TempDir()
	d := NewYamlDriver(dir)

	// 首次 Save
	snap := &TestGrainSnapshot{Value: 100}
	if err := d.Save(context.Background(), "test", "actor-ow", "node-1", snap, 1); err != nil {
		t.Fatalf("first save failed: %v", err)
	}

	// 覆盖写入
	snap2 := &TestGrainSnapshot{Value: 200}
	if err := d.Save(context.Background(), "test", "actor-ow", "node-1", snap2, 2); err != nil {
		t.Fatalf("second save failed: %v", err)
	}

	var loaded TestGrainSnapshot
	_, err := d.Load(context.Background(), "test", "actor-ow", "node-1", &loaded)
	if err != nil {
		t.Fatalf("load failed: %v", err)
	}
	if loaded.Value != 200 {
		t.Errorf("overwrite: want 200, got %d", loaded.Value)
	}
}

func TestYamlDriver_MultipleActorTypes(t *testing.T) {
	dir := t.TempDir()
	d := NewYamlDriver(dir)

	snap1 := &TestGrainSnapshot{Value: 1}
	snap2 := &TestGrainSnapshot{Value: 2}

	if err := d.Save(context.Background(), "player", "p1", "node-1", snap1, 1); err != nil {
		t.Fatalf("save player failed: %v", err)
	}
	if err := d.Save(context.Background(), "npc", "p1", "node-1", snap2, 1); err != nil {
		t.Fatalf("save npc failed: %v", err)
	}

	var loaded TestGrainSnapshot
	_, err := d.Load(context.Background(), "player", "p1", "node-1", &loaded)
	if err != nil {
		t.Fatalf("load player failed: %v", err)
	}
	if loaded.Value != 1 {
		t.Errorf("player: want 1, got %d", loaded.Value)
	}

	_, err = d.Load(context.Background(), "npc", "p1", "node-1", &loaded)
	if err != nil {
		t.Fatalf("load npc failed: %v", err)
	}
	if loaded.Value != 2 {
		t.Errorf("npc: want 2, got %d", loaded.Value)
	}
}

func TestYamlDriver_SaveCreatesDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "deep", "nested", "path")
	d := NewYamlDriver(dir)

	snap := &TestGrainSnapshot{Value: 42}
	if err := d.Save(context.Background(), "type", "id", "node-1", snap, 1); err != nil {
		t.Fatalf("save failed: %v", err)
	}

	expectedPath := filepath.Join(dir, "type", "id.yaml")
	if _, err := os.Stat(expectedPath); os.IsNotExist(err) {
		t.Errorf("file not created at %s", expectedPath)
	}
}

func TestYamlDriver_ForceRelease(t *testing.T) {
	dir := t.TempDir()
	d := NewYamlDriver(dir)

	// 保存数据
	snapshot := &TestGrainSnapshot{Value: 100}
	if err := d.Save(context.Background(), "test", "yaml-fr", "node-1", snapshot, 1); err != nil {
		t.Fatalf("save failed: %v", err)
	}

	// 强制释放
	newGen, err := d.ForceRelease(context.Background(), "test", "yaml-fr")
	if err != nil {
		t.Fatalf("ForceRelease failed: %v", err)
	}
	if newGen != 2 {
		t.Errorf("ForceRelease: want newGen=2, got %d", newGen)
	}

	// 释放后数据应保留，其他节点可重新 Load
	var loaded TestGrainSnapshot
	lease, err := d.Load(context.Background(), "test", "yaml-fr", "node-2", &loaded)
	if err != nil {
		t.Fatalf("Load after ForceRelease failed: %v", err)
	}
	// 读取 gen=2，返回 gen+1=3
	if lease.Generation != 3 {
		t.Errorf("Load after ForceRelease: want generation=3, got %d", lease.Generation)
	}
	if loaded.Value != 100 {
		t.Errorf("Load after ForceRelease: data preserved, want 100, got %d", loaded.Value)
	}
}

func TestYamlDriver_ForceRelease_NotFound(t *testing.T) {
	dir := t.TempDir()
	d := NewYamlDriver(dir)

	newGen, err := d.ForceRelease(context.Background(), "test", "no-such")
	if err != nil {
		t.Fatalf("ForceRelease on not found: %v", err)
	}
	if newGen != 1 {
		t.Errorf("ForceRelease on not found: want 1, got %d", newGen)
	}
}

func TestYamlDriver_Release(t *testing.T) {
	dir := t.TempDir()
	d := NewYamlDriver(dir)

	// 先保存数据
	snap := &TestGrainSnapshot{Value: 77}
	if err := d.Save(context.Background(), "test", "actor-rel", "node-1", snap, 1); err != nil {
		t.Fatalf("save failed: %v", err)
	}

	// Release 不删除数据
	if err := d.Release(context.Background(), "test", "actor-rel", "node-1", 1); err != nil {
		t.Fatalf("Release failed: %v", err)
	}

	// 确认文件仍然存在，数据未丢失
	var loaded TestGrainSnapshot
	_, err := d.Load(context.Background(), "test", "actor-rel", "node-1", &loaded)
	if err != nil {
		t.Fatalf("Load after Release failed: %v", err)
	}
	if loaded.Value != 77 {
		t.Errorf("Release should not delete data: want 77, got %d", loaded.Value)
	}
}
