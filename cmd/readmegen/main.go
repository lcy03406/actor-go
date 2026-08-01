// Command readmegen 把 example_test.go 里的示例片段同步进 README。
//
// 示例的唯一事实来源是各包的 example_test.go：片段用 region 标记圈出，
// README 用同名 marker 占位，由本工具注入。示例因此始终是编译单元的一部分
// （go test 会编译 example_test.go），不会再随 API 演进慢慢腐化。
//
// 用法：
//
//	go run ./cmd/readmegen            # 把 region 注入 README.md / README.zh.md
//	go run ./cmd/readmegen -check     # 只校验 README 是否最新，不一致时非零退出
//
// 标记格式（Go 侧，标记行本身不会进入 README）：
//
//	// region: send
//	actor.Post(mgr, playerId, &Login{InitHP: 100})
//	// endregion: send
//
// 标记格式（README 侧，两个 marker 之间（含围栏）由工具生成）：
//
//	<!-- example: actor/send -->
//	```go
//	...
//	```
//	<!-- /example: actor/send -->
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const (
	regionBegin = "// region: "
	regionEnd   = "// endregion: "
	markerBegin = "<!-- example: "
	markerEnd   = "<!-- /example: "
	markerClose = " -->"
)

func main() {
	root := flag.String("root", ".", "仓库根目录（扫描其一级子包中的 example_test.go）")
	check := flag.Bool("check", false, "只校验 README 是否最新，不写回")
	readmes := flag.String("readmes", "README.md,README.zh.md", "要同步的 README，逗号分隔")
	flag.Parse()

	regions, err := loadRegions(*root)
	if err != nil {
		fail(err)
	}

	names := strings.Split(*readmes, ",")
	for _, name := range names {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		path := filepath.Join(*root, name)
		if err := syncFile(path, regions, *check); err != nil {
			fail(err)
		}
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "readmegen:", err)
	os.Exit(1)
}

// loadRegions 扫描 <root>/*/example_test.go 中的 region，返回 "包名/region 名" → 片段行。
func loadRegions(root string) (map[string][]string, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}

	regions := make(map[string][]string)
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		path := filepath.Join(root, entry.Name(), "example_test.go")
		data, err := os.ReadFile(path)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, err
		}
		if err := parseRegions(path, entry.Name(), string(data), regions); err != nil {
			return nil, err
		}
	}
	if len(regions) == 0 {
		return nil, fmt.Errorf("在 %s 下没有找到任何 region", root)
	}
	return regions, nil
}

func parseRegions(path, pkg, src string, regions map[string][]string) error {
	var (
		name  string
		body  []string
		line0 int
	)
	for i, line := range strings.Split(toLF(src), "\n") {
		trimmed := strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(trimmed, regionBegin):
			if name != "" {
				return fmt.Errorf("%s:%d: region %q 未结束（缺少 endregion）", path, line0, name)
			}
			name = strings.TrimSpace(strings.TrimPrefix(trimmed, regionBegin))
			line0 = i + 1
			body = nil
		case strings.HasPrefix(trimmed, regionEnd):
			end := strings.TrimSpace(strings.TrimPrefix(trimmed, regionEnd))
			if end != name {
				return fmt.Errorf("%s:%d: endregion %q 与 region %q 不匹配", path, i+1, end, name)
			}
			key := pkg + "/" + name
			if _, dup := regions[key]; dup {
				return fmt.Errorf("%s:%d: region 重复定义: %s", path, line0, key)
			}
			regions[key] = dedent(body)
			name = ""
		default:
			if name != "" {
				body = append(body, line)
			}
		}
	}
	if name != "" {
		return fmt.Errorf("%s:%d: region %q 未结束（缺少 endregion）", path, line0, name)
	}
	return nil
}

// dedent 去掉所有非空行的公共前导空白，让函数体内的片段在 README 中顶格显示。
func dedent(lines []string) []string {
	prefix := ""
	for i, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		indent := line[:len(line)-len(strings.TrimLeft(line, " \t"))]
		if i == 0 || prefix == "" {
			prefix = indent
			continue
		}
		prefix = commonPrefix(prefix, indent)
	}
	out := make([]string, len(lines))
	for i, line := range lines {
		if strings.TrimSpace(line) == "" {
			out[i] = ""
			continue
		}
		out[i] = strings.TrimPrefix(line, prefix)
	}
	return out
}

// trimBlank 去掉片段首尾的空行：region 标记通常会独占一行，
// 其上下文会带来多余空行，直接注入会让代码块末尾多出空行。
func trimBlank(lines []string) []string {
	start, end := 0, len(lines)
	for start < end && strings.TrimSpace(lines[start]) == "" {
		start++
	}
	for end > start && strings.TrimSpace(lines[end-1]) == "" {
		end--
	}
	return lines[start:end]
}

func commonPrefix(a, b string) string {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return a[:i]
		}
	}
	return a[:n]
}

// toLF 把行尾统一成 LF。Windows 工作区的文件（core.autocrlf=true）是 CRLF，
// 不清掉 \r 会让 region 里的每行都带一个 \r，注入后变成空行。
func toLF(s string) string {
	s = strings.ReplaceAll(s, "\r\n", "\n")
	return strings.ReplaceAll(s, "\r", "")
}

// syncFile 把 regions 注入一个 README。-check 时不写回，只报告差异。
// 内部统一按 LF 处理，写回时恢复文件原有的 CRLF 行尾，避免整篇被改写。
func syncFile(path string, regions map[string][]string, check bool) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	crlf := strings.Contains(string(data), "\r\n")
	src := toLF(string(data))

	rendered, err := render(path, src, regions)
	if err != nil {
		return err
	}
	if crlf {
		rendered = strings.ReplaceAll(rendered, "\n", "\r\n")
	}
	if rendered == string(data) {
		return nil
	}
	if check {
		return fmt.Errorf("%s 已过期：运行 go run ./cmd/readmegen 重新生成（首个差异在%s）",
			path, firstDiff(string(data), rendered))
	}
	return os.WriteFile(path, []byte(rendered), 0o644)
}

func render(path, src string, regions map[string][]string) (string, error) {
	lines := strings.Split(src, "\n")
	out := make([]string, 0, len(lines))
	used := make(map[string]bool)

	for i := 0; i < len(lines); i++ {
		trimmed := strings.TrimSpace(lines[i])
		name, ok := markerName(trimmed, markerBegin)
		if !ok {
			out = append(out, lines[i])
			continue
		}

		body, ok := regions[name]
		if !ok {
			return "", fmt.Errorf("%s:%d: README 引用了不存在的 region: %s", path, i+1, name)
		}
		body = trimBlank(body)
		used[name] = true

		end := -1
		for j := i + 1; j < len(lines); j++ {
			if endName, ok := markerName(strings.TrimSpace(lines[j]), markerEnd); ok {
				if endName != name {
					return "", fmt.Errorf("%s:%d: 结束标记 %q 与开始标记 %q 不匹配", path, j+1, endName, name)
				}
				end = j
				break
			}
		}
		if end < 0 {
			return "", fmt.Errorf("%s:%d: marker %q 未闭合", path, i+1, name)
		}

		out = append(out, markerBegin+name+markerClose)
		out = append(out, "```go")
		out = append(out, body...)
		out = append(out, "```")
		out = append(out, markerEnd+name+markerClose)
		i = end
	}

	rendered := strings.Join(out, "\n")
	if err := checkUnmanagedGoBlocks(path, rendered); err != nil {
		return "", err
	}
	for name := range regions {
		if !used[name] {
			return "", fmt.Errorf("%s: region %q 没有被任何 README 引用", path, name)
		}
	}
	return rendered, nil
}

// markerName 解析 "<!-- example: name -->" / "<!-- /example: name -->"。
func markerName(line, prefix string) (string, bool) {
	if !strings.HasPrefix(line, prefix) || !strings.HasSuffix(line, markerClose) {
		return "", false
	}
	return strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(line, prefix), markerClose)), true
}

// checkUnmanagedGoBlocks 拒绝游离的 go 代码块：所有示例都必须来自可编译的 region，
// 否则 README 里又会慢慢长出没人校验的片段。
func checkUnmanagedGoBlocks(path, src string) error {
	prev := ""
	for i, line := range strings.Split(src, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed != "```go" {
			if trimmed != "" {
				prev = trimmed
			}
			continue
		}
		if _, ok := markerName(prev, markerBegin); !ok {
			return fmt.Errorf("%s:%d: go 代码块未被 <!-- example: ... --> 标记包裹；"+
				"请把片段移入包的 example_test.go region", path, i+1)
		}
	}
	return nil
}

func firstDiff(a, b string) string {
	al := strings.Split(a, "\n")
	bl := strings.Split(b, "\n")
	for i := 0; i < len(al) && i < len(bl); i++ {
		if al[i] != bl[i] {
			return fmt.Sprintf("第 %d 行", i+1)
		}
	}
	if len(al) != len(bl) {
		return fmt.Sprintf("第 %d 行（行数不同）", min(len(al), len(bl))+1)
	}
	return "末尾"
}
