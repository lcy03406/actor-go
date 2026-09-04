package logger

import (
	"encoding/hex"
	"log/slog"
	"reflect"
	"strconv"
	"strings"
	"sync"
)

// fieldMeta 缓存结构体字段元信息（与之前相同）
type fieldMeta struct {
	JSONName string
	LogTag   string
	Index    int
	Type     reflect.Type
}

var fieldCache sync.Map

func getFieldMetas(typ reflect.Type) []fieldMeta {
	if cached, ok := fieldCache.Load(typ); ok {
		return cached.([]fieldMeta)
	}
	numField := typ.NumField()
	metas := make([]fieldMeta, 0, numField)
	for i := 0; i < numField; i++ {
		field := typ.Field(i)
		if field.PkgPath != "" && !field.Anonymous {
			continue
		}
		logTag := field.Tag.Get("log")
		jsonName := field.Name
		if jsonTag := field.Tag.Get("json"); jsonTag != "" {
			parts := strings.Split(jsonTag, ",")
			if parts[0] != "" && parts[0] != "-" {
				jsonName = parts[0]
			}
		}
		metas = append(metas, fieldMeta{
			JSONName: jsonName,
			LogTag:   logTag,
			Index:    i,
			Type:     field.Type,
		})
	}
	fieldCache.Store(typ, metas)
	return metas
}

// StructToJSONString 将结构体转换为 JSON 格式的字符串，
// 直接写入 strings.Builder，无中间 map 分配。
func StructToJSONString(v any, level slog.Level) string {
	val := unwrapValue(reflect.ValueOf(v))
	if !val.IsValid() {
		return "null"
	}
	builder := &strings.Builder{}
	writeValue(builder, val, level)
	return builder.String()
}

// writeValue 是递归写入的核心
func writeValue(b *strings.Builder, val reflect.Value, level slog.Level) {
	val = unwrapValue(val)
	if !val.IsValid() {
		b.WriteString("null")
		return
	}

	switch val.Kind() {
	case reflect.Struct:
		writeStruct(b, val, level)

	case reflect.Slice, reflect.Array:
		// 特殊处理 []byte
		if val.Kind() == reflect.Slice && val.Type().Elem().Kind() == reflect.Uint8 {
			b.WriteByte('"')
			b.WriteString(hex.EncodeToString(val.Bytes()))
			b.WriteByte('"')
			return
		}
		writeSlice(b, val, level)

	case reflect.Map:
		writeMap(b, val, level)

	case reflect.String:
		b.WriteByte('"')
		escapeString(b, val.String())
		b.WriteByte('"')

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		b.WriteString(strconv.FormatInt(val.Int(), 10))

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		b.WriteString(strconv.FormatUint(val.Uint(), 10))

	case reflect.Float32, reflect.Float64:
		b.WriteString(strconv.FormatFloat(val.Float(), 'f', -1, 64))

	case reflect.Bool:
		b.WriteString(strconv.FormatBool(val.Bool()))

	default:
		// 其他类型 fallback（如 complex, chan, func 等）
		b.WriteString(`"<unsupported>"`)
	}
}

// writeStruct 写入结构体字段，处理 log 标签
func writeStruct(b *strings.Builder, val reflect.Value, level slog.Level) {
	typ := val.Type()
	metas := getFieldMetas(typ) // 复用之前的缓存

	b.WriteByte('{')
	needComma := false

	for _, m := range metas {
		fieldVal := val.Field(m.Index)
		if !fieldVal.CanInterface() {
			continue
		}

		// 根据标签决定是否跳过
		tag := m.LogTag
		if tag == "-" {
			continue
		}

		// 处理 omitempty
		if strings.Contains(tag, "omitempty") && fieldVal.IsZero() {
			continue
		}

		// 处理 debug 级别
		if tag == "debug" && level < slog.LevelDebug {
			continue
		}

		// 处理 mask（脱敏）
		if tag == "mask" {
			if needComma {
				b.WriteByte(',')
			}
			b.WriteByte('"')
			b.WriteString(m.JSONName)
			b.WriteString(`":"***"`)
			needComma = true
			continue
		}

		// 正常输出字段
		if needComma {
			b.WriteByte(',')
		}
		// 写入 key
		b.WriteByte('"')
		b.WriteString(m.JSONName)
		b.WriteString(`":`)

		// 递归写入 value
		writeValue(b, fieldVal, level)
		needComma = true
	}

	b.WriteByte('}')
}

// writeSlice 写入切片/数组
func writeSlice(b *strings.Builder, val reflect.Value, level slog.Level) {
	length := val.Len()
	// 可选：限制大小防止日志爆炸
	maxLen := 1000
	if length > maxLen {
		b.WriteString(`{"len":`)
		b.WriteString(strconv.Itoa(length))
		b.WriteString(`,"truncated":true}`)
		return
	}

	b.WriteByte('[')
	for i := 0; i < length; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		writeValue(b, val.Index(i), level)
	}
	b.WriteByte(']')
}

// writeMap 写入 map（key 会被转为字符串）
func writeMap(b *strings.Builder, val reflect.Value, level slog.Level) {
	b.WriteByte('{')
	iter := val.MapRange()
	needComma := false
	for iter.Next() {
		k := iter.Key()
		v := iter.Value()
		// map key 只支持基础类型，转为字符串
		keyStr := ""
		if k.Kind() == reflect.String {
			keyStr = k.String()
		} else {
			keyStr = formatBasicValue(k)
		}
		if needComma {
			b.WriteByte(',')
		}
		b.WriteByte('"')
		escapeString(b, keyStr)
		b.WriteString(`":`)
		writeValue(b, v, level)
		needComma = true
	}
	b.WriteByte('}')
}

// escapeString 简单的字符串转义（只转义必要的控制字符）
func escapeString(b *strings.Builder, s string) {
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch c {
		case '"':
			b.WriteString(`\"`)
		case '\\':
			b.WriteString(`\\`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		default:
			if c < 0x20 {
				b.WriteString(`\u00`)
				b.WriteByte(hexChar(c >> 4))
				b.WriteByte(hexChar(c & 0xF))
			} else {
				b.WriteByte(c)
			}
		}
	}
}

func hexChar(b byte) byte {
	if b < 10 {
		return '0' + b
	}
	return 'a' + b - 10
}

// formatBasicValue 用于将 map key 格式化为字符串
func formatBasicValue(val reflect.Value) string {
	switch val.Kind() {
	case reflect.String:
		return val.String()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(val.Int(), 10)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.FormatUint(val.Uint(), 10)
	case reflect.Float32, reflect.Float64:
		return strconv.FormatFloat(val.Float(), 'f', -1, 64)
	case reflect.Bool:
		return strconv.FormatBool(val.Bool())
	default:
		return "<key>"
	}
}

// unwrapValue 解包指针和接口（零拷贝）
func unwrapValue(val reflect.Value) reflect.Value {
	if !val.IsValid() {
		return val
	}
	for val.Kind() == reflect.Interface || val.Kind() == reflect.Ptr {
		if val.IsNil() {
			return reflect.Value{}
		}
		val = val.Elem()
	}
	return val
}
