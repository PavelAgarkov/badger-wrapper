package badger_sdk

import (
	"fmt"
	"net/url"
	"strings"
)

func Uint64ToFixedWidthBytes(n uint64, width int) []byte {
	return []byte(fmt.Sprintf("%0*d", width, n))
}

func normalize(s string) string {
	return strings.ToLower(strings.TrimSpace(s))
}

func esc(s string) string {
	return url.QueryEscape(s)
}

func escBytes(b []byte) string {
	return url.QueryEscape(string(b))
}

// BuildPKKey — теперь принимает id как []byte (готовый сегмент ключа: zero-padded число, UUID, ULID и т.п.).
// ВНИМАНИЕ: id НЕ нормализуем, только экранируем. Считаем, что вызывающий сам выбрал формат и регистр.
func BuildPKKey(dataBase, version, table string, id []byte) []byte {
	return []byte(
		"d:" + esc(normalize(dataBase)) +
			":" + esc(normalize(version)) +
			":" + esc(normalize(table)) +
			":i=" + escBytes(id),
	)
}

func BuildPKPrefix(db, ver, table string) []byte {
	return []byte(
		"d:" + esc(normalize(db)) +
			":" + esc(normalize(ver)) +
			":" + esc(normalize(table)) +
			":i=",
	)
}

type IndexPart struct {
	Field string
	Value string
}

// BuildCompositeIndexKey — id теперь []byte.
func BuildCompositeIndexKey(dataBase, version, table string, parts []IndexPart, id []byte) []byte {
	var b strings.Builder
	b.Grow(96)
	b.WriteString("x:")
	b.WriteString(esc(normalize(dataBase)))
	b.WriteByte(':')
	b.WriteString(esc(normalize(version)))
	b.WriteByte(':')
	b.WriteString(esc(normalize(table)))
	b.WriteByte(':')

	for i, p := range parts {
		if i > 0 {
			b.WriteByte(':')
		}
		b.WriteString(esc(normalize(p.Field)))
		b.WriteString("=")
		b.WriteString(esc(normalize(p.Value)))
	}
	b.WriteString("#i=")
	b.WriteString(escBytes(id))
	return []byte(b.String())
}

func BuildCompositeIndexPrefix(dataBase, version, table string, parts []IndexPart) []byte {
	var b strings.Builder
	b.Grow(96)
	b.WriteString("x:")
	b.WriteString(esc(normalize(dataBase)))
	b.WriteByte(':')
	b.WriteString(esc(normalize(version)))
	b.WriteByte(':')
	b.WriteString(esc(normalize(table)))
	b.WriteByte(':')

	for i, p := range parts {
		if i > 0 {
			b.WriteByte(':') // между частями — двоеточие
		}
		b.WriteString(esc(normalize(p.Field)))
		b.WriteString("=")
		b.WriteString(esc(normalize(p.Value)))
	}
	// префикс оканчивается на "...<fieldN>=<valueN>"
	return []byte(b.String())
}

func BuildCompositeUniqueIndexKey(dataBase, version, table string, parts []IndexPart) []byte {
	var b strings.Builder
	b.Grow(96)
	b.WriteString("u:")
	b.WriteString(esc(normalize(dataBase)))
	b.WriteByte(':')
	b.WriteString(esc(normalize(version)))
	b.WriteByte(':')
	b.WriteString(esc(normalize(table)))
	b.WriteByte(':')

	for i, p := range parts {
		if i > 0 {
			b.WriteByte(':')
		}
		b.WriteString(esc(normalize(p.Field)))
		b.WriteString("=")
		b.WriteString(esc(normalize(p.Value)))
	}
	return []byte(b.String())
}
