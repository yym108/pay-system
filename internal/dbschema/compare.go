package dbschema

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// DiffAccountShards 比较两个账户分片的表、列、索引和 CHECK。
// 不比较 AUTO_INCREMENT 当前值：种子写入后两个分片行数不同，那不是结构差异。
// 返回空字符串表示一致。
func DiffAccountShards(ctx context.Context, db *sql.DB) (string, error) {
	left, err := snapshot(ctx, db, "pay_account_0")
	if err != nil {
		return "", err
	}
	right, err := snapshot(ctx, db, "pay_account_1")
	if err != nil {
		return "", err
	}
	if left == right {
		return "", nil
	}
	return firstDifference(left, right), nil
}

func snapshot(ctx context.Context, db *sql.DB, schema string) (string, error) {
	queries := []string{
		`SELECT TABLE_NAME, TABLE_TYPE, TABLE_COLLATION, IFNULL(TABLE_COMMENT, '')
		   FROM information_schema.TABLES
		  WHERE TABLE_SCHEMA = ?
		  ORDER BY TABLE_NAME`,
		`SELECT TABLE_NAME, COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, IFNULL(COLUMN_DEFAULT, '<null>'),
		        EXTRA, COLUMN_KEY, IFNULL(COLUMN_COMMENT, ''), ORDINAL_POSITION
		   FROM information_schema.COLUMNS
		  WHERE TABLE_SCHEMA = ?
		  ORDER BY TABLE_NAME, ORDINAL_POSITION`,
		`SELECT TABLE_NAME, INDEX_NAME, NON_UNIQUE, SEQ_IN_INDEX, COLUMN_NAME, INDEX_TYPE, NULLABLE
		   FROM information_schema.STATISTICS
		  WHERE TABLE_SCHEMA = ?
		  ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX`,
		`SELECT CONSTRAINT_NAME, CHECK_CLAUSE
		   FROM information_schema.CHECK_CONSTRAINTS
		  WHERE CONSTRAINT_SCHEMA = ?
		  ORDER BY CONSTRAINT_NAME`,
	}
	var b strings.Builder
	for _, query := range queries {
		rows, err := db.QueryContext(ctx, query, schema)
		if err != nil {
			return "", fmt.Errorf("dbschema: query %s: %w", schema, err)
		}
		text, err := dump(rows)
		if err != nil {
			return "", err
		}
		b.WriteString(text)
		b.WriteString("\n")
	}
	return b.String(), nil
}

func dump(rows *sql.Rows) (string, error) {
	defer func() { _ = rows.Close() }()
	cols, err := rows.Columns()
	if err != nil {
		return "", fmt.Errorf("dbschema: columns: %w", err)
	}
	var b strings.Builder
	raw := make([]sql.RawBytes, len(cols))
	dest := make([]any, len(cols))
	for i := range raw {
		dest[i] = &raw[i]
	}
	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			return "", fmt.Errorf("dbschema: scan: %w", err)
		}
		for i, cell := range raw {
			if i > 0 {
				b.WriteByte('\t')
			}
			b.Write(cell)
		}
		b.WriteByte('\n')
	}
	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("dbschema: rows: %w", err)
	}
	return b.String(), nil
}

func firstDifference(left, right string) string {
	ll := strings.Split(left, "\n")
	rl := strings.Split(right, "\n")
	n := len(ll)
	if len(rl) < n {
		n = len(rl)
	}
	for i := 0; i < n; i++ {
		if ll[i] != rl[i] {
			return fmt.Sprintf("line %d\nshard0: %s\nshard1: %s", i+1, ll[i], rl[i])
		}
	}
	return fmt.Sprintf("shard0 lines %d, shard1 lines %d", len(ll), len(rl))
}
