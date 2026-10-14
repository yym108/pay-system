package config

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
)

const (
	reconUser        = "svc_recon"
	channelMockUser  = "svc_channel_mock"
	dbPayRecon       = "pay_recon"
	dbPayPayment     = "pay_payment"
	dbPayAccount0    = "pay_account_0"
	dbPayAccount1    = "pay_account_1"
	dbPayChannelMock = "pay_channel_mock"
	channelMockQuery = "parseTime=true&loc=Asia%2FShanghai&charset=utf8mb4"
)

// mysqlDSN 只保存文本片段。这里不调用驱动的 ParseDSN，因此不会 LoadLocation，也不连接数据库。
type mysqlDSN struct {
	Addr   string
	DBName string
	Query  string
}

func parseMySQLDSN(dsn string) (mysqlDSN, error) {
	slash := strings.LastIndex(dsn, "/")
	if slash < 0 {
		return mysqlDSN{}, errors.New("missing database name")
	}
	left := dsn[:slash]
	right := dsn[slash+1:]
	dbEsc, query, _ := strings.Cut(right, "?")
	dbName, err := url.PathUnescape(dbEsc)
	if err != nil || dbName == "" {
		return mysqlDSN{}, errors.New("missing database name")
	}
	netaddr := left
	if at := strings.LastIndex(left, "@"); at >= 0 {
		netaddr = left[at+1:]
	}
	addr, err := tcpAddr(netaddr)
	if err != nil {
		return mysqlDSN{}, err
	}
	return mysqlDSN{Addr: addr, DBName: dbName, Query: query}, nil
}

func tcpAddr(netaddr string) (string, error) {
	open := strings.IndexByte(netaddr, '(')
	if open < 0 || !strings.HasSuffix(netaddr, ")") || open >= len(netaddr)-1 {
		return "", errors.New("address must be tcp(host:port)")
	}
	if netaddr[:open] != "tcp" {
		return "", errors.New("network must be tcp")
	}
	addr := netaddr[open+1 : len(netaddr)-1]
	if _, _, err := net.SplitHostPort(addr); err != nil {
		return "", errors.New("address must be host:port")
	}
	return addr, nil
}

func formatMySQLDSN(user, password, addr, db, query string) string {
	var b strings.Builder
	b.WriteString(user)
	b.WriteByte(':')
	b.WriteString(password)
	b.WriteString("@tcp(")
	b.WriteString(addr)
	b.WriteString(")/")
	b.WriteString(url.PathEscape(db))
	if query != "" {
		b.WriteByte('?')
		b.WriteString(query)
	}
	return b.String()
}

func assembleRecon(password, dsn0, dsn1 string) (payRecon, payPayment, account0, account1 string, err error) {
	left, err := parseMySQLDSN(dsn0)
	if err != nil {
		return "", "", "", "", fmt.Errorf("invalid ACCOUNT_DB_DSN_0: %w", err)
	}
	right, err := parseMySQLDSN(dsn1)
	if err != nil {
		return "", "", "", "", fmt.Errorf("invalid ACCOUNT_DB_DSN_1: %w", err)
	}
	if left.DBName != dbPayAccount0 {
		return "", "", "", "", invalid("ACCOUNT_DB_DSN_0", "database must be "+dbPayAccount0)
	}
	if right.DBName != dbPayAccount1 {
		return "", "", "", "", invalid("ACCOUNT_DB_DSN_1", "database must be "+dbPayAccount1)
	}
	// host、port、query 都取自 ACCOUNT_DB_DSN_0。分片库名保持原来的 pay_account_0、pay_account_1。
	payRecon = formatMySQLDSN(reconUser, password, left.Addr, dbPayRecon, left.Query)
	payPayment = formatMySQLDSN(reconUser, password, left.Addr, dbPayPayment, left.Query)
	account0 = formatMySQLDSN(reconUser, password, left.Addr, left.DBName, left.Query)
	account1 = formatMySQLDSN(reconUser, password, left.Addr, right.DBName, left.Query)
	return payRecon, payPayment, account0, account1, nil
}

func assembleChannelMock(host string, port int, password string) (string, error) {
	if err := checkHost("MYSQL_HOST", host); err != nil {
		return "", err
	}
	if err := checkPort("MYSQL_PORT", port); err != nil {
		return "", err
	}
	addr := net.JoinHostPort(host, fmt.Sprintf("%d", port))
	return formatMySQLDSN(channelMockUser, password, addr, dbPayChannelMock, channelMockQuery), nil
}
