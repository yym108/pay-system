-- 只删表，不删库。golang-migrate 在本文件成功后删除 schema_migrations 版本行。
DROP TABLE IF EXISTS transfer_branch;
DROP TABLE IF EXISTS account_freeze;
DROP TABLE IF EXISTS ledger_entry;
DROP TABLE IF EXISTS ledger_txn;
DROP TABLE IF EXISTS account;
