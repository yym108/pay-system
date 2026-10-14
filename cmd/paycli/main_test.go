package main

import (
	"bytes"
	"testing"
)

func TestPayCLIRunEmpty(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"WORKER_ID=nope"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit code = %d, stderr=%s", code, stderr.String())
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %s", stderr.String())
	}
}
