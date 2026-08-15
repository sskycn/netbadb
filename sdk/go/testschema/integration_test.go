//go:build integration

package testschema

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"testing"
	"time"

	netbadb "github.com/sskycn/netbadb/sdk/go"
)

type fixtureReady struct {
	Address string `json:"address"`
}

func TestGeneratedBindingsAgainstRustServer(t *testing.T) {
	binary := os.Getenv("NETBADB_GO_FIXTURE_BIN")
	if binary == "" {
		t.Fatal("NETBADB_GO_FIXTURE_BIN is required")
	}
	command := exec.Command(binary, "plaintext")
	stdin, err := command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	command.Stderr = os.Stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := stdin.Close(); err != nil {
			t.Error(err)
		}
		if err := command.Wait(); err != nil {
			t.Error(err)
		}
	})

	line, err := bufio.NewReader(stdout).ReadBytes('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	var ready fixtureReady
	if err := json.Unmarshal(line, &ready); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := Dial(ctx, netbadb.Config{
		Address: ready.Address,
		RequiredCapabilities: netbadb.CapabilityExplicitTransactions |
			netbadb.CapabilityStreamedQueryResults,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if _, err := client.Exec(ctx, "INSERT INTO users (id, name) VALUES (42, NULL)"); err != nil {
		t.Fatal(err)
	}

	rows, err := QueryUsers(ctx, client, "SELECT id, name FROM users ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	if !rows.Next() {
		t.Fatalf("Next = false: %v", rows.Err())
	}
	row := rows.Row()
	if row.Id != UserId(42) || row.Name.Valid {
		t.Fatalf("row = %#v", row)
	}
	if rows.Next() || rows.Err() != nil {
		t.Fatalf("stream end error = %v", rows.Err())
	}

	tx, err := client.Begin(ctx, UsersTableId)
	if err != nil {
		t.Fatal(err)
	}
	txRows, err := QueryUsers(ctx, tx, "SELECT id, name FROM users ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	if !txRows.Next() || txRows.Row().Id != UserId(42) {
		t.Fatalf("transaction row = %#v, error = %v", txRows.Row(), txRows.Err())
	}
	if err := txRows.Close(); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	_, err = QueryUsers(ctx, client, "SELECT name, id FROM users")
	var shape *netbadb.ResultShapeError
	if !errors.As(err, &shape) {
		t.Fatalf("wrong-shape error = %T %v", err, err)
	}
	if err := client.Ping(ctx); err != nil {
		t.Fatalf("Ping after wrong-shape drain: %v", err)
	}
}
