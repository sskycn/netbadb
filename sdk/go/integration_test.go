//go:build integration

package netbadb

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"testing"
	"time"
)

type fixtureReady struct {
	Address                   string  `json:"address"`
	Transport                 string  `json:"transport"`
	TableID                   TableID `json:"table_id"`
	Fingerprint               string  `json:"fingerprint"`
	ClientCA                  string  `json:"client_ca"`
	ClientCertificate         string  `json:"client_certificate"`
	ClientPrivateKey          string  `json:"client_private_key"`
	UnlistedClientCertificate string  `json:"unlisted_client_certificate"`
	UnlistedClientPrivateKey  string  `json:"unlisted_client_private_key"`
}

type runningFixture struct {
	ready fixtureReady
	cmd   *exec.Cmd
	stdin io.WriteCloser
}

func startFixture(t *testing.T, transport string) *runningFixture {
	t.Helper()
	binary := os.Getenv("NETBADB_GO_FIXTURE_BIN")
	if binary == "" {
		t.Fatal("NETBADB_GO_FIXTURE_BIN is required")
	}
	cmd := exec.Command(binary, transport)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	scanner := bufio.NewScanner(stdout)
	if !scanner.Scan() {
		_ = stdin.Close()
		_ = cmd.Wait()
		t.Fatalf("fixture did not become ready: %v", scanner.Err())
	}
	var ready fixtureReady
	if err := json.Unmarshal(scanner.Bytes(), &ready); err != nil {
		_ = stdin.Close()
		_ = cmd.Wait()
		t.Fatal(err)
	}
	return &runningFixture{ready: ready, cmd: cmd, stdin: stdin}
}

func (fixture *runningFixture) stop(t *testing.T) {
	t.Helper()
	if err := fixture.stdin.Close(); err != nil {
		t.Error(err)
	}
	if err := fixture.cmd.Wait(); err != nil {
		t.Error(err)
	}
}

func fixtureFingerprint(t *testing.T, value string) SchemaFingerprint {
	t.Helper()
	bytes, err := hex.DecodeString(value)
	if err != nil {
		t.Fatal(err)
	}
	if len(bytes) != 32 {
		t.Fatalf("fingerprint has %d bytes", len(bytes))
	}
	var fingerprint SchemaFingerprint
	copy(fingerprint[:], bytes)
	return fingerprint
}

func fixtureTLS(t *testing.T, ready fixtureReady, listed bool) *tls.Config {
	t.Helper()
	ca, err := os.ReadFile(ready.ClientCA)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		t.Fatal("fixture CA PEM is invalid")
	}
	certificatePath, keyPath := ready.ClientCertificate, ready.ClientPrivateKey
	if !listed {
		certificatePath, keyPath = ready.UnlistedClientCertificate, ready.UnlistedClientPrivateKey
	}
	certificate, err := tls.LoadX509KeyPair(certificatePath, keyPath)
	if err != nil {
		t.Fatal(err)
	}
	return &tls.Config{RootCAs: roots, Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12}
}

func fixtureConfig(t *testing.T, ready fixtureReady, tlsConfig *tls.Config) Config {
	return Config{Address: ready.Address, TLS: tlsConfig, RequiredCapabilities: CapabilityExplicitTransactions | CapabilityAnalyze | CapabilityStreamedQueryResults, RequiredSchemas: []TableIdentity{{TableID: ready.TableID, Fingerprint: fixtureFingerprint(t, ready.Fingerprint)}}}
}

func TestRustServerPlaintextIntegration(t *testing.T) {
	fixture := startFixture(t, "plaintext")
	defer fixture.stop(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := Dial(ctx, fixtureConfig(t, fixture.ready, nil))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if err := client.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	if count, err := client.Exec(ctx, "INSERT INTO users (id, name) VALUES (1, 'permanent')"); err != nil || count != 1 {
		t.Fatalf("Exec = %d, %v", count, err)
	}
	assertRows(t, mustQuery(t, client, ctx, "SELECT id, name FROM users ORDER BY id"), []int64{1})

	tx, err := client.Begin(ctx, fixture.ready.TableID)
	if err != nil {
		t.Fatal(err)
	}
	if count, err := tx.Exec(ctx, "INSERT INTO users (id, name) VALUES (2, 'temporary')"); err != nil || count != 1 {
		t.Fatalf("Tx Exec = %d, %v", count, err)
	}
	rows, err := tx.Query(ctx, "SELECT id, name FROM users ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	assertRows(t, rows, []int64{1, 2})
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	assertRows(t, mustQuery(t, client, ctx, "SELECT id, name FROM users ORDER BY id"), []int64{1})
	if err := client.Analyze(ctx, fixture.ready.TableID); err != nil {
		t.Fatal(err)
	}

	wrong := fixtureConfig(t, fixture.ready, nil)
	wrong.RequiredSchemas[0].Fingerprint[0] ^= 1
	_, err = Dial(ctx, wrong)
	var mismatch *SchemaMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("schema mismatch error = %T %v", err, err)
	}
}

func TestRustServerMutualTLSIntegration(t *testing.T) {
	fixture := startFixture(t, "mtls")
	defer fixture.stop(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := Dial(ctx, fixtureConfig(t, fixture.ready, fixtureTLS(t, fixture.ready, true)))
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	assertRows(t, mustQuery(t, client, ctx, "SELECT id, name FROM users ORDER BY id"), nil)
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}

	_, err = Dial(ctx, fixtureConfig(t, fixture.ready, fixtureTLS(t, fixture.ready, false)))
	if err == nil {
		t.Fatal("unlisted CA-signed client passed admission")
	}
}

func mustQuery(t *testing.T, client *Client, ctx context.Context, sql string) *Rows {
	t.Helper()
	rows, err := client.Query(ctx, sql)
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

func assertRows(t *testing.T, rows *Rows, ids []int64) {
	t.Helper()
	columns := rows.Columns()
	if len(columns) != 2 || columns[0].Name != "id" || columns[0].Type != (SemanticType{Physical: PhysicalTypeInt64, Name: "UserId", Named: true}) || columns[0].Nullable || columns[1].Name != "name" || columns[1].Type.Physical != PhysicalTypeText || !columns[1].Nullable {
		t.Fatalf("columns = %#v", columns)
	}
	var actual []int64
	for rows.Next() {
		values := rows.Values()
		id, ok := values[0].Int64()
		if !ok {
			t.Fatalf("id value = %#v", values[0])
		}
		actual = append(actual, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(actual) != fmt.Sprint(ids) {
		t.Fatalf("ids = %v, want %v", actual, ids)
	}
}
