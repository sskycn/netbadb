package netbadb

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"math"
	"net"
	"time"
)

// Config defines one remote session and its handshake requirements.
type Config struct {
	Address              string
	TLS                  *tls.Config
	Dialer               *net.Dialer
	RequiredSchemas      []TableIdentity
	RequiredCapabilities uint64
}

// Client owns one Protocol v1 connection and session. It is not safe for
// concurrent use and Protocol v1 does not multiplex requests.
type Client struct {
	conn          net.Conn
	nextRequestID uint64
	serverInfo    ServerInfo
	rows          *Rows
	tx            *Tx
	closed        bool
	broken        error
}

// Dial connects, optionally completes verified mutual TLS, performs Hello, and
// enforces the required capability and schema-fingerprint gates.
func Dial(ctx context.Context, config Config) (*Client, error) {
	if ctx == nil {
		return nil, &ClientStateError{Reason: "nil context"}
	}
	if config.Address == "" {
		return nil, &ClientStateError{Reason: "empty address"}
	}
	if config.TLS != nil && config.TLS.InsecureSkipVerify {
		return nil, &ClientStateError{Reason: "TLS InsecureSkipVerify is forbidden"}
	}
	dialer := config.Dialer
	if dialer == nil {
		dialer = &net.Dialer{}
	}
	raw, err := dialer.DialContext(ctx, "tcp", config.Address)
	if err != nil {
		return nil, err
	}
	conn := net.Conn(raw)
	if config.TLS == nil {
		if err := validatePlaintextRemote(raw.RemoteAddr()); err != nil {
			_ = raw.Close()
			return nil, err
		}
	} else {
		tlsConfig := config.TLS.Clone()
		if tlsConfig.ServerName == "" {
			host, _, splitErr := net.SplitHostPort(config.Address)
			if splitErr != nil {
				_ = raw.Close()
				return nil, fmt.Errorf("netbadb: derive TLS server name: %w", splitErr)
			}
			tlsConfig.ServerName = host
		}
		tlsConn := tls.Client(raw, tlsConfig)
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			_ = raw.Close()
			return nil, err
		}
		conn = tlsConn
	}

	client := &Client{conn: conn, nextRequestID: 1}
	if err := client.handshake(ctx, config.RequiredCapabilities, config.RequiredSchemas); err != nil {
		client.closeWithError(err)
		return nil, err
	}
	return client, nil
}

func validatePlaintextRemote(address net.Addr) error {
	remote, ok := address.(*net.TCPAddr)
	if ok && remote.IP != nil && remote.IP.IsLoopback() {
		return nil
	}
	return &PlaintextRemoteNotAllowedError{Address: address.String()}
}

func (c *Client) handshake(ctx context.Context, requiredCapabilities uint64, requiredSchemas []TableIdentity) error {
	requestID, err := c.allocateRequestID()
	if err != nil {
		return err
	}
	finish, err := c.beginIO(ctx)
	if err != nil {
		return err
	}
	defer finish()
	if err := c.write(requestID, clientMessage{kind: kindClientHello}); err != nil {
		return err
	}
	message, err := c.read(requestID)
	if err != nil {
		return err
	}
	if message.kind == kindServerError {
		return message.remoteError
	}
	if message.kind != kindServerHelloAck {
		return c.protocolFailure("Hello response is not HelloAck")
	}
	if message.protocolVersion != protocolVersion {
		return c.protocolFailure("HelloAck inner protocol version is not 1")
	}
	if message.maxFramePayload == 0 || message.maxFramePayload > maxFramePayload {
		return c.protocolFailure("HelloAck maximum frame payload is invalid")
	}
	seen := make(map[TableID]SchemaFingerprint, len(message.tables))
	for _, table := range message.tables {
		if _, duplicate := seen[table.TableID]; duplicate {
			return c.protocolFailure("HelloAck contains a duplicate table ID")
		}
		seen[table.TableID] = table.Fingerprint
	}
	if message.capabilities&requiredCapabilities != requiredCapabilities {
		return &CapabilityMismatchError{Required: requiredCapabilities, Actual: message.capabilities}
	}
	for _, required := range requiredSchemas {
		actual, ok := seen[required.TableID]
		if !ok {
			return &SchemaUnavailableError{TableID: required.TableID}
		}
		if actual != required.Fingerprint {
			return &SchemaMismatchError{TableID: required.TableID, Required: required.Fingerprint, Actual: actual}
		}
	}
	c.serverInfo = ServerInfo{
		ProtocolVersion: message.protocolVersion,
		MaxFramePayload: message.maxFramePayload,
		Capabilities:    message.capabilities,
		Tables:          append([]TableIdentity(nil), message.tables...),
	}
	return nil
}

// ServerInfo returns a copy of the negotiated handshake information.
func (c *Client) ServerInfo() ServerInfo {
	info := c.serverInfo
	info.Tables = append([]TableIdentity(nil), info.Tables...)
	return info
}

// Query starts a streaming query. The caller must consume or close Rows before
// issuing another request.
func (c *Client) Query(ctx context.Context, sql string) (*Rows, error) {
	if c.tx != nil {
		return nil, ErrTransactionActive
	}
	return c.query(ctx, sql, nil)
}

// Exec executes SQL expected to return an affected-row count.
func (c *Client) Exec(ctx context.Context, sql string) (uint64, error) {
	if c.tx != nil {
		return 0, ErrTransactionActive
	}
	return c.exec(ctx, sql, nil)
}

// Ping verifies that the established session responds. It is allowed while an
// explicit transaction is active.
func (c *Client) Ping(ctx context.Context) error {
	message, err := c.pingRoundTrip(ctx)
	if err != nil {
		return err
	}
	if message.kind == kindServerError {
		return message.remoteError
	}
	if message.kind != kindServerPong {
		return c.protocolFailure("Ping response is not Pong")
	}
	return nil
}

func (c *Client) pingRoundTrip(ctx context.Context) (serverMessage, error) {
	if c.closed || c.conn == nil {
		return serverMessage{}, ErrClientClosed
	}
	if c.broken != nil {
		return serverMessage{}, c.broken
	}
	if c.rows != nil {
		return serverMessage{}, ErrRowsOpen
	}
	requestID, err := c.allocateRequestID()
	if err != nil {
		return serverMessage{}, err
	}
	finish, err := c.beginIO(ctx)
	if err != nil {
		return serverMessage{}, err
	}
	defer finish()
	if err := c.write(requestID, clientMessage{kind: kindClientPing}); err != nil {
		return serverMessage{}, err
	}
	return c.read(requestID)
}

// Analyze refreshes optimizer statistics for a table.
func (c *Client) Analyze(ctx context.Context, tableID TableID) error {
	if c.tx != nil {
		return ErrTransactionActive
	}
	message, _, err := c.roundTrip(ctx, clientMessage{kind: kindClientAnalyze, tableID: tableID})
	if err != nil {
		return err
	}
	if message.kind == kindServerError {
		return message.remoteError
	}
	if message.kind != kindServerAnalyzeAck {
		return c.protocolFailure("Analyze response is not AnalyzeAck")
	}
	return nil
}

// Begin starts one explicit table-scoped transaction.
func (c *Client) Begin(ctx context.Context, tableID TableID) (*Tx, error) {
	if c.tx != nil {
		return nil, ErrTransactionActive
	}
	message, _, err := c.roundTrip(ctx, clientMessage{kind: kindClientBegin, tableID: tableID})
	if err != nil {
		return nil, err
	}
	if message.kind == kindServerError {
		return nil, message.remoteError
	}
	if message.kind != kindServerTransactionStarted {
		return nil, c.protocolFailure("Begin response is not TransactionStarted")
	}
	tx := &Tx{client: c}
	c.tx = tx
	return tx, nil
}

// Close terminates the session. It does not confirm rollback of an active
// transaction; call Tx.Rollback when that confirmation matters.
func (c *Client) Close() error {
	if c.closed {
		return nil
	}
	c.closed = true
	if c.rows != nil {
		c.rows.err = ErrClientClosed
		c.rows.release()
	}
	if c.tx != nil {
		c.tx.terminal = true
		c.tx = nil
	}
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

func (c *Client) query(ctx context.Context, sql string, tx *Tx) (*Rows, error) {
	if err := c.checkReady(tx); err != nil {
		return nil, err
	}
	requestID, err := c.allocateRequestID()
	if err != nil {
		return nil, err
	}
	finish, err := c.beginIO(ctx)
	if err != nil {
		return nil, err
	}
	if err := c.write(requestID, clientMessage{kind: kindClientExecute, sql: sql}); err != nil {
		finish()
		return nil, err
	}
	message, err := c.read(requestID)
	if err != nil {
		finish()
		return nil, err
	}
	if message.kind == kindServerError {
		finish()
		c.applyRemoteState(tx, message.remoteError.TransactionState)
		return nil, message.remoteError
	}
	if message.kind == kindServerAffectedRows {
		finish()
		return nil, &ExpectedQueryError{}
	}
	if message.kind != kindServerQueryStart {
		finish()
		return nil, c.protocolFailure("Query response is not QueryStart")
	}
	rows := &Rows{client: c, tx: tx, requestID: requestID, columns: message.columns, finishIO: finish}
	c.rows = rows
	return rows, nil
}

func (c *Client) exec(ctx context.Context, sql string, tx *Tx) (uint64, error) {
	if err := c.checkReady(tx); err != nil {
		return 0, err
	}
	requestID, err := c.allocateRequestID()
	if err != nil {
		return 0, err
	}
	finish, err := c.beginIO(ctx)
	if err != nil {
		return 0, err
	}
	if err := c.write(requestID, clientMessage{kind: kindClientExecute, sql: sql}); err != nil {
		finish()
		return 0, err
	}
	message, err := c.read(requestID)
	if err != nil {
		finish()
		return 0, err
	}
	if message.kind == kindServerError {
		finish()
		c.applyRemoteState(tx, message.remoteError.TransactionState)
		return 0, message.remoteError
	}
	if message.kind == kindServerAffectedRows {
		finish()
		return message.count, nil
	}
	if message.kind != kindServerQueryStart {
		finish()
		return 0, c.protocolFailure("Exec response is neither AffectedRows nor QueryStart")
	}
	rows := &Rows{client: c, tx: tx, requestID: requestID, columns: message.columns, finishIO: finish}
	c.rows = rows
	if err := rows.Close(); err != nil {
		return 0, err
	}
	return 0, &ExpectedAffectedRowsError{}
}

func (c *Client) roundTrip(ctx context.Context, request clientMessage) (serverMessage, uint64, error) {
	if err := c.checkReady(nil); err != nil {
		return serverMessage{}, 0, err
	}
	requestID, err := c.allocateRequestID()
	if err != nil {
		return serverMessage{}, 0, err
	}
	finish, err := c.beginIO(ctx)
	if err != nil {
		return serverMessage{}, 0, err
	}
	defer finish()
	if err := c.write(requestID, request); err != nil {
		return serverMessage{}, 0, err
	}
	message, err := c.read(requestID)
	return message, requestID, err
}

func (c *Client) checkReady(tx *Tx) error {
	if c.broken != nil {
		return c.broken
	}
	if c.closed || c.conn == nil {
		return ErrClientClosed
	}
	if c.rows != nil {
		return ErrRowsOpen
	}
	if tx != nil {
		if tx.terminal || c.tx != tx {
			return ErrTransactionDone
		}
	} else if c.tx != nil {
		return ErrTransactionActive
	}
	return nil
}

func (c *Client) allocateRequestID() (uint64, error) {
	if c.nextRequestID == 0 || c.nextRequestID == math.MaxUint64 {
		return 0, &ClientStateError{Reason: "request ID space exhausted"}
	}
	id := c.nextRequestID
	c.nextRequestID++
	return id, nil
}

func (c *Client) beginIO(ctx context.Context) (func(), error) {
	if ctx == nil {
		return nil, &ClientStateError{Reason: "nil context"}
	}
	if err := ctx.Err(); err != nil {
		c.closeWithError(err)
		return nil, err
	}
	if deadline, ok := ctx.Deadline(); ok {
		if err := c.conn.SetDeadline(deadline); err != nil {
			c.closeWithError(err)
			return nil, err
		}
	}
	stop := context.AfterFunc(ctx, func() { _ = c.conn.Close() })
	return func() {
		stop()
		_ = c.conn.SetDeadline(time.Time{})
	}, nil
}

func (c *Client) write(requestID uint64, message clientMessage) error {
	frame, err := encodeClientFrame(requestID, message)
	if err != nil {
		return err
	}
	if c.serverInfo.MaxFramePayload != 0 && uint32(len(frame)-frameHeaderSize) > c.serverInfo.MaxFramePayload {
		return &ProtocolError{Reason: "request exceeds server maximum frame payload"}
	}
	for len(frame) != 0 {
		written, err := c.conn.Write(frame)
		if err != nil {
			c.closeWithError(err)
			return err
		}
		if written == 0 {
			c.closeWithError(io.ErrNoProgress)
			return io.ErrNoProgress
		}
		frame = frame[written:]
	}
	return nil
}

func (c *Client) read(requestID uint64) (serverMessage, error) {
	actual, message, err := readServerFrame(c.conn)
	if err != nil {
		c.closeWithError(err)
		return serverMessage{}, err
	}
	if actual != requestID {
		return serverMessage{}, c.protocolFailure(fmt.Sprintf("response request ID %d does not match %d", actual, requestID))
	}
	return message, nil
}

func (c *Client) protocolFailure(reason string) error {
	err := &ProtocolError{Reason: reason}
	c.closeWithError(err)
	return err
}

func (c *Client) closeWithError(err error) {
	if c.broken == nil {
		c.broken = err
	}
	c.closed = true
	if c.conn != nil {
		_ = c.conn.Close()
	}
	if c.tx != nil {
		c.tx.terminal = true
		c.tx = nil
	}
}

func (c *Client) applyRemoteState(tx *Tx, state TransactionState) {
	if tx != nil && state == TransactionStateNone {
		tx.terminal = true
		if c.tx == tx {
			c.tx = nil
		}
	}
}
