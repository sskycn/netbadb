package netbadb

import (
	"errors"
	"fmt"
)

var (
	ErrClientClosed      = errors.New("netbadb: client is closed")
	ErrRowsOpen          = errors.New("netbadb: query rows are still open")
	ErrTransactionActive = errors.New("netbadb: use the active transaction")
	ErrTransactionDone   = errors.New("netbadb: transaction is terminal")
)

type ErrorCode uint16

const (
	ErrorCodeProtocol                         ErrorCode = 1
	ErrorCodeHandshakeRequired                ErrorCode = 2
	ErrorCodeAlreadyHandshaken                ErrorCode = 3
	ErrorCodeTransactionAlreadyActive         ErrorCode = 4
	ErrorCodeNoActiveTransaction              ErrorCode = 5
	ErrorCodeOperationNotAllowedInTransaction ErrorCode = 6
	ErrorCodeCompile                          ErrorCode = 7
	ErrorCodeSchema                           ErrorCode = 8
	ErrorCodeStorage                          ErrorCode = 9
	ErrorCodeExecution                        ErrorCode = 10
	ErrorCodeDatabase                         ErrorCode = 11
	ErrorCodeResponseTooLarge                 ErrorCode = 12
	ErrorCodeInternalResultMismatch           ErrorCode = 13
)

type RemoteError struct {
	Code             ErrorCode
	TransactionState TransactionState
	Message          string
}

func (e *RemoteError) Error() string {
	return fmt.Sprintf("netbadb remote error %d (transaction state %d): %s", e.Code, e.TransactionState, e.Message)
}

// ProtocolError reports an invalid response from an untrusted peer. Such an
// error always poisons the connection.
type ProtocolError struct{ Reason string }

func (e *ProtocolError) Error() string { return "netbadb protocol error: " + e.Reason }

type PlaintextRemoteNotAllowedError struct{ Address string }

func (e *PlaintextRemoteNotAllowedError) Error() string {
	return fmt.Sprintf("netbadb: plaintext peer %s is not loopback", e.Address)
}

type CapabilityMismatchError struct {
	Required uint64
	Actual   uint64
}

func (e *CapabilityMismatchError) Error() string {
	return fmt.Sprintf("netbadb: required capabilities %#x are not available in %#x", e.Required, e.Actual)
}

type SchemaUnavailableError struct{ TableID TableID }

func (e *SchemaUnavailableError) Error() string {
	return fmt.Sprintf("netbadb: required table %d is not visible", e.TableID)
}

type SchemaMismatchError struct {
	TableID  TableID
	Required SchemaFingerprint
	Actual   SchemaFingerprint
}

func (e *SchemaMismatchError) Error() string {
	return fmt.Sprintf("netbadb: schema fingerprint mismatch for table %d", e.TableID)
}

type ExpectedAffectedRowsError struct{}

func (*ExpectedAffectedRowsError) Error() string { return "netbadb: statement returned query rows" }

type ExpectedQueryError struct{}

func (*ExpectedQueryError) Error() string { return "netbadb: statement returned an affected-row count" }

type ClientStateError struct{ Reason string }

func (e *ClientStateError) Error() string { return "netbadb client state error: " + e.Reason }

// ResultShapeError reports a local mismatch between a generated typed result
// contract and the base client's result metadata or scalar values.
type ResultShapeError struct{ Reason string }

func (e *ResultShapeError) Error() string { return "netbadb result shape error: " + e.Reason }
