package netbadb

import "context"

// Tx is a table-scoped explicit transaction on its client's single session.
type Tx struct {
	client   *Client
	terminal bool
}

// Query starts a streaming query in this transaction.
func (tx *Tx) Query(ctx context.Context, sql string) (*Rows, error) {
	if err := tx.ready(); err != nil {
		return nil, err
	}
	return tx.client.query(ctx, sql, tx)
}

// Exec executes affected-row SQL in this transaction.
func (tx *Tx) Exec(ctx context.Context, sql string) (uint64, error) {
	if err := tx.ready(); err != nil {
		return 0, err
	}
	return tx.client.exec(ctx, sql, tx)
}

// Commit durably commits this transaction when the server confirms success.
func (tx *Tx) Commit(ctx context.Context) error {
	return tx.finish(ctx, kindClientCommit, kindServerTransactionCommitted)
}

// Rollback synchronously requests and confirms transaction rollback.
func (tx *Tx) Rollback(ctx context.Context) error {
	return tx.finish(ctx, kindClientRollback, kindServerTransactionRolledBack)
}

func (tx *Tx) finish(ctx context.Context, requestKind, responseKind uint16) error {
	if err := tx.ready(); err != nil {
		return err
	}
	message, err := tx.client.txRoundTrip(ctx, tx, clientMessage{kind: requestKind})
	if err != nil {
		return err
	}
	if message.kind == kindServerError {
		tx.client.applyRemoteState(tx, message.remoteError.TransactionState)
		return message.remoteError
	}
	if message.kind != responseKind {
		return tx.client.protocolFailure("transaction response has the wrong message kind")
	}
	tx.terminal = true
	if tx.client.tx == tx {
		tx.client.tx = nil
	}
	return nil
}

func (tx *Tx) ready() error {
	if tx == nil || tx.client == nil || tx.terminal || tx.client.tx != tx {
		return ErrTransactionDone
	}
	if tx.client.rows != nil {
		return ErrRowsOpen
	}
	return nil
}

func (c *Client) txRoundTrip(ctx context.Context, tx *Tx, request clientMessage) (serverMessage, error) {
	if err := c.checkReady(tx); err != nil {
		return serverMessage{}, err
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
	if err := c.write(requestID, request); err != nil {
		return serverMessage{}, err
	}
	return c.read(requestID)
}
