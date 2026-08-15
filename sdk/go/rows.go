package netbadb

// Rows streams QueryRow frames. Values returns a copy of the current row.
type Rows struct {
	client    *Client
	tx        *Tx
	requestID uint64
	columns   []ResultColumn
	values    []Value
	rowCount  uint64
	err       error
	done      bool
	finishIO  func()
}

func (r *Rows) Columns() []ResultColumn {
	return append([]ResultColumn(nil), r.columns...)
}

// Next advances to the next validated QueryRow frame.
func (r *Rows) Next() bool {
	if r == nil || r.done || r.err != nil {
		return false
	}
	message, err := r.client.read(r.requestID)
	if err != nil {
		r.fail(err)
		return false
	}
	switch message.kind {
	case kindServerQueryRow:
		if len(message.values) != len(r.columns) {
			r.fail(r.client.protocolFailure("QueryRow value count does not match QueryStart columns"))
			return false
		}
		for i, value := range message.values {
			physical, nonNull := value.physicalType()
			if !nonNull {
				if !value.IsNull() {
					r.fail(r.client.protocolFailure("QueryRow contains an invalid value kind"))
					return false
				}
				if !r.columns[i].Nullable {
					r.fail(r.client.protocolFailure("QueryRow contains NULL for a non-nullable column"))
					return false
				}
			} else if physical != r.columns[i].Type.Physical {
				r.fail(r.client.protocolFailure("QueryRow value physical type does not match column"))
				return false
			}
		}
		r.values = message.values
		r.rowCount++
		return true
	case kindServerQueryEnd:
		if message.count != r.rowCount {
			r.fail(r.client.protocolFailure("QueryEnd row count does not match streamed rows"))
			return false
		}
		r.release()
		return false
	default:
		r.fail(r.client.protocolFailure("query stream contains an unexpected response"))
		return false
	}
}

// Values returns a copy of the current row's explicitly tagged values.
func (r *Rows) Values() []Value {
	return append([]Value(nil), r.values...)
}

// Err reports the first stream, protocol, or I/O error.
func (r *Rows) Err() error {
	if r == nil {
		return nil
	}
	return r.err
}

// Close drains the remaining query response so the client can be reused. A
// drain failure closes the client connection.
func (r *Rows) Close() error {
	if r == nil || r.done {
		return r.Err()
	}
	for r.Next() {
	}
	return r.err
}

func (r *Rows) fail(err error) {
	r.err = err
	r.release()
}

func (r *Rows) release() {
	if r.done {
		return
	}
	r.done = true
	if r.client.rows == r {
		r.client.rows = nil
	}
	if r.finishIO != nil {
		r.finishIO()
		r.finishIO = nil
	}
}
