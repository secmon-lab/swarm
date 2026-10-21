package bq_test

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/m-mizutani/gt"
	"github.com/secmon-lab/swarm/pkg/domain/types"
	"github.com/secmon-lab/swarm/pkg/infra/bq"
	"github.com/secmon-lab/swarm/pkg/infra/bq/writer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakeAppender records AppendRows calls. The result of each call is decided by the opener that created it.
type fakeAppender struct {
	opener *fakeOpener
	id     int
	closed bool
}

func (x *fakeAppender) AppendRows(ctx context.Context, rows [][]byte) (writer.AppendResult, error) {
	return x.opener.append(x.id, rows)
}

func (x *fakeAppender) Close() error {
	x.opener.mutex.Lock()
	defer x.opener.mutex.Unlock()
	x.closed = true
	return nil
}

type fakeResult struct {
	opener   *fakeOpener
	call     int
	appender int
}

func (x *fakeResult) Wait(ctx context.Context) error {
	x.opener.mutex.Lock()
	x.opener.waited = true
	resultErr := x.opener.resultErr
	x.opener.mutex.Unlock()

	if resultErr == nil {
		return nil
	}
	return resultErr(x.call, x.appender)
}

type appendCall struct {
	appender int
	rows     int
}

// fakeOpener opens fakeAppender and decides the result of each AppendRows call with appendErr and resultErr. appendErr is returned by AppendRows with the 0-based index of the call. resultErr is returned by Wait of the result with the index of the call and of the appender; it is called without holding the lock of fakeOpener, so it can block.
type fakeOpener struct {
	mutex     sync.Mutex
	appenders []*fakeAppender
	calls     []appendCall
	waited    bool

	appendErr func(call int) error
	resultErr func(call, appender int) error
}

func (x *fakeOpener) open(ctx context.Context) (writer.RowAppender, error) {
	x.mutex.Lock()
	defer x.mutex.Unlock()
	a := &fakeAppender{opener: x, id: len(x.appenders)}
	x.appenders = append(x.appenders, a)
	return a, nil
}

func (x *fakeOpener) append(appender int, rows [][]byte) (writer.AppendResult, error) {
	x.mutex.Lock()
	defer x.mutex.Unlock()
	call := len(x.calls)
	x.calls = append(x.calls, appendCall{appender: appender, rows: len(rows)})
	if x.appendErr != nil {
		if err := x.appendErr(call); err != nil {
			return nil, err
		}
	}
	return &fakeResult{opener: x, call: call, appender: appender}, nil
}

func (x *fakeOpener) appenderCount() int {
	x.mutex.Lock()
	defer x.mutex.Unlock()
	return len(x.appenders)
}

func (x *fakeOpener) appenderClosed(i int) bool {
	x.mutex.Lock()
	defer x.mutex.Unlock()
	return x.appenders[i].closed
}

var testStreamSchema = bigquery.Schema{
	{Name: "id", Type: bigquery.StringFieldType},
}

func testRows(n int) []any {
	data := make([]any, n)
	for i := range data {
		data[i] = map[string]any{"id": "x"}
	}
	return data
}

func newTestStream(t *testing.T, opener *fakeOpener) *bq.Stream {
	return gt.R1(bq.NewStreamWithOpener(context.Background(), testStreamSchema, opener.open)).NoError(t)
}

func TestStreamInsertSendsAllRowsInOneRequest(t *testing.T) {
	opener := &fakeOpener{}
	s := newTestStream(t, opener)

	// More rows than the 256 rows per request of the pending stream path.
	gt.NoError(t, s.Insert(context.Background(), testRows(1000)))

	gt.Equal(t, opener.calls, []appendCall{{appender: 0, rows: 1000}})
	// Insert returns after waiting for the response.
	gt.True(t, opener.waited)
}

func TestStreamInsertSerializedRows(t *testing.T) {
	opener := &fakeOpener{}
	s := newTestStream(t, opener)

	rows := gt.R1(s.SerializeRows(testRows(3))).NoError(t)
	gt.A(t, rows).Length(3)
	gt.NoError(t, s.InsertSerializedRows(context.Background(), rows))

	gt.Equal(t, opener.calls, []appendCall{{appender: 0, rows: 3}})
}

func TestStreamInsertRenewsAndRetriesOnEOF(t *testing.T) {
	testCases := map[string]*fakeOpener{
		"AppendRows returns EOF": {
			appendErr: func(call int) error {
				if call == 0 {
					return io.EOF
				}
				return nil
			},
		},
		"response is EOF": {
			resultErr: func(call, appender int) error {
				if call == 0 {
					return io.EOF
				}
				return nil
			},
		},
	}

	for label, opener := range testCases {
		t.Run(label, func(t *testing.T) {
			s := newTestStream(t, opener)

			gt.NoError(t, s.Insert(context.Background(), testRows(2)))

			// The same rows are sent again through a new managed stream.
			gt.Equal(t, opener.calls, []appendCall{{appender: 0, rows: 2}, {appender: 1, rows: 2}})
			gt.False(t, opener.appenderClosed(1))

			// The failed managed stream is closed in background after its use is released, and Close waits for it.
			gt.NoError(t, s.Close())
			gt.True(t, opener.appenderClosed(0))
			gt.True(t, opener.appenderClosed(1))
		})
	}
}

func TestStreamInsertRenewsOnceForConcurrentFailures(t *testing.T) {
	ctx := context.Background()

	// Call 0 waits for its response until releaseFirst is closed. Two other calls on the same managed stream fail with EOF after both of them have been sent.
	firstWaiting := make(chan struct{})
	releaseFirst := make(chan struct{})
	bothSent := make(chan struct{})
	var mutex sync.Mutex
	failing := 0
	opener := &fakeOpener{
		resultErr: func(call, appender int) error {
			if call == 0 {
				close(firstWaiting)
				<-releaseFirst
				return nil
			}
			if appender == 0 {
				mutex.Lock()
				failing++
				if failing == 2 {
					close(bothSent)
				}
				mutex.Unlock()
				<-bothSent
				return io.EOF
			}
			return nil
		},
	}
	s := newTestStream(t, opener)

	firstDone := make(chan error)
	go func() { firstDone <- s.Insert(ctx, testRows(1)) }()
	<-firstWaiting

	// Other inserts are sent and retried while the first insert waits for its response.
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			gt.NoError(t, s.Insert(ctx, testRows(1)))
		}()
	}
	wg.Wait()

	// The two failures on the same managed stream open only one new managed stream.
	gt.Equal(t, opener.appenderCount(), 2)
	// The failed managed stream is not closed while the first insert still uses it.
	gt.False(t, opener.appenderClosed(0))

	close(releaseFirst)
	gt.NoError(t, <-firstDone)
	gt.NoError(t, s.Close())
	gt.True(t, opener.appenderClosed(0))
}

func TestStreamInsertRetriesSchemaMismatch(t *testing.T) {
	opener := &fakeOpener{
		resultErr: func(call, appender int) error {
			if call < bq.MaxAppendFailures+1 {
				return types.ErrSchemaNotMatched
			}
			return nil
		},
	}
	s := newTestStream(t, opener)

	// A schema mismatch is not limited by MaxAppendFailures because it continues while a schema change propagates.
	gt.NoError(t, s.Insert(context.Background(), testRows(1)))
	gt.A(t, opener.calls).Length(bq.MaxAppendFailures + 2)
}

func TestStreamInsertGivesUpAfterMaxFailures(t *testing.T) {
	opener := &fakeOpener{
		appendErr: func(call int) error { return io.EOF },
	}
	s := newTestStream(t, opener)

	err := s.Insert(context.Background(), testRows(1))
	gt.Error(t, err)
	gt.True(t, errors.Is(err, io.EOF))
	gt.A(t, opener.calls).Length(bq.MaxAppendFailures)
}

func TestStreamInsertDoesNotRetryErrorsOfRequest(t *testing.T) {
	for _, code := range []codes.Code{codes.InvalidArgument, codes.NotFound, codes.PermissionDenied, codes.Unauthenticated} {
		t.Run(code.String(), func(t *testing.T) {
			opener := &fakeOpener{
				resultErr: func(call, appender int) error { return status.Error(code, "rejected") },
			}
			s := newTestStream(t, opener)

			err := s.Insert(context.Background(), testRows(1))
			gt.Equal(t, status.Code(err), code)
			gt.A(t, opener.calls).Length(1)
			gt.A(t, opener.appenders).Length(1)
		})
	}
}

func TestStreamInsertRetriesUnavailable(t *testing.T) {
	opener := &fakeOpener{
		resultErr: func(call, appender int) error {
			if call == 0 {
				return status.Error(codes.Unavailable, "unavailable")
			}
			return nil
		},
	}
	s := newTestStream(t, opener)

	gt.NoError(t, s.Insert(context.Background(), testRows(1)))
	gt.A(t, opener.calls).Length(2)
}
