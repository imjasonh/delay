package delay

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/gob"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	taskspb "google.golang.org/api/cloudtasks/v2beta3"
	"google.golang.org/api/idtoken"
)

const queueName = "queue-name"

var req = &http.Request{Host: "dummy"}

type CustomType struct {
	N int
}

type CustomInterface interface {
	N() int
}

type CustomImpl int

func (c CustomImpl) N() int { return int(c) }

// CustomImpl needs to be registered with gob.
func init() {
	gob.Register(CustomImpl(0))

	validator = fakeValidator{}
}

type fakeValidator struct{}

func (fakeValidator) Validate(context.Context, string, string) (*idtoken.Payload, error) {
	return nil, nil
}

func payload(t *taskspb.Task) []byte {
	if t == nil || t.HttpRequest == nil {
		return nil
	}
	b, err := base64.StdEncoding.DecodeString(t.HttpRequest.Body)
	if err != nil {
		panic(err)
	}
	return b
}

var (
	invalidFunc = Func("invalid", func() {})

	regFuncRuns = 0
	regFuncMsg  = ""
	regFunc     = Func("reg", func(c context.Context, arg string) {
		regFuncRuns++
		regFuncMsg = arg
	})

	custFuncTally = 0
	custFunc      = Func("cust", func(c context.Context, ct *CustomType, ci CustomInterface) {
		a, b := 2, 3
		if ct != nil {
			a = ct.N
		}
		if ci != nil {
			b = ci.N()
		}
		custFuncTally += a + b
	})

	anotherCustFunc = Func("cust2", func(c context.Context, n int, ct *CustomType, ci CustomInterface) {
	})

	varFuncMsg = ""
	varFunc    = Func("variadic", func(c context.Context, format string, args ...int) {
		// convert []int to []interface{} for fmt.Sprintf.
		as := make([]interface{}, len(args))
		for i, a := range args {
			as[i] = a
		}
		varFuncMsg = fmt.Sprintf(format, as...)
	})

	errFuncRuns = 0
	errFuncErr  = errors.New("error!")
	errFunc     = Func("err", func(c context.Context) error {
		errFuncRuns++
		if errFuncRuns == 1 {
			return nil
		}
		return errFuncErr
	})

	dupeWhich = 0
	dupe1Func = Func("dupe", func(c context.Context) {
		if dupeWhich == 0 {
			dupeWhich = 1
		}
	})
	dupe2Func = Func("dupe", func(c context.Context) {
		if dupeWhich == 0 {
			dupeWhich = 2
		}
	})

	reqFuncRuns    = 0
	reqFuncHeaders *RequestHeaders
	reqFuncErr     error
	reqFunc        = Func("req", func(c context.Context) {
		reqFuncRuns++
		reqFuncHeaders, reqFuncErr = GetRequestHeaders(c)
	})

	stdCtxRuns = 0
	stdCtxFunc = Func("context", func(ctx context.Context) {
		stdCtxRuns++
	})
)

type fakeContext struct {
	ctx context.Context
}

func newFakeContext() *fakeContext {
	return &fakeContext{ctx: context.Background()}
}

func TestInvalidFunction(t *testing.T) {
	c := newFakeContext()

	if got, want := invalidFunc.Call(c.ctx, req, queueName), fmt.Errorf("delay: func is invalid: %s", errFirstArg); got.Error() != want.Error() {
		t.Errorf("Incorrect error: got %q, want %q", got, want)
	}
}

func TestVariadicFunctionArguments(t *testing.T) {
	// Check the argument type validation for variadic functions.

	c := newFakeContext()

	calls := 0
	taskqueueAdder = func(c context.Context, t *taskspb.Task, _ string) error {
		calls++
		return nil
	}

	varFunc.Call(c.ctx, req, queueName, WithArgs("hi"))
	varFunc.Call(c.ctx, req, queueName, WithArgs("%d", 12))
	varFunc.Call(c.ctx, req, queueName, WithArgs("%d %d %d", 3, 1, 4))
	if calls != 3 {
		t.Errorf("Got %d calls to taskqueueAdder, want 3", calls)
	}

	if got, want := varFunc.Call(c.ctx, req, queueName, WithArgs("%d %s", 12, "a string is bad")), errors.New("delay: argument 3 has wrong type: string is not assignable to int"); got.Error() != want.Error() {
		t.Errorf("Incorrect error: got %q, want %q", got, want)
	}
}

func TestBadArguments(t *testing.T) {
	// Try running regFunc with different sets of inappropriate arguments.

	c := newFakeContext()

	tests := []struct {
		args    []interface{} // all except context
		wantErr string
	}{
		{
			args:    nil,
			wantErr: "delay: too few arguments to func: 1 < 2",
		},
		{
			args:    []interface{}{"lala", 53},
			wantErr: "delay: too many arguments to func: 3 > 2",
		},
		{
			args:    []interface{}{53},
			wantErr: "delay: argument 1 has wrong type: int is not assignable to string",
		},
	}
	for i, tc := range tests {
		got := regFunc.Call(c.ctx, req, queueName, WithArgs(tc.args...))
		if got.Error() != tc.wantErr {
			t.Errorf("Call %v: got %q, want %q", i, got, tc.wantErr)
		}
	}
}

func TestRunningFunction(t *testing.T) {
	c := newFakeContext()

	// Fake out the adding of a task.
	var task *taskspb.Task
	taskqueueAdder = func(_ context.Context, tk *taskspb.Task, queue string) error {
		if queue != queueName {
			t.Errorf("Got queue %q, expected %q", queue, queueName)
		}
		task = tk
		return nil
	}

	regFuncRuns, regFuncMsg = 0, "" // reset state
	const msg = "Why, hello!"
	regFunc.Call(c.ctx, req, queueName, WithArgs(msg))

	// Simulate the Task Queue service.
	req, err := http.NewRequest("POST", handlerPath, bytes.NewBuffer(payload(task)))
	if err != nil {
		t.Fatalf("Failed making http.Request: %v", err)
	}
	rw := httptest.NewRecorder()
	runFunc(c.ctx, rw, req)

	if regFuncRuns != 1 {
		t.Errorf("regFuncRuns: got %d, want 1", regFuncRuns)
	}
	if regFuncMsg != msg {
		t.Errorf("regFuncMsg: got %q, want %q", regFuncMsg, msg)
	}
}

func TestCustomType(t *testing.T) {
	c := newFakeContext()

	// Fake out the adding of a task.
	var task *taskspb.Task
	taskqueueAdder = func(_ context.Context, tk *taskspb.Task, queue string) error {
		if queue != queueName {
			t.Errorf("Got queue %q, expected %q", queue, queueName)
		}
		task = tk
		return nil
	}

	custFuncTally = 0 // reset state
	custFunc.Call(c.ctx, req, queueName, WithArgs(&CustomType{N: 11}, CustomImpl(13)))

	// Simulate the Task Queue service.
	req, err := http.NewRequest("POST", handlerPath, bytes.NewBuffer(payload(task)))
	if err != nil {
		t.Fatalf("Failed making http.Request: %v", err)
	}
	rw := httptest.NewRecorder()
	runFunc(c.ctx, rw, req)

	if custFuncTally != 24 {
		t.Errorf("custFuncTally = %d, want 24", custFuncTally)
	}

	// Try the same, but with nil values; one is a nil pointer (and thus a non-nil interface value),
	// and the other is a nil interface value.
	custFuncTally = 0 // reset state
	custFunc.Call(c.ctx, req, queueName, WithArgs((*CustomType)(nil), nil))

	// Simulate the Task Queue service.
	req, err = http.NewRequest("POST", handlerPath, bytes.NewBuffer(payload(task)))
	if err != nil {
		t.Fatalf("Failed making http.Request: %v", err)
	}
	rw = httptest.NewRecorder()
	runFunc(c.ctx, rw, req)

	if custFuncTally != 5 {
		t.Errorf("custFuncTally = %d, want 5", custFuncTally)
	}
}

func TestRunningVariadic(t *testing.T) {
	c := newFakeContext()

	// Fake out the adding of a task.
	var task *taskspb.Task
	taskqueueAdder = func(_ context.Context, tk *taskspb.Task, queue string) error {
		if queue != queueName {
			t.Errorf("Got queue %q, expected %q", queue, queueName)
		}
		task = tk
		return nil
	}

	varFuncMsg = "" // reset state
	varFunc.Call(c.ctx, req, queueName, WithArgs("Amiga %d has %d KB RAM", 500, 512))

	// Simulate the Task Queue service.
	req, err := http.NewRequest("POST", handlerPath, bytes.NewBuffer(payload(task)))
	if err != nil {
		t.Fatalf("Failed making http.Request: %v", err)
	}
	rw := httptest.NewRecorder()
	runFunc(c.ctx, rw, req)

	const expected = "Amiga 500 has 512 KB RAM"
	if varFuncMsg != expected {
		t.Errorf("varFuncMsg = %q, want %q", varFuncMsg, expected)
	}
}

func TestErrorFunction(t *testing.T) {
	c := newFakeContext()

	// Fake out the adding of a task.
	var task *taskspb.Task
	taskqueueAdder = func(_ context.Context, tk *taskspb.Task, queue string) error {
		if queue != queueName {
			t.Errorf("Got queue %q, expected %q", queue, queueName)
		}
		task = tk
		return nil
	}

	errFunc.Call(c.ctx, req, queueName)

	// Simulate the Task Queue service.
	// The first call should succeed; the second call should fail.
	{
		req, err := http.NewRequest("POST", handlerPath, bytes.NewBuffer(payload(task)))
		if err != nil {
			t.Fatalf("Failed making http.Request: %v", err)
		}
		rw := httptest.NewRecorder()
		runFunc(c.ctx, rw, req)
	}
	{
		req, err := http.NewRequest("POST", handlerPath, bytes.NewBuffer(payload(task)))
		if err != nil {
			t.Fatalf("Failed making http.Request: %v", err)
		}
		rw := httptest.NewRecorder()
		runFunc(c.ctx, rw, req)
		if rw.Code != http.StatusInternalServerError {
			t.Errorf("Got status code %d, want %d", rw.Code, http.StatusInternalServerError)
		}
	}
}

func TestDuplicateFunction(t *testing.T) {
	c := newFakeContext()

	// Fake out the adding of a task.
	var task *taskspb.Task
	taskqueueAdder = func(_ context.Context, tk *taskspb.Task, queue string) error {
		if queue != queueName {
			t.Errorf("Got queue %q, expected %q", queue, queueName)
		}
		task = tk
		return nil
	}

	if err := dupe1Func.Call(c.ctx, req, queueName); err == nil {
		t.Error("dupe1Func.Call did not return error")
	}
	if task != nil {
		t.Error("dupe1Func.Call posted a task")
	}
	if err := dupe2Func.Call(c.ctx, req, queueName); err != nil {
		t.Errorf("dupe2Func.Call error: %v", err)
	}
	if task == nil {
		t.Fatalf("dupe2Func.Call did not post a task")
	}

	// Simulate the Task Queue service.
	req, err := http.NewRequest("POST", handlerPath, bytes.NewBuffer(payload(task)))
	if err != nil {
		t.Fatalf("Failed making http.Request: %v", err)
	}
	rw := httptest.NewRecorder()
	runFunc(c.ctx, rw, req)

	if dupeWhich == 1 {
		t.Error("dupe2Func.Call used old registered function")
	} else if dupeWhich != 2 {
		t.Errorf("dupeWhich = %d; want 2", dupeWhich)
	}
}

func TestGetRequestHeadersFromContext(t *testing.T) {
	c := newFakeContext()

	// Outside a delay.Func should return an error.
	headers, err := GetRequestHeaders(c.ctx)
	if headers != nil {
		t.Errorf("RequestHeaders outside Func, got %v, want nil", headers)
	}
	if err != errOutsideDelayFunc {
		t.Errorf("RequestHeaders outside Func err, got %v, want %v", err, errOutsideDelayFunc)
	}

	// Fake out the adding of a task.
	var task *taskspb.Task
	taskqueueAdder = func(_ context.Context, tk *taskspb.Task, queue string) error {
		if queue != queueName {
			t.Errorf("Got queue %q, expected %q", queue, queueName)
		}
		task = tk
		return nil
	}

	reqFunc.Call(c.ctx, req, queueName)

	reqFuncRuns, reqFuncHeaders = 0, nil // reset state
	// Simulate the Task Queue service.
	req, err := http.NewRequest("POST", handlerPath, bytes.NewBuffer(payload(task)))
	req.Header.Set("x-cloudtasks-taskname", "foobar")
	if err != nil {
		t.Fatalf("Failed making http.Request: %v", err)
	}
	rw := httptest.NewRecorder()
	runFunc(c.ctx, rw, req)

	if reqFuncRuns != 1 {
		t.Errorf("reqFuncRuns: got %d, want 1", reqFuncRuns)
	}
	if reqFuncHeaders.TaskName != "foobar" {
		t.Errorf("reqFuncHeaders.TaskName: got %v, want 'foobar'", reqFuncHeaders.TaskName)
	}
	if reqFuncErr != nil {
		t.Errorf("reqFuncErr: got %v, want nil", reqFuncErr)
	}
}
