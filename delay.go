/*
Package delay provides a way to execute code outside the scope of a
user request by using the Cloud Tasks API.

To declare a function that may be executed later, call Func in a top-level
assignment context, passing it a function whose first argument is of type
context.Context.

	var laterFunc = delay.Func(myFunc)

It is also possible to use a function literal.

	var laterFunc = delay.Func(func(ctx context.Context, x string) {
		// ...
	})

To call a function, invoke its Call method.

	err = laterFunc.Call(ctx, req, queueName, "something", 42, 3.14)

To call a function after a delay, invoke its Delay method.

	err = laterFunc.Delay(ctx, req, queueName, time.Minute, "something", 42, 3.14)

A function may be called any number of times. If the function has any
return arguments, and the last one is of type error, the function may
return a non-nil error to signal that the function failed and should be
retried. Other return values are ignored.

The arguments to functions may be of any type that is encodable by the gob
package. If an argument is of interface{} type, it is the client's responsibility
to register with the gob package whatever concrete type may be passed for that
argument; see http://golang.org/pkg/gob/#Register for details.

Any errors during initialization or execution of a function will be
logged to the application logs. Error logs that occur during initialization will
be associated with the request that invoked the Call method.

The state of a function invocation that has not yet successfully
executed is preserved by combining the file name in which it is declared
with the string key that was passed to the Func function. Updating an app
with pending function invocations should safe as long as the relevant
functions have the (filename, key) combination preserved. The filename is
parsed according to these rules:
  - Paths in package main are shortened to just the file name (github.com/foo/foo.go -> foo.go)
  - Paths are stripped to just package paths (/go/src/github.com/foo/bar.go -> github.com/foo/bar.go)
  - Module versions are stripped (/go/pkg/mod/github.com/foo/bar@v0.0.0-20181026220418-f595d03440dc/baz.go -> github.com/foo/bar/baz.go)

The delay package uses the Cloud Tasks API to create tasks that call the
reserved application path "/internal/queue/go/delay".
*/
package delay

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/gob"
	"errors"
	"fmt"
	"net/http"
	"path"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/compute/metadata"
	"github.com/chainguard-dev/clog"
	taskspb "google.golang.org/api/cloudtasks/v2"
	"google.golang.org/api/idtoken"
)

// Function represents a function that may have a delayed invocation.
type Function struct {
	fv  reflect.Value // Kind() == reflect.Func
	key string
	err error // any error during initialization
}

// The HTTP path for invocations.
const handlerPath = "/internal/queue/go/delay"

type contextKey int

var (
	// registry of all delayed functions
	funcs = make(map[string]*Function)

	// precomputed types
	errorType = reflect.TypeOf((*error)(nil)).Elem()

	// errors
	errFirstArg = errors.New("first argument must be context.Context")

	// context keys
	headersContextKey contextKey = 0
	stdContextType               = reflect.TypeOf((*context.Context)(nil)).Elem()
)

func isContext(t reflect.Type) bool { return t == stdContextType }

// Func declares a new Function. The argument must be a function with
// context.Context as its first argument.
//
// This function must be called at program initialization time. That means it
// must be called in a global variable declaration or from an init function.
// This restriction is necessary because the instance that delays a function
// call may not be the one that executes it. Only the code executed at program
// initialization time is guaranteed to have been run by an instance before it
// receives a request.
func Func(i interface{}) *Function {
	f := &Function{fv: reflect.ValueOf(i)}

	// Check that i is a function.
	t := f.fv.Type()
	if t.Kind() != reflect.Func {
		f.err = errors.New("not a function")
		return f
	}

	// Name of the function being passed.
	// For package-level functions, this is the fully-qualified name, e.g., "main.foo".
	// For anonymous functions, this is named like "main.func1".
	key := runtime.FuncForPC(f.fv.Pointer()).Name()

	// Derive a unique, somewhat stable key for this func.
	_, file, _, _ := runtime.Caller(1)
	fk := filepath.Base(file)
	f.key = fk + ":" + key

	if t.NumIn() == 0 || !isContext(t.In(0)) {
		f.err = errFirstArg
		return f
	}

	// Register the function's arguments with the gob package.
	// This is required because they are marshaled inside a []interface{}.
	// gob.Register only expects to be called during initialization;
	// that's fine because this function expects the same.
	for i := 0; i < t.NumIn(); i++ {
		// Only concrete types may be registered. If the argument has
		// interface type, the client is resposible for registering the
		// concrete types it will hold.
		if t.In(i).Kind() == reflect.Interface {
			continue
		}
		gob.Register(reflect.Zero(t.In(i)).Interface())
	}

	if old := funcs[f.key]; old != nil {
		old.err = fmt.Errorf("multiple functions registered for %s in %s", key, file)
	}
	funcs[f.key] = f
	return f
}

type invocation struct {
	Key  string
	Args []interface{}
}

// Call invokes a delayed function immediately.
func (f *Function) Call(ctx context.Context, req *http.Request, queueName string, args ...interface{}) error {
	return f.Delay(ctx, req, queueName, 0, args...)
}

// Call invokes a delayed function after a delay.
func (f *Function) Delay(ctx context.Context, req *http.Request, queueName string, delay time.Duration, args ...interface{}) error {
	t, err := f.task(req.Host, delay, args...)
	if err != nil {
		return fmt.Errorf("new task: %w", err)
	}
	return taskqueueAdder(ctx, t, queueName)
}

// Task creates a Task that will invoke the function.
// Its parameters may be tweaked before adding it to a queue.
// Users should not modify the Path or Payload fields of the returned Task.
func (f *Function) task(host string, delay time.Duration, args ...interface{}) (*taskspb.Task, error) {
	if f.err != nil {
		return nil, fmt.Errorf("delay: func is invalid: %v", f.err)
	}

	nArgs := len(args) + 1 // +1 for the context.Context
	ft := f.fv.Type()
	minArgs := ft.NumIn()
	if ft.IsVariadic() {
		minArgs--
	}
	if nArgs < minArgs {
		return nil, fmt.Errorf("delay: too few arguments to func: %d < %d", nArgs, minArgs)
	}
	if !ft.IsVariadic() && nArgs > minArgs {
		return nil, fmt.Errorf("delay: too many arguments to func: %d > %d", nArgs, minArgs)
	}

	// Check arg types.
	for i := 1; i < nArgs; i++ {
		at := reflect.TypeOf(args[i-1])
		var dt reflect.Type
		if i < minArgs {
			// not a variadic arg
			dt = ft.In(i)
		} else {
			// a variadic arg
			dt = ft.In(minArgs).Elem()
		}
		// nil arguments won't have a type, so they need special handling.
		if at == nil {
			// nil interface
			switch dt.Kind() {
			case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
				continue // may be nil
			}
			return nil, fmt.Errorf("delay: argument %d has wrong type: %v is not nilable", i, dt)
		}
		switch at.Kind() {
		case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
			av := reflect.ValueOf(args[i-1])
			if av.IsNil() {
				// nil value in interface; not supported by gob, so we replace it
				// with a nil interface value
				args[i-1] = nil
			}
		}
		if !at.AssignableTo(dt) {
			return nil, fmt.Errorf("delay: argument %d has wrong type: %v is not assignable to %v", i, at, dt)
		}
	}

	inv := invocation{
		Key:  f.key,
		Args: args,
	}

	buf := new(bytes.Buffer)
	if err := gob.NewEncoder(buf).Encode(inv); err != nil {
		return nil, fmt.Errorf("delay: gob encoding failed: %v", err)
	}

	return &taskspb.Task{
		HttpRequest: &taskspb.HttpRequest{
			Url:  "https://" + path.Join(host, handlerPath),
			Body: base64.StdEncoding.EncodeToString(buf.Bytes()),
			// https://cloud.google.com/run/docs/authenticating/service-to-service#go
			OidcToken: &taskspb.OidcToken{ServiceAccountEmail: email},
		},
		ScheduleTime: time.Now().Add(delay).Format(time.RFC3339Nano),
	}, nil
}

// Request returns the special task-queue HTTP request headers for the current
// task queue handler. Returns an error if called from outside a delay.Func.
func MetaFromContext(ctx context.Context) Meta {
	if ret, ok := ctx.Value(headersContextKey).(Meta); ok {
		return ret
	}
	clog.FromContext(ctx).Error("delay: GetRequestHeaders called outside delay.Func")
	return Meta{}
}

var taskqueueAdder = addTask           // for testing
var validateIDToken = idtoken.Validate // for testing

func addTask(ctx context.Context, t *taskspb.Task, queue string) error {
	clog.FromContext(ctx).With("queue", queue).Info("adding task to queue")

	if queue == "" {
		return errors.New("delay: empty queue name")
	}

	svc, err := taskspb.NewService(ctx)
	if err != nil {
		return fmt.Errorf("creating task service: %w", err)
	}

	parent := fmt.Sprintf("projects/%s/locations/%s/queues/%s", projectID, location, queue)
	if _, err := svc.Projects.Locations.Queues.Tasks.Create(parent, &taskspb.CreateTaskRequest{
		Task: t,
	}).Do(); err != nil {
		return fmt.Errorf("creating task: %w", err)
	}
	return nil
}

var projectID, location, email string

func Init() {
	var err error
	projectID, err = metadata.ProjectID()
	if err != nil {
		clog.Fatalf("error getting project ID: %v", err)
	}
	location, err = metadata.Zone()
	if err != nil {
		clog.Fatalf("error getting region: %v", err)
	}
	location = strings.TrimSuffix(location, "-1")

	email, err = metadata.Email("default")
	if err != nil {
		clog.Fatalf("error getting service account email: %v", err)
	}

	http.HandleFunc(handlerPath, func(w http.ResponseWriter, req *http.Request) {
		runFunc(req.Context(), w, req)
	})
}

// Meta contains the special HTTP request headers available to push task
// HTTP request handlers. These headers are set internally by Cloud Tasks.
//
// See https://cloud.google.com/appengine/docs/standard/go/taskqueue/push/creating-handlers#reading_request_headers
// for a description of the fields.
// https://cloud.google.com/tasks/docs/creating-http-target-tasks#handler
type Meta struct {
	// The name of the queue.
	QueueName string `json:"queueName,omitempty"`

	// The "short" name of the task; a unique system-generated id.
	TaskName string `json:"taskName,omitempty"`

	// The number of times this task has been retried. For the first attempt,
	// this value is 0. This number includes attempts where the task failed due
	// to 5XX error codes and never reached the execution phase.
	TaskRetryCount int64 `json:"taskRetryCount,omitempty"`

	// The total number of times that the task has received a response from the
	// handler. Since Cloud Tasks deletes the task once a successful response
	// has been received, all previous handler responses were failures. This
	// number does not include failures due to 5XX error codes.
	TaskExecutionCount int64 `json:"taskExecutionCount,omitempty"`

	// The schedule time of the task.
	TaskETA *time.Time `json:"taskETA,omitempty"`

	// The HTTP response code from the previous retry.
	TaskPreviousResponse int `json:"taskPreviousResponse,omitempty"`

	// The reason for retrying the task.
	TaskRetryReason string `json:"taskRetryReason,omitempty"`
}

// parseRequestHeaders parses the special HTTP request headers available to push
// task request handlers. This function silently ignores values of the wrong
// format.
func parseRequestHeaders(h http.Header) Meta {
	ret := Meta{
		QueueName: h.Get("X-CloudTasks-QueueName"),
		TaskName:  h.Get("X-CloudTasks-TaskName"),
	}
	if val, _ := strconv.ParseInt(h.Get("X-CloudTasks-TaskRetryCount"), 10, 64); val != 0 {
		ret.TaskRetryCount = val
	}
	if val, _ := strconv.ParseInt(h.Get("X-CloudTasks-TaskExecutionCount"), 10, 64); val != 0 {
		ret.TaskExecutionCount = val
	}
	if etaSecs, _ := strconv.ParseInt(h.Get("X-CloudTasks-TaskETA"), 10, 64); etaSecs != 0 {
		t := time.Unix(etaSecs, 0)
		ret.TaskETA = &t
	}
	if val, _ := strconv.Atoi(h.Get("X-CloudTasks-TaskPreviousResponse")); val != 0 {
		ret.TaskPreviousResponse = val
	}
	if val := h.Get("X-CloudTasks-TaskRetryReason"); val != "" {
		ret.TaskRetryReason = val
	}
	return ret
}

func runFunc(ctx context.Context, w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()

	log := clog.FromContext(ctx)

	// Validate ID token, including the audience, which will be the original
	// request host+handlerPath.
	tok := strings.TrimPrefix(req.Header.Get("Authorization"), "Bearer ")
	aud := "https://" + path.Join(req.Host, handlerPath)
	if _, err := validateIDToken(ctx, tok, aud); err != nil {
		log.Errorf("delay: idtoken Validate: %v", err)
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	meta := parseRequestHeaders(req.Header)
	ctx = context.WithValue(ctx, headersContextKey, meta)
	log = log.With("meta", meta)
	ctx = clog.WithLogger(ctx, log)

	var inv invocation
	if err := gob.NewDecoder(req.Body).Decode(&inv); err != nil {
		log.Errorf("delay: failed decoding task payload: %v", err)
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	f := funcs[inv.Key]
	if f == nil {
		log.Errorf("delay: no func with key %q found", inv.Key)
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	ft := f.fv.Type()
	in := []reflect.Value{reflect.ValueOf(ctx)}
	for _, arg := range inv.Args {
		var v reflect.Value
		if arg != nil {
			v = reflect.ValueOf(arg)
		} else {
			// Task was passed a nil argument, so we must construct
			// the zero value for the argument here.
			n := len(in) // we're constructing the nth argument
			var at reflect.Type
			if !ft.IsVariadic() || n < ft.NumIn()-1 {
				at = ft.In(n)
			} else {
				at = ft.In(ft.NumIn() - 1).Elem()
			}
			v = reflect.Zero(at)
		}
		in = append(in, v)
	}
	out := f.fv.Call(in)

	if n := ft.NumOut(); n > 0 && ft.Out(n-1) == errorType {
		if errv := out[n-1]; !errv.IsNil() {
			log.Warnf("delay: func failed (will retry): %v", errv.Interface())
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}
}
