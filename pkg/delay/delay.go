/*
Package delay provides a way to execute code outside the scope of a
user request by using the Cloud Tasks API.

To declare a function that may be executed later, call Func
in a top-level assignment context, passing it an arbitrary string key
and a function whose first argument is of type context.Context.
The key is used to look up the function so it can be called later.
	var laterFunc = delay.Func("key", myFunc)
It is also possible to use a function literal.
	var laterFunc = delay.Func("key", func(ctx context.Context, x string) {
		// ...
	})

To call a function, invoke its Call method.
	err = laterFunc.Call(c, queueName, delay.WithArgs("something"))
A function may be called any number of times. If the function has any
return arguments, and the last one is of type error, the function may
return a non-nil error to signal that the function failed and should be
retried.

The arguments to functions may be of any type that is encodable by the gob
package. If an argument is of interface type, it is the client's responsibility
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
  * Paths in package main are shortened to just the file name (github.com/foo/foo.go -> foo.go)
  * Paths are stripped to just package paths (/go/src/github.com/foo/bar.go -> github.com/foo/bar.go)
  * Module versions are stripped (/go/pkg/mod/github.com/foo/bar@v0.0.0-20181026220418-f595d03440dc/baz.go -> github.com/foo/bar/baz.go)

The delay package uses the Cloud Tasks API to create tasks that call the
reserved application path "/internal/queue/go/delay".
*/
package delay

import (
	"os"
	"path"

	"cloud.google.com/go/compute/metadata"
	taskspb "google.golang.org/api/cloudtasks/v2beta3"

	"bytes"
	"context"
	"encoding/base64"
	"encoding/gob"
	"errors"
	"fmt"
	"log"
	"net/http"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"time"
)

type options struct {
	Args  []interface{}
	Delay time.Duration
}
type Option func(o *options) error

func WithArgs(i ...interface{}) Option {
	return func(o *options) error {
		o.Args = i
		return nil
	}
}

func WithDelay(d time.Duration) Option {
	return func(o *options) error {
		o.Delay = d
		return nil
	}
}

// Function represents a function that may have a delayed invocation.
type Function struct {
	fv  reflect.Value // Kind() == reflect.Func
	key string
	err error // any error during initialization
}

const (
	// The HTTP path for invocations.
	// TODO: make this configurable.
	handlerPath = "/internal/queue/go/delay"
)

type contextKey int

var (
	hostname string // TODO get this from somewhere

	// registry of all delayed functions
	funcs = make(map[string]*Function)

	// precomputed types
	errorType = reflect.TypeOf((*error)(nil)).Elem()

	// errors
	errFirstArg         = errors.New("first argument must be context.Context")
	errOutsideDelayFunc = errors.New("request headers are only available inside a delay.Func")

	// context keys
	headersContextKey contextKey = 0
	stdContextType               = reflect.TypeOf((*context.Context)(nil)).Elem()
)

func isContext(t reflect.Type) bool { return t == stdContextType }

// Func declares a new Function. The second argument must be a function with a
// first argument of type context.Context.
// This function must be called at program initialization time. That means it
// must be called in a global variable declaration or from an init function.
// This restriction is necessary because the instance that delays a function
// call may not be the one that executes it. Only the code executed at program
// initialization time is guaranteed to have been run by an instance before it
// receives a request.
func Func(key string, i interface{}) *Function {
	f := &Function{fv: reflect.ValueOf(i)}

	// Derive unique, somewhat stable key for this func.
	_, file, _, _ := runtime.Caller(1)
	fk := filepath.Base(file)
	f.key = fk + ":" + key

	t := f.fv.Type()
	if t.Kind() != reflect.Func {
		f.err = errors.New("not a function")
		return f
	}
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
	Key     string
	Options options
}

// Call invokes a delayed function.
func (f *Function) Call(ctx context.Context, queueName string, opts ...Option) error {
	o := &options{}
	for _, op := range opts {
		if err := op(o); err != nil {
			return err
		}
	}

	t, err := f.task(*o)
	if err != nil {
		return err
	}
	return taskqueueAdder(ctx, t, queueName)
}

// Task creates a Task that will invoke the function.
// Its parameters may be tweaked before adding it to a queue.
// Users should not modify the Path or Payload fields of the returned Task.
func (f *Function) task(opts options) (*taskspb.Task, error) {
	if f.err != nil {
		return nil, fmt.Errorf("delay: func is invalid: %v", f.err)
	}

	nArgs := len(opts.Args) + 1 // +1 for the context.Context
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
		at := reflect.TypeOf(opts.Args[i-1])
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
			av := reflect.ValueOf(opts.Args[i-1])
			if av.IsNil() {
				// nil value in interface; not supported by gob, so we replace it
				// with a nil interface value
				opts.Args[i-1] = nil
			}
		}
		if !at.AssignableTo(dt) {
			return nil, fmt.Errorf("delay: argument %d has wrong type: %v is not assignable to %v", i, at, dt)
		}
	}

	inv := invocation{
		Key: f.key,
		Options: options{
			Args: opts.Args,
		},
	}

	buf := new(bytes.Buffer)
	if err := gob.NewEncoder(buf).Encode(inv); err != nil {
		return nil, fmt.Errorf("delay: gob encoding failed: %v", err)
	}

	return &taskspb.Task{
		HttpRequest: &taskspb.HttpRequest{
			Url:  "https://" + path.Join(hostname, handlerPath),
			Body: base64.StdEncoding.EncodeToString(buf.Bytes()),
		},
		ScheduleTime: time.Now().Add(opts.Delay).Format(time.RFC3339Nano),
		// TODO: OIDC ID token: https://cloud.google.com/run/docs/authenticating/service-to-service#go
	}, nil
}

// Request returns the special task-queue HTTP request headers for the current
// task queue handler. Returns an error if called from outside a delay.Func.
func GetRequestHeaders(ctx context.Context) (*RequestHeaders, error) {
	if ret, ok := ctx.Value(headersContextKey).(*RequestHeaders); ok {
		return ret, nil
	}
	return nil, errOutsideDelayFunc
}

var taskqueueAdder = addTask // for testing

func addTask(ctx context.Context, t *taskspb.Task, queue string) error {
	log.Printf("adding task to queue %q", queue)

	svc, err := taskspb.NewService(ctx)
	if err != nil {
		return err
	}
	parent := fmt.Sprintf("projects/%s/locations/%s/queues/%s", projectID, location, queue)
	_, err = svc.Projects.Locations.Queues.Tasks.Create(parent, &taskspb.CreateTaskRequest{
		Task: t,
	}).Do()
	return err
}

var projectID, location string

func Init() {
	hostname = os.Getenv("HOSTNAME")
	if hostname == "" {
		log.Fatal("No HOSTNAME set")
	}

	var err error
	projectID, err = metadata.ProjectID()
	if err != nil {
		log.Fatalf("error getting project ID: %v", err)
	}
	location, err = metadata.Zone()
	if err != nil {
		log.Fatalf("error getting region: %v", err)
	}
	location = strings.TrimSuffix(location, "-1")

	http.HandleFunc(handlerPath, func(w http.ResponseWriter, req *http.Request) {
		runFunc(req.Context(), w, req)
	})
}

// RequestHeaders are the special HTTP request headers available to push task
// HTTP request handlers. These headers are set internally by App Engine.
// See https://cloud.google.com/appengine/docs/standard/go/taskqueue/push/creating-handlers#reading_request_headers
// for a description of the fields.
// https://cloud.google.com/tasks/docs/creating-http-target-tasks#handler
type RequestHeaders struct {
	QueueName            string
	TaskName             string
	TaskRetryCount       int64
	TaskExecutionCount   int64
	TaskETA              time.Time
	TaskPreviousResponse int
	TaskRetryReason      string
}

// parseRequestHeaders parses the special HTTP request headers available to push
// task request handlers. This function silently ignores values of the wrong
// format.
func parseRequestHeaders(h http.Header) *RequestHeaders {
	ret := &RequestHeaders{
		QueueName: h.Get("X-CloudTasks-QueueName"),
		TaskName:  h.Get("X-CloudTasks-TaskName"),
	}

	ret.TaskRetryCount, _ = strconv.ParseInt(h.Get("X-CloudTasks-TaskRetryCount"), 10, 64)
	ret.TaskExecutionCount, _ = strconv.ParseInt(h.Get("X-CloudTasks-TaskExecutionCount"), 10, 64)

	etaSecs, _ := strconv.ParseInt(h.Get("X-CloudTasks-TaskETA"), 10, 64)
	if etaSecs != 0 {
		ret.TaskETA = time.Unix(etaSecs, 0)
	}

	ret.TaskPreviousResponse, _ = strconv.Atoi(h.Get("X-CloudTasks-TaskPreviousResponse"))
	ret.TaskRetryReason = h.Get("X-CloudTasks-TaskRetryReason")
	return ret
}

func runFunc(ctx context.Context, w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()

	// TODO: validate ID token
	// https://developers.google.com/identity/protocols/oauth2/openid-connect#validatinganidtoken

	ctx = context.WithValue(ctx, headersContextKey, parseRequestHeaders(req.Header))

	var inv invocation
	if err := gob.NewDecoder(req.Body).Decode(&inv); err != nil {
		log.Printf("delay: failed decoding task payload: %v", err)
		log.Printf("delay: dropping task")
		return
	}

	f := funcs[inv.Key]
	if f == nil {
		log.Printf("delay: no func with key %q found", inv.Key)
		log.Printf("delay: dropping task")
		return
	}

	ft := f.fv.Type()
	in := []reflect.Value{reflect.ValueOf(ctx)}
	for _, arg := range inv.Options.Args {
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
			log.Printf("delay: func failed (will retry): %v", errv.Interface())
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}
}
