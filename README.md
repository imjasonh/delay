# `delay`

`delay` is a package that attempts to bring the simplicity of
`google.golang.org/appengine/delay` to Cloud Run apps, to make it simpler to
enqueue work to be handled later using Cloud Tasks.

## Usage

First, register your handler function. This must be done at init-time.

```
import "github.com/imjasonh/delay/pkg/delay"

var laterFunc = delay.Func("my-key", myFunc)
```

You can also use a function literal:

```
var laterFunc = delay.Func("my-key", func(ctx context.Context, some, args string) error {
	...
})
```

To call the function, invoke its `Call` method.

```
err := laterFunc.Call(ctx, queueName, delay.WithArgs("arg", "values"))
```

Each time the function is invoked, a Cloud Task will be enqueued which will be
handled by the specified handler function.

# Prerequisites

You must create the Task Queue yourself, which requires creating an App Engine
application. That application and region must match the region where the Cloud
Run service is deployed.
