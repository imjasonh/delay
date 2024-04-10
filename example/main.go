package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/chainguard-dev/clog"
	_ "github.com/chainguard-dev/clog/gcp/init"
	"github.com/imjasonh/delay"
)

func main() {
	delay.Init()

	http.HandleFunc("/start", handler)
	if err := http.ListenAndServe(":8080", nil); err != nil {
		clog.Fatalf("ListenAndServe: %v", err)
	}
}

var delayFunc = delay.Func(func(ctx context.Context, a, b, c string) error {
	clog.FromContext(ctx).Infof("Delayed func called: %s %s %s", a, b, c)
	if a == b {
		return errors.New("a == b")
	}
	return nil
})

func handler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	queueName := os.Getenv("QUEUE")

	// call it now
	if err := delayFunc.Call(ctx, r, queueName,
		"right", "now", "!!!1!",
	); err != nil {
		clog.Errorf("call: %v", err)
		fmt.Fprintln(w, err)
	} else {
		fmt.Fprintln(w, "immediate call done")
	}

	// ...and call it in 10 seconds
	if err := delayFunc.Delay(ctx, r, queueName, 10*time.Second,
		"a", "b", "c",
	); err != nil {
		clog.Errorf("delay: %v", err)
		fmt.Fprintln(w, err)
	} else {
		fmt.Fprintln(w, "delay enqueued")
	}
}
