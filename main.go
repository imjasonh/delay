package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/imjasonh/delay/pkg/delay"
)

func main() {
	delay.Init()

	http.HandleFunc("/start", handler)
	log.Fatal(http.ListenAndServe(":8080", nil))
}

var delayFunc = delay.Func("Delayed", func(ctx context.Context, a, b, c string) error {
	log.Println("Delayed called:", a, b, c)
	if a == b {
		return errors.New("a == b")
	}
	return nil
})

func handler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	if err := delayFunc.Call(ctx, "my-queue",
		delay.WithArgs("a", "b", "c"),
		delay.WithDelay(10*time.Second),
	); err != nil {
		log.Printf("ERROR: %v", err)
		fmt.Fprintln(w, err)
	} else {
		fmt.Fprintln(w, "delay enqueued")
	}
}
