// A sample app for a Cloud Native Buildpacks image run as a Firecracker rootfs:
// listens on $PORT and answers with what tells the VMs apart.
package main

import (
	"fmt"
	"net/http"
	"os"
	"sync/atomic"
	"time"
)

func main() {
	started := time.Now()
	var served atomic.Int64
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "Hello from a CNB-built Go app in a Firecracker microVM\nserved by this process: %d\nsince start: %s\nrequest: %s %s %s, x-forwarded-for %s\n",
			served.Add(1), time.Since(started).Round(time.Millisecond), r.Method, r.URL, r.Proto, r.Header.Get("X-Forwarded-For"))
	})
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	// http/1.1 and h2c, as the data plane relays both
	var protocols http.Protocols
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)
	server := &http.Server{Addr: ":" + port, Protocols: &protocols}
	if err := server.ListenAndServe(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
