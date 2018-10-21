package middleware

import (
	"log"
	"net/http"
)

func LoggingMiddleWare(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health" && r.URL.Path != "health" {
			log.Println("[" + r.RequestURI + "]")
		}
		next.ServeHTTP(w, r)
	})
}
