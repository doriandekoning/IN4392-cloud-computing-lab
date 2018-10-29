package middleware

import (
	"log"
	"net/http"

	"github.com/doriandekoning/IN4392-cloud-computing-lab/metriclogger"
)

type LoggingMiddleware struct {
	InstanceId string
}

func (loggingMiddleware *LoggingMiddleware) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health" && r.URL.Path != "health" {
			log.Println("[" + r.RequestURI + "]")
		}

		// Measure all incoming requests for this instance.
		metriclogger.Measurement{loggingMiddleware.InstanceId, metriclogger.NetworkRequest, 1, 0}.Log()

		next.ServeHTTP(w, r)
	})
}

type AuthenticationMiddleware struct {
	ApiKey string
}

func (authMiddleware *AuthenticationMiddleware) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		apikey := r.Header.Get("X-Auth")
		if apikey == authMiddleware.ApiKey {
			next.ServeHTTP(w, r)
		} else {
			http.Error(w, "Forbidden", http.StatusForbidden)
		}
	})
}
