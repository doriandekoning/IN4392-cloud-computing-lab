package util

import (
	"fmt"
	"net/http"
)

//BadRequest returns a bad request response on the given response writer and prints the provided error
func BadRequest(w http.ResponseWriter, text string, err error) {
	w.WriteHeader(http.StatusBadRequest)
	w.Write([]byte(text))
	fmt.Println(text, ", Bad request with error:", err)
}

//InternalServerError returns a bad request response on the given response writer and prints the provided error
func InternalServerError(w http.ResponseWriter, text string, err error) {
	w.WriteHeader(http.StatusBadRequest)
	w.Write([]byte(text))
	fmt.Println(text, ", Internal server error with error:", err)
}
