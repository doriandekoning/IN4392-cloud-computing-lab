package util

import (
	"encoding/json"
	"fmt"
	"net/http"
)

type GeneralResponseObject struct {
	succes  bool
	message string
}

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

func GeneralResponse(w http.ResponseWriter, succes bool, text string) {
	var response = GeneralResponseObject{succes: succes, message: text}
	js, err := json.Marshal(response)
	if err != nil {
		InternalServerError(w, "Error in parsing JSON response", err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(js)
}
