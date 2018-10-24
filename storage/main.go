package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/doriandekoning/IN4392-cloud-computing-lab/graphs"
	"github.com/doriandekoning/IN4392-cloud-computing-lab/middleware"
	"github.com/doriandekoning/IN4392-cloud-computing-lab/util"
	"github.com/gorilla/mux"
	"github.com/levigross/grequests"
	uuid "github.com/satori/go.uuid"
	"github.com/vrischmann/envconfig"
)

//Config is the main
type Config struct {
	Master struct {
		Port    int
		Address string
	}
	Own struct {
		Port    int
		Address string
	}
}

type Result struct {
	ID        uuid.UUID
	Algorithm string
	Values    []float64
}

var conf Config
var graph graphs.Graph

func main() {
	err := envconfig.Init(&conf)
	if err != nil {
		log.Fatal(err)
	}

	router := mux.NewRouter()
	router.Use(middleware.LoggingMiddleWare)
	router.HandleFunc("/health", GetHealth)
	router.HandleFunc("/storeresult", storeResult).Methods("POST")

	register()
	go checkMasterHealth()
	log.Fatal(http.ListenAndServe(":"+strconv.Itoa(conf.Own.Port), router))
	defer unregister()
}

func GetHealth(w http.ResponseWriter, r *http.Request) {

}

func register() {
	for {
		options := grequests.RequestOptions{
			JSON:    map[string]string{"address": getOwnURL()},
			Headers: map[string]string{"Content-Type": "application/json"},
		}
		resp, err := grequests.Post(getMasterURL()+"/storage/register", &options)
		if err == nil && resp.StatusCode < 300 {
			fmt.Println("Successfully registered")
			break
		}
		fmt.Println("Unable to register, statuscode: ", resp.StatusCode)
		//Try again in 10 sec
		time.Sleep(10 * time.Second)
	}
}

func unregister() {
	options := grequests.RequestOptions{
		JSON:    map[string]string{"address": getOwnURL()},
		Headers: map[string]string{"Content-Type": "application/json"},
	}
	resp, err := grequests.Delete(getMasterURL()+"/storage/unregister", &options)
	if err != nil && resp.StatusCode >= 300 {
		fmt.Println("Unable to register, statuscode:", resp.StatusCode)
		return
	}
	fmt.Println("Sucessfully unregistered")

}

func getMasterURL() string {
	return conf.Master.Address + ":" + strconv.Itoa(conf.Master.Port)
}

func getOwnURL() string {
	return conf.Own.Address + ":" + strconv.Itoa(conf.Own.Port)
}

func checkMasterHealth() {
	for {
		_, err := grequests.Get(getMasterURL()+"/health", nil)
		if err != nil {
			fmt.Println("Master seems to be offline")
			register()
		}
		time.Sleep(10 * time.Second)
	}
}

func storeResult(w http.ResponseWriter, r *http.Request) {
	bodyBytes, _ := ioutil.ReadAll(r.Body)
	result := Result{}
	json.Unmarshal(bodyBytes, &result)
	file, err := os.Create("out/" + result.ID.String())
	if err != nil {
		util.InternalServerError(w, "Error writing to file", err)
		return
	}
	defer file.Close()
	file.WriteString(strings.Trim(strings.Join(strings.Fields(fmt.Sprint(result.Values)), ","), "[]"))

}
