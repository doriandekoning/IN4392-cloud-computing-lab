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
	"github.com/doriandekoning/IN4392-cloud-computing-lab/metriclogger"
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
	ApiKey string
}

type Result struct {
	ID        uuid.UUID
	Algorithm string
	Values    []float64
}

var conf Config
var graph graphs.Graph
var outDir = "/home/ubuntu/out"

func main() {
	err := envconfig.Init(&conf)
	if err != nil {
		log.Fatal(err)
	}
	go metriclogger.MonitorResourceUsage("storage")
	go metriclogger.SendMetrics(getMasterURL(), getOwnURL(), conf.ApiKey)

	router := mux.NewRouter()
	loggingMiddleware := middleware.LoggingMiddleware{InstanceId: "storage"}
	router.Use(loggingMiddleware.Middleware)
	authenticationMiddleware := middleware.AuthenticationMiddleware{ApiKey: conf.ApiKey}
	router.Use(authenticationMiddleware.Middleware)
	router.HandleFunc("/health", GetHealth)
	router.HandleFunc("/storeresult", storeResult).Methods("POST")
	router.HandleFunc("/result/{processingRequestID}", hasResult).Methods("GET")
	router.PathPrefix("/results/").Handler(http.StripPrefix("/results/", http.FileServer(http.Dir(outDir))))

	register()
	go checkMasterHealth()
	server := &http.Server{
		Handler:      router,
		Addr:         ":" + strconv.Itoa(conf.Own.Port),
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
	log.Fatal(server.ListenAndServe())

	defer unregister()
}

func GetHealth(w http.ResponseWriter, r *http.Request) {

}

func register() {
	for {
		options := grequests.RequestOptions{
			JSON:    map[string]string{"address": getOwnURL()},
			Headers: map[string]string{"Content-Type": "application/json", "X-Auth": conf.ApiKey},
		}
		resp, err := grequests.Post(getMasterURL()+"/storage/register", &options)
		defer resp.Close()
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
		Headers: map[string]string{"Content-Type": "application/json", "X-Auth": conf.ApiKey},
	}
	resp, err := grequests.Delete(getMasterURL()+"/storage/unregister", &options)
	defer resp.Close()
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
	options := grequests.RequestOptions{
		Headers: map[string]string{"X-Auth": conf.ApiKey},
	}
	for {
		resp, err := grequests.Get(getMasterURL()+"/health", &options)
		if err != nil {
			fmt.Println("Master seems to be offline")
			register()
		}
		resp.Close()
		time.Sleep(10 * time.Second)
	}
}

func storeResult(w http.ResponseWriter, r *http.Request) {
	bodyBytes, _ := ioutil.ReadAll(r.Body)
	result := Result{}
	json.Unmarshal(bodyBytes, &result)
	file, err := os.Create(outDir + "/" + result.ID.String())
	if err != nil {
		util.InternalServerError(w, "Error writing to file", err)
		return
	}
	defer file.Close()
	file.WriteString(strings.Trim(strings.Join(strings.Fields(fmt.Sprint(result.Values)), ","), "[]"))

}

func hasResult(w http.ResponseWriter, r *http.Request) {
	requestID, err := uuid.FromString(mux.Vars(r)["processingRequestID"])
	if err != nil {
		util.BadRequest(w, "Error parsing processingRequestId", err)
		return
	}
	files, err := ioutil.ReadDir(outDir)
	if err != nil {
		util.InternalServerError(w, "Error reading results from filesystem", err)
		return
	}

	for _, f := range files {
		if f.Name() == requestID.String() {
			w.WriteHeader(200)
			return
		}
	}
	w.WriteHeader(404)
}
