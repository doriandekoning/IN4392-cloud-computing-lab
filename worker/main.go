package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
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
		Port       int
		Address    string
		Instanceid string `envconfig:"optional"`
	}
	ApiKey string
}

type node struct {
	Address string
	Healthy bool
}

type Result struct {
	ID        uuid.UUID
	Algorithm string
	Values    []float64
}

type task struct {
	Graph      *graphs.Graph
	Parameters map[string]string
}

var conf Config
var taskChannel chan task

func main() {
	err := envconfig.Init(&conf)
	if err != nil {
		log.Fatal(err)
	}

	go metriclogger.MonitorResourceUsage(conf.Own.Instanceid)
	go metriclogger.SendMetrics(getMasterURL(), getOwnURL(), conf.ApiKey)

	router := mux.NewRouter()
	loggingMiddleware := middleware.LoggingMiddleware{InstanceId: conf.Own.Instanceid}
	router.Use(loggingMiddleware.Middleware)
	authenticationMiddleware := middleware.AuthenticationMiddleware{ApiKey: conf.ApiKey}
	router.Use(authenticationMiddleware.Middleware)
	router.HandleFunc("/health", GetHealth)
	router.HandleFunc("/graph", ReceiveGraph).Methods("POST")

	register()
	go checkMasterHealth()

	taskChannel = make(chan task)
	go ProcessGraphsWhenAvailable()
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
			JSON:    map[string]string{"address": getOwnURL(), "instanceId": conf.Own.Instanceid},
			Headers: map[string]string{"Content-Type": "application/json", "X-Auth": conf.ApiKey},
		}
		resp, err := grequests.Post(getMasterURL()+"/worker/register", &options)
		if err == nil && resp.StatusCode < 300 {
			fmt.Println("Successfully registered")
			defer resp.Close()
			break
		}
		fmt.Println("Unable to register, statuscode: ", resp.StatusCode)
		//Try again in 10 sec
		time.Sleep(10 * time.Second)
	}
}

func unregister() {
	options := grequests.RequestOptions{
		JSON:    map[string]string{"address": getOwnURL(), "instanceId": conf.Own.Instanceid},
		Headers: map[string]string{"Content-Type": "application/json", "X-Auth": conf.ApiKey},
	}
	resp, err := grequests.Delete(getMasterURL()+"/worker/unregister", &options)
	if err != nil && resp.StatusCode >= 300 {
		fmt.Println("Unable to register, statuscode:", resp.StatusCode)
		return
	}
	defer resp.Close()
	fmt.Println("Sucessfully unregistered")

}

func getMasterURL() string {
	return conf.Master.Address + ":" + strconv.Itoa(conf.Master.Port)
}

func getOwnURL() string {
	return conf.Own.Address + ":" + strconv.Itoa(conf.Own.Port)
}

func ReceiveGraph(w http.ResponseWriter, r *http.Request) {
	algorithm := r.URL.Query().Get("algorithm")
	maxSteps, err := strconv.Atoi(r.URL.Query().Get("maxsteps"))
	if err != nil || maxSteps < 1 {
		util.BadRequest(w, "Max steps is not a valid number: "+r.URL.Query().Get("maxsteps"), nil)
		return
	}
	size, err := strconv.Atoi(r.URL.Query().Get("size"))
	if err != nil {
		util.BadRequest(w, "Size is not a valid number: "+r.URL.Query().Get("size"), nil)
		return
	}
	initialNodeValue, err := strconv.ParseFloat(r.URL.Query().Get("initialNodeValue"), 64)
	if err != nil {
		util.BadRequest(w, "InitialnodeValue is not a valid number: "+r.URL.Query().Get("initialNodeValue"), nil)
		return
	}

	id, err := uuid.FromString(r.URL.Query().Get("requestID"))
	if err != nil {
		util.BadRequest(w, "Size is not a valid uuid: "+r.URL.Query().Get("requestID"), nil)
		return
	}
	var graph graphs.Graph
	graphBytes, err := ioutil.ReadAll(r.Body)
	if err != nil {
		util.BadRequest(w, "Error reading file from request", err)
	}

	graph = graphs.FromBytes(graphBytes, size, initialNodeValue)
	graph.Id = id
	taskChannel <- task{&graph, map[string]string{"algorithm": algorithm, "maxSteps": r.URL.Query().Get("maxsteps")}}

	// Log if a graph is added to the channel.
	metriclogger.Measurement{conf.Own.Instanceid, metriclogger.WorkerQueue, len(taskChannel), 0}.Log()
}

func ProcessGraphsWhenAvailable() {
	for {
		task := <-taskChannel
		ProcessGraph(task.Graph, task.Parameters)

		// Log if a graph is processed from the channel.
		metriclogger.Measurement{conf.Own.Instanceid, metriclogger.WorkerQueue, len(taskChannel), 0}.Log()
	}
}

func ProcessGraph(graph *graphs.Graph, parameters map[string]string) {
	algorithm := parameters["algorithm"]
	maxSteps, err := strconv.Atoi(parameters["maxSteps"])
	if err != nil || maxSteps < 1 {
		fmt.Println("Invalid maxSteps parameter")
		return
	}
	var instance graphs.AlgorithmInterface
	switch algorithm {
	case "pagerank":
		instance = &graphs.PagerankInstance{Graph: graph, MaxSteps: maxSteps}
	case "shortestpath":
		instance = &graphs.ShortestPathInstance{Graph: graph}
	default:
		fmt.Println("Algorithm not found")
		return
	}

	instance.Initialize()

	for _, node := range graph.Nodes {
		node.Graph = graph
	}
	step := 0
outerloop:
	for true {
		time.Sleep(10 * time.Millisecond)
		for _, node := range graph.Nodes {
			if node.Active {
				instance.Step(node, step)
			}
		}
		step++

		for _, node := range graph.Nodes {
			if node.Active {
				continue outerloop
			}
		}
		break
	}
	result := Result{ID: graph.Id, Algorithm: algorithm, Values: make([]float64, len(graph.Nodes))}
	for nodeIndex, node := range graph.Nodes {
		// values = append(values, strconv.FormatFloat(node.Value, 'f', 6, 64))
		result.Values[nodeIndex] = node.Value
	}

	writeResultToStorage(&result)
	notifyMasterOnProcessCompletion(graph.Id)

}

func notifyMasterOnProcessCompletion(graphId uuid.UUID) {
	requestOptions := grequests.RequestOptions{
		Headers: map[string]string{"Content-Type": "application/json", "X-Auth": conf.ApiKey},
		Params:  map[string]string{"requestID": graphId.String(), "instanceID": conf.Own.Instanceid},
	}

	_, err := grequests.Get(getMasterURL()+"/worker/done", &requestOptions)
	if err != nil {
		fmt.Println("Unable to notify master about finishing processing a graph, error:", err)
		return
	}
}

func checkMasterHealth() {
	requestOptions := grequests.RequestOptions{Headers: map[string]string{"X-Auth": conf.ApiKey}}
	for {
		resp, err := grequests.Get(getMasterURL()+"/health", &requestOptions)

		if err != nil {
			fmt.Println("Master seems to be offline")
			register()
		} else {
			defer resp.Close()
		}
		time.Sleep(10 * time.Second)
	}
}

func writeResultToStorage(result *Result) {
	//TODO maybe return this from health
	requestOptions := grequests.RequestOptions{Headers: map[string]string{"X-Auth": conf.ApiKey}}
	resp, err := grequests.Get(getMasterURL()+"/storagenode", &requestOptions)
	if err != nil || resp.StatusCode >= 300 {
		fmt.Println("Error when trying to get storage node adresses from master: ", resp.StatusCode, err)
		return
	}
	storageNodes := make([]node, 0)
	err = json.Unmarshal(resp.Bytes(), &storageNodes)
	if err != nil {
		fmt.Println("Error unmarshaling storage nodes", resp.String(), err)
		return
	}
	options := grequests.RequestOptions{
		JSON:    result,
		Headers: map[string]string{"Content-Type": "application/json", "X-Auth": conf.ApiKey},
	}
	respChannel := make(chan int)
	for i := 0; i < len(storageNodes); i++ {
		go writeResultToSpecificStorageNode(storageNodes[i], options, respChannel)
	}
	var successfullWrites int
	var writesNeeded = (len(storageNodes) + 1) / 2
	for i := 0; i < len(storageNodes); i++ {
		statusCode := <-respChannel
		if statusCode > 0 && statusCode < 300 {
			successfullWrites++
			if successfullWrites >= writesNeeded {
				fmt.Println("Successfull write to enough nodes for: " + result.ID.String())
				return
			}
		}
	}
	fmt.Println("Failed to write result:" + result.ID.String())
}

func writeResultToSpecificStorageNode(storageNode node, options grequests.RequestOptions, respChannel chan int) {

	resp, err := grequests.Post(storageNode.Address+"/storeresult", &options)
	if err != nil {
		respChannel <- -1
	} else {
		respChannel <- resp.StatusCode
	}
}
