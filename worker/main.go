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
	"github.com/doriandekoning/IN4392-cloud-computing-lab/middleware"
	"github.com/doriandekoning/IN4392-cloud-computing-lab/util"
	uuid "github.com/satori/go.uuid"

	"github.com/gorilla/mux"
	"github.com/levigross/grequests"
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

type node struct {
	Address string
	Healty  bool
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
	router.HandleFunc("/graph", ReceiveGraph).Methods("POST")

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
		resp, err := grequests.Post(getMasterURL()+"/worker/register", &options)
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
	resp, err := grequests.Delete(getMasterURL()+"/worker/unregister", &options)
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

func ReceiveGraph(w http.ResponseWriter, r *http.Request) {
	b, _ := ioutil.ReadAll(r.Body)
	algorithm := r.URL.Query().Get("algorithm")
	maxSteps, err := strconv.Atoi(r.URL.Query().Get("maxsteps"))
	if err != nil || maxSteps < 1 {
		util.BadRequest(w, "Max steps is not a valid number: "+r.URL.Query().Get("maxsteps"), nil)
		return
	}
	err = json.Unmarshal(b, &graph)
	if err != nil {
		util.BadRequest(w, "Cannot unmarshal graph", err)
		return
	}

	var instance graphs.AlgorithmInterface
	switch algorithm {
	case "pagerank":
		instance = &graphs.PagerankInstance{Graph: &graph, MaxSteps: maxSteps}
	case "shortestpath":
		instance = &graphs.ShortestPathInstance{Graph: &graph}
	default:
		fmt.Println("Algorithm not found")
		return
	}

	instance.Initialize()

	for _, node := range graph.Nodes {
		node.Graph = &graph
	}
	step := 0
outerloop:
	for true {
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

func writeResultToStorage(result *Result) {
	//TODO maybe return this from health
	resp, err := grequests.Get(getMasterURL()+"/storagenode", nil)
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
		Headers: map[string]string{"Content-Type": "application/json"},
	}
	//Choose a random node to write to
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
