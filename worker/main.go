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

type worker struct {
	Address string
	Healty  bool
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
		_, err := grequests.Post(getMasterURL()+"/worker/register", &options)
		if err == nil {
			fmt.Println("Successfully registered")
			break
		}
		fmt.Println("Unable to register")
		//Try again in 10 sec
		time.Sleep(10 * time.Second)
	}
}

func unregister() {
	options := grequests.RequestOptions{
		JSON:    map[string]string{"address": getOwnURL()},
		Headers: map[string]string{"Content-Type": "application/json"},
	}
	_, err := grequests.Delete(getMasterURL()+"/worker/unregister", &options)
	if err != nil {
		log.Fatal("Unable to unregister: ", err)
	}
	fmt.Println("Sucessfully unregistered")

}

func getSubProblem(w http.ResponseWriter, r *http.Request) {
	bodyBytes, _ := ioutil.ReadAll(r.Body)
	type payload struct {
		workers []worker
		nodes   []graphs.Node
	}
	actualPayload := payload{}
	json.Unmarshal(bodyBytes, &actualPayload)
}

func getMasterURL() string {
	return conf.Master.Address + ":" + strconv.Itoa(conf.Master.Port)
}

func getOwnURL() string {
	return conf.Own.Address + ":" + strconv.Itoa(conf.Own.Port)
}

func ReceiveGraph(w http.ResponseWriter, r *http.Request) {
	b, _ := ioutil.ReadAll(r.Body)
	err := json.Unmarshal(b, &graph)
	algorithm := graphs.SortestPath{Graph: &graph}
	algorithm.Initialize()
	if err != nil {
		log.Fatal("Error unmashalling graph", err)
	}
	for _, node := range graph.Nodes {
		node.Graph = &graph
	}
	step := 0
outerloop:
	for true {
		for _, node := range graph.Nodes {
			algorithm.Step(node, step)
		}
		step++

		for _, node := range graph.Nodes {
			if !node.VotedToHalt {
				continue outerloop
			}
		}
		break
	}

	for _, node := range graph.Nodes {
		fmt.Printf("Final val for node %d: %f\n", node.Id, node.Value)
	}
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
