package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/doriandekoning/IN4392-cloud-computing-lab/graphs"
	"github.com/doriandekoning/IN4392-cloud-computing-lab/middleware"
	"github.com/levigross/grequests"
	uuid "github.com/satori/go.uuid"

	"github.com/gorilla/mux"
)

var g graphs.Graph

type worker struct {
	Address string
	Healty  bool
}

var workers []*worker

func main() {

	router := mux.NewRouter()
	router.Use(middleware.LoggingMiddleWare)
	router.HandleFunc("/health", GetHealth).Methods("GET")
	router.HandleFunc("/processgraph", ProcessGraph).Methods("POST")
	router.HandleFunc("/worker/register", registerWorker).Methods("POST")
	router.HandleFunc("/worker/unregister", unregisterWorker).Methods("DELETE")

	go getWorkersHealth()
	log.Fatal(http.ListenAndServe(":8000", router))

}

func GetHealth(w http.ResponseWriter, r *http.Request) {

}

func ProcessGraph(w http.ResponseWriter, r *http.Request) {
	algorithm := r.URL.Query().Get("algorithm")
	csvReader := csv.NewReader(r.Body)
	//Parse first line with vertex weights
	line, err := csvReader.Read()
	if err == io.EOF {
		log.Fatalf("Cannot parse node weights")
	}
	//Init graph
	graph := graphs.Graph{Nodes: make([]*graphs.Node, len(line))}
	graph.Id = uuid.Must(uuid.NewV4())

	//Init all nodes
	for index, weight := range line {
		parsedWeight, err := strconv.ParseFloat(weight, 64)
		if err != nil {
			log.Fatalf("Error reading edge weight %s", weight)
		}
		graph.Nodes[index] = &graphs.Node{
			Id:            index,
			IncomingEdges: []*graphs.Edge{},
			OutgoingEdges: []*graphs.Edge{},
			Value:         parsedWeight,
		}
	}
	//Parse edges
	for {
		line, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal("Error parsing csv line")
		}
		if len(line) != 3 {
			log.Fatal("Line length too long")
		}
		from, err1 := strconv.Atoi(line[0])
		to, err2 := strconv.Atoi(line[1])
		weight, err3 := strconv.ParseFloat(line[2], 32)
		if err1 != nil || err2 != nil || err3 != nil {
			log.Fatal("Error converting string to int", err)
		}

		graph.AddEdge(graphs.Edge{Start: from, End: to, Weight: weight})
	}
	g = graph

	//Asynchronously distribute the graph
	go distributeGraph(&graph, algorithm)
	idBytes, _ := graph.Id.MarshalText()
	w.Write(idBytes)

}

func registerWorker(w http.ResponseWriter, r *http.Request) {
	var newWorker worker

	b, _ := ioutil.ReadAll(r.Body)
	err := json.Unmarshal(b, &newWorker)
	if err != nil {
		log.Fatal("Error", err)
	}
	workers = append(workers, &newWorker)
	fmt.Println("Worker: " + newWorker.Address + " successfully registered!")
}

func unregisterWorker(w http.ResponseWriter, r *http.Request) {
	var oldWorker worker
	bodyBytes, _ := ioutil.ReadAll(r.Body)
	err := json.Unmarshal(bodyBytes, &oldWorker)
	if err != nil {
		log.Fatal("Error", err)
	}
	for index, worker := range workers {
		if worker.Address == oldWorker.Address {
			//Move last worker to location of worker to remove
			workers[index] = workers[len(workers)-1]
			//Chop of last worker (since it is now at location of old one)
			workers = workers[:len(workers)-1]
			break
		}
	}
	fmt.Println("Worker: " + oldWorker.Address + " successfully unregistered!")
}

func distributeGraph(graph *graphs.Graph, algorithm string) {
	// Distribute graph among workers
	var workerID int
	if len(workers) == 0 {
		log.Fatal("No workers available")
		//TODO determine to which worker to send node (for now to first free worker)
		workerID = 0
	}
	err := sendGraphToWorker(*graph, workers[workerID], algorithm)
	if err != nil {
		fmt.Println("Cannot distributes graph to: " + workers[workerID].Address)
	}
}

func sendGraphToWorker(graph graphs.Graph, worker *worker, algorithm string) error {
	options := grequests.RequestOptions{
		JSON:    graph,
		Headers: map[string]string{"Content-Type": "application/json"},
		Params:  map[string]string{"algorithm": algorithm},
	}
	_, err := grequests.Post(worker.Address+"/graph", &options)
	if err != nil {
		return err
	}
	return nil
}

func getWorkersHealth() {
	for {
		for _, worker := range workers {
			_, err := grequests.Get(worker.Address+"/health", nil)
			if err != nil {
				worker.Healty = false
			} else {
				worker.Healty = true
			}
		}
		time.Sleep(15 * time.Second)
	}
}
