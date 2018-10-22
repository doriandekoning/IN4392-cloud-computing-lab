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

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/gorilla/mux"
	"github.com/levigross/grequests"
)

var g Graph

type worker struct {
	Address    string
	InstanceId string
	Healty     bool
}

var workers []*worker

var sess *session.Session

func main() {

	var err error

	sess, err = session.NewSession(&aws.Config{
		Region: aws.String("us-east-1"),
	})

	if err != nil {
		log.Fatal("Error", err)
	}

	if err != nil {
		log.Println(err)
	}

	router := mux.NewRouter()
	router.Use(loggingMiddleWare)
	router.HandleFunc("/health", GetHealth).Methods("GET")
	router.HandleFunc("/killworkers", KillWorkersRequest).Methods("GET")
	router.HandleFunc("/addworker", AddWorkerRequest).Methods("GET")
	router.HandleFunc("/processgraph", ProcessGraph).Methods("POST")
	router.HandleFunc("/worker/register", registerWorker).Methods("POST")
	router.HandleFunc("/worker/unregister", unregisterWorker).Methods("DELETE")

	go getWorkersHealth()
	log.Fatal(http.ListenAndServe(":8000", router))

}

func GetHealth(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "Registered workers: "+strconv.Itoa(len(workers)))
	for _, worker := range workers {
		fmt.Fprintln(w, worker.Address+" -- "+worker.InstanceId+" -- "+strconv.FormatBool(worker.Healty))
	}
}

func KillWorkersRequest(w http.ResponseWriter, r *http.Request) {
	err := TerminateWorkers(workers)
	if err == nil {
		workers = nil
		fmt.Println("Workers terminated")
	}
}

func AddWorkerRequest(w http.ResponseWriter, r *http.Request) {
	startNewWorker()
	fmt.Println("New worker started")
}

func loggingMiddleWare(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Println("[" + r.RequestURI + "]")
		next.ServeHTTP(w, r)
	})
}

func ProcessGraph(w http.ResponseWriter, r *http.Request) {
	graphSize, _ := strconv.Atoi(r.URL.Query().Get("graphsize"))

	graph := Graph{Nodes: make([]*Node, graphSize)}

	for i := 0; i < graphSize; i++ {
		graph.Nodes[i] = &Node{Id: i, IncomingEdges: []*Edge{}, OutgoingEdges: []*Edge{}}
	}

	csvReader := csv.NewReader(r.Body)
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
		weight, err3 := strconv.Atoi(line[2])
		if err1 != nil || err2 != nil || err3 != nil {
			log.Fatal("Error converting string to int", err)
		}

		graph.addEdge(Edge{from, to, weight})
	}
	g = graph

	//Asynchronously distribute the graph
	go distributeGraph(&graph)
}

func registerWorker(w http.ResponseWriter, r *http.Request) {
	var newWorker worker

	b, _ := ioutil.ReadAll(r.Body)
	err := json.Unmarshal(b, &newWorker)
	if err != nil {
		log.Fatal("Error", err)
	}
	workers = append(workers, &newWorker)
	fmt.Println("Worker: " + newWorker.InstanceId + " successfully registered!")
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
	if oldWorker.InstanceId != "" {
		//TODO terminate worker
	}
	fmt.Println("Worker: " + oldWorker.Address + " successfully unregistered!")
}

func distributeGraph(graph *Graph) {
	if len(workers) == 0 {
		fmt.Println("No workers yet registered")
		return
	}
	subGraphs := make([]Graph, len(workers))
	for nodeIndex, node := range g.Nodes {
		if subGraphs[nodeIndex%len(workers)].Nodes == nil {
			subGraphs[nodeIndex%len(workers)].Nodes = make([]*Node, 0)
		}
		subGraphs[nodeIndex%len(workers)].Nodes = append(subGraphs[nodeIndex%len(workers)].Nodes, node)
	}
	// Distribute graph among workers
	for index, worker := range workers {
		err := sendSubgraphToWorker(subGraphs[index], worker)
		if err != nil {
			fmt.Println("Cannot distrubte graph to: " + worker.Address)
		}
		fmt.Println(worker)
	}
}

func sendSubgraphToWorker(subGraph Graph, worker *worker) error {
	options := grequests.RequestOptions{
		JSON:    subGraph,
		Headers: map[string]string{"Content-Type": "application/json"},
	}
	_, err := grequests.Post(worker.Address+"/subgraph", &options)
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
				fmt.Println("Worker with address: "+worker.Address+" seems to have gone offline:", err)
				worker.Healty = false
			} else {
				fmt.Println("Worker with address: " + worker.Address + " seems healthy!")
				worker.Healty = true
			}
		}
		time.Sleep(15 * time.Second)
	}
}
