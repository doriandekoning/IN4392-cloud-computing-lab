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

	"github.com/davecgh/go-spew/spew"

	"github.com/gorilla/mux"
)

type worker struct {
	address string
}

var workers []*worker

type edge struct {
	start  int
	end    int
	weight int
}

type graph struct {
	edges []*edge
}

func main() {

	router := mux.NewRouter()
	router.Use(loggingMiddleWare)
	router.HandleFunc("/health", GetHealth).Methods("GET")
	router.HandleFunc("/processgraph", ProcessGraph).Methods("POST")
	router.HandleFunc("/worker/register", registerWorker).Methods("POST")
	router.HandleFunc("/worker/unregister", unregisterWorker).Methods("DELETE")
	log.Fatal(http.ListenAndServe(":8000", router))

}

func GetHealth(w http.ResponseWriter, r *http.Request) {

}

func loggingMiddleWare(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Println("[" + r.RequestURI + "]")
		next.ServeHTTP(w, r)
	})
}

func ProcessGraph(w http.ResponseWriter, r *http.Request) {
	graphSize := r.URL.Query().Get("graphsize")
	fmt.Println(graphSize)

	csvReader := csv.NewReader(r.Body)
	graph := graph{edges: make([]*edge, 0)}
	lineNumber := 0
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

		edge := edge{from, to, weight}
		graph.edges = append(graph.edges, &edge)
		lineNumber++
	}

	//Asynchronously distribute the graph
	go distributeGraph(&graph)
}

func registerWorker(w http.ResponseWriter, r *http.Request) {
	var newWorker worker
	bodyBytes, _ := ioutil.ReadAll(r.Body)
	err := json.Unmarshal(bodyBytes, &newWorker)
	if err != nil {
		log.Fatal("Error", err)
	}
	workers = append(workers, &newWorker)
	fmt.Println("Worker: " + newWorker.address + " successfully registered!")
}

func unregisterWorker(w http.ResponseWriter, r *http.Request) {
	var oldWorker worker
	bodyBytes, _ := ioutil.ReadAll(r.Body)
	err := json.Unmarshal(bodyBytes, &oldWorker)
	if err != nil {
		log.Fatal("Error", err)
	}
	for index, worker := range workers {
		if worker.address == oldWorker.address {
			//Move last worker to location of worker to remove
			workers[index] = workers[len(workers)-1]
			//Chop of last worker (since it is now at location of old one)
			workers = workers[:len(workers)-1]
			break
		}
	}
	fmt.Println("Worker: " + oldWorker.address + " successfully unregistered!")
}

func distributeGraph(graph *graph) {
	if len(workers) == 0 {
		fmt.Println("No workers yet registered")
		return
	}
	// Distribute graph among workers
	for _, worker := range workers {
		spew.Dump(worker)
	}
}
