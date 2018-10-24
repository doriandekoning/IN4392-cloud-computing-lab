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
	"github.com/doriandekoning/IN4392-cloud-computing-lab/util"
	"github.com/levigross/grequests"
	uuid "github.com/satori/go.uuid"

	"github.com/gorilla/mux"
)

var g graphs.Graph

type node struct {
	Address string
	Healthy bool
}

var workers []*node
var storageNodes []*node

func main() {

	router := mux.NewRouter()
	router.Use(middleware.LoggingMiddleWare)
	router.HandleFunc("/health", GetHealth).Methods("GET")
	router.HandleFunc("/processgraph", ProcessGraph).Methods("POST")
	router.HandleFunc("/{nodetype}/register", registerNode).Methods("POST")
	router.HandleFunc("/storagenode", listStorageNodes).Methods("GET")
	router.HandleFunc("/worker/unregister", unregisterWorker).Methods("DELETE")
	router.HandleFunc("/result/{processingRequestId}", getResult).Methods("GET")

	go getNodesHealth()
	log.Fatal(http.ListenAndServe(":8000", router))

}

func GetHealth(w http.ResponseWriter, r *http.Request) {

}

func ProcessGraph(w http.ResponseWriter, r *http.Request) {
	csvReader := csv.NewReader(r.Body)
	//Parse first line with vertex weights
	line, err := csvReader.Read()
	if err == io.EOF {
		util.BadRequest(w, "Provided csv file is empty", err)
		return
	}
	//Init graph
	graph := graphs.Graph{Nodes: make([]*graphs.Node, len(line))}
	graph.Id = uuid.Must(uuid.NewV4())

	//Init all nodes
	for index, weight := range line {
		parsedWeight, err := strconv.ParseFloat(weight, 64)
		if err != nil {
			util.BadRequest(w, fmt.Sprint("Cannot convert edge weigth %s to float", weight), err)
			return
		}
		graph.Nodes[index] = &graphs.Node{
			Id:            index,
			IncomingEdges: []*graphs.Edge{},
			OutgoingEdges: []*graphs.Edge{},
			Value:         parsedWeight,
		}
	}
	//Parse edges
	var lineNumber int
	for {
		lineNumber++
		line, err := csvReader.Read()

		if err == io.EOF {
			break
		}
		if err != nil {
			util.BadRequest(w, fmt.Sprintf("Error reading line:%d", lineNumber), err)
			return
		}
		if len(line) != 3 {
			util.BadRequest(w, fmt.Sprint(w, "To much comma seperated parts in:%d", lineNumber), nil)
			return
		}
		from, err1 := strconv.Atoi(line[0])
		to, err2 := strconv.Atoi(line[1])
		weight, err3 := strconv.ParseFloat(line[2], 32)
		if err1 != nil || err2 != nil || err3 != nil {
			util.BadRequest(w, fmt.Sprintf("Error reading line:%d", lineNumber), err1)
			return
		}

		graph.AddEdge(graphs.Edge{Start: from, End: to, Weight: weight})
		lineNumber++
	}
	g = graph

	if len(workers) == 0 {
		util.InternalServerError(w, "No workers found", nil)
		return
	}
	//Asynchronously distribute the graph
	go distributeGraph(&graph, r.URL.Query())
	//Write id to response
	idBytes, _ := graph.Id.MarshalText()
	w.Write(idBytes)
	//TODO check why this throws "multiple writes to header"
	w.WriteHeader(http.StatusAccepted)

}

func unregisterWorker(w http.ResponseWriter, r *http.Request) {
	var oldWorker node
	bodyBytes, _ := ioutil.ReadAll(r.Body)
	err := json.Unmarshal(bodyBytes, &oldWorker)
	if err != nil {
		util.BadRequest(w, "Error unmarshalling body", err)
		return
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

func registerNode(w http.ResponseWriter, r *http.Request) {
	var newNode node
	var allNodes *[]*node
	nodeType := mux.Vars(r)["nodetype"]
	if nodeType == "storage" {
		allNodes = &storageNodes
	} else if nodeType == "worker" {
		allNodes = &workers
	} else {
		util.BadRequest(w, "Nodetype "+nodeType+" is not known", nil)
		return
	}
	b, _ := ioutil.ReadAll(r.Body)
	err := json.Unmarshal(b, &newNode)
	if err != nil {
		util.BadRequest(w, "Error unmarshalling storagenode registration body", err)
		return
	}
	var replaced bool
	for storageNodeIndex, storageNode := range *allNodes {
		if storageNode.Address == newNode.Address {
			storageNodes[storageNodeIndex] = &newNode
			replaced = true
			break
		}
	}

	if !replaced {
		*allNodes = append(*allNodes, &newNode)
	}
	fmt.Println("Storage node sucessfully registered")
}

func distributeGraph(graph *graphs.Graph, parameters map[string][]string) {
	// Distribute graph among workers
	var workerID int
	//TODO determine to which worker to send node (for now to first free worker)
	err := sendGraphToWorker(*graph, workers[workerID], parameters)
	if err != nil {
		fmt.Println("Cannot distributes graph to: " + workers[workerID].Address)
	}
}

func sendGraphToWorker(graph graphs.Graph, worker *node, parameters map[string][]string) error {
	options := grequests.RequestOptions{
		JSON:    graph,
		Headers: map[string]string{"Content-Type": "application/json"},
		Params:  paramsMapToRequestParamsMap(parameters),
	}
	_, err := grequests.Post(worker.Address+"/graph", &options)
	if err != nil {
		return err
	}
	return nil
}

func getNodesHealth() {
	for {
		for _, node := range append(workers, storageNodes...) {
			_, err := grequests.Get(node.Address+"/health", nil)
			if err != nil {
				node.Healthy = false
			} else {
				node.Healthy = true
			}
		}
		time.Sleep(15 * time.Second)
	}
}

func paramsMapToRequestParamsMap(original map[string][]string) map[string]string {
	retval := map[string]string{}
	for k, v := range original {
		retval[k] = v[0]
	}
	return retval
}

func listStorageNodes(w http.ResponseWriter, r *http.Request) {
	if storageNodes == nil {
		w.Write([]byte("{}"))
	} else {
		retVal, err := json.Marshal(storageNodes)
		if err != nil {
			util.InternalServerError(w, "Cannot marshall nodes", err)
			return
		}
		w.Write(retVal)
	}
}

type hasRequestResult struct {
	statusCode  int
	nodeAddress string
}

func getResult(w http.ResponseWriter, r *http.Request) {
	requestID, err := uuid.FromString(mux.Vars(r)["processingRequestId"])
	if err != nil {
		util.BadRequest(w, "Error parsing processingRequestId", err)
		return
	}

	respChannel := make(chan hasRequestResult)
	for _, node := range storageNodes {
		go storageNodeHasResult(respChannel, node.Address, requestID)
	}
	amountResults := 0
	var resultsNeeded = (len(storageNodes) + 1) / 2
	for i := 0; i < len(storageNodes); i++ {
		result := <-respChannel
		if result.statusCode != -1 && result.statusCode < 300 {
			amountResults++
			if amountResults >= resultsNeeded {
				w.Write([]byte(result.nodeAddress + "/results/" + requestID.String()))
				return
			}
		}
	}
}

func storageNodeHasResult(respChannel chan hasRequestResult, nodeAddress string, requestID uuid.UUID) {
	resp, err := grequests.Get(nodeAddress+"/result/"+requestID.String(), nil)
	if err != nil {
		fmt.Println("Error getting info if node has result ", err)
	}
	respChannel <- hasRequestResult{resp.StatusCode, nodeAddress}
}
