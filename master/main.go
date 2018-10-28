package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/doriandekoning/IN4392-cloud-computing-lab/graphs"
	"github.com/doriandekoning/IN4392-cloud-computing-lab/middleware"
	"github.com/doriandekoning/IN4392-cloud-computing-lab/util"
	"github.com/gorilla/mux"
	"github.com/levigross/grequests"
	uuid "github.com/satori/go.uuid"
	"github.com/vrischmann/envconfig"
)

type Config struct {
	ApiKey     string
	MaxWorkers int
}

var config Config

type NodeCollection []*node

var g graphs.Graph

type task struct {
	Graph      *graphs.Graph
	Parameters map[string][]string
}

type node struct {
	Address               string
	InstanceId            string
	LastResponseTimestamp int64
	Healthy               bool
	Active                bool
	TasksProcessing       []task
}

var workers NodeCollection
var storageNodes NodeCollection

var Sess *session.Session

const minWorkers = 1

var requestsSinceScaling = 0

func main() {

	err := envconfig.Init(&config)
	if err != nil {
		log.Fatal(err)
	}

	Sess, err = session.NewSession(&aws.Config{
		Region: aws.String("us-east-1"),
	})

	if err != nil {
		log.Fatal("Error", err)
	}

	router := mux.NewRouter()
	router.Use(middleware.LoggingMiddleWare)
	authenticationMiddleware := middleware.AuthenticationMiddleware{ApiKey: config.ApiKey}
	router.Use(authenticationMiddleware.Middleware)
	router.HandleFunc("/health", GetHealth).Methods("GET")
	router.HandleFunc("/killworkers", KillWorkersRequest).Methods("GET")
	router.HandleFunc("/addworker", AddWorkerRequest).Methods("GET")
	router.HandleFunc("/processgraph", ProcessGraph).Methods("POST")
	router.HandleFunc("/{nodetype}/register", registerNode).Methods("POST")
	router.HandleFunc("/worker/done", workerDoneProcessing).Methods("GET")
	router.HandleFunc("/storagenode", listStorageNodes).Methods("GET")
	router.HandleFunc("/worker/unregister", unregisterWorkerRequest).Methods("DELETE")
	router.HandleFunc("/result/{processingRequestId}", getResult).Methods("GET")

	go scaleWorkers()
	go getNodesHealth()
	server := &http.Server{
		Handler:      router,
		Addr:         ":8000",
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	log.Fatal(server.ListenAndServe())

}

func GetHealth(w http.ResponseWriter, r *http.Request) {
	var response = struct {
		MaxWorkers           int
		MinWorkers           int
		RequestsSinceScaling int
		ActiveWorkers        int
		Workers              []*node
	}{
		MaxWorkers:           config.MaxWorkers,
		MinWorkers:           minWorkers,
		RequestsSinceScaling: requestsSinceScaling,
		ActiveWorkers:        len(workers.filter(true, true)),
		Workers:              workers,
	}

	js, err := json.Marshal(response)
	if err != nil {
		util.InternalServerError(w, "Health could not be retrieved", err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(js)
}

func KillWorkersRequest(w http.ResponseWriter, r *http.Request) {
	err := TerminateWorkers(workers)
	if err != nil {
		util.InternalServerError(w, "Workers could not be terminated", err)
		return
	}

	workers = nil
	util.GeneralResponse(w, true, "Workers terminated")
}

func AddWorkerRequest(w http.ResponseWriter, r *http.Request) {
	StartNewWorker()
	util.GeneralResponse(w, true, "New worker started")
}

func workerDoneProcessing(w http.ResponseWriter, r *http.Request) {
	b, _ := ioutil.ReadAll(r.Body)
	type payload struct {
		RequestId  string
		InstanceId string
	}
	var actualPayload payload
	err := json.Unmarshal(b, &actualPayload)
	if err != nil {
		util.BadRequest(w, "Error unmarshaling body", err)
		return
	}
	processingRequestId, err := uuid.FromString(actualPayload.RequestId)
	if err != nil {
		util.BadRequest(w, "Error parsing processingRequestId", err)
		return
	}
	var worker = getNode(actualPayload.InstanceId)
	if worker == nil {
		util.BadRequest(w, "Error finding worker instanceId", err)
		return
	}

	for index, job := range worker.TasksProcessing {
		if job.Graph.Id == processingRequestId {
			worker.TasksProcessing[index] = worker.TasksProcessing[len(worker.TasksProcessing)-1]
			worker.TasksProcessing = worker.TasksProcessing[:len(worker.TasksProcessing)-1]
			break
		}
	}
}

func ProcessGraph(w http.ResponseWriter, r *http.Request) {
	requestsSinceScaling++
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

	if len(workers.filter(true, true)) == 0 {
		util.InternalServerError(w, "No workers found", nil)
		return
	}
	//Asynchronously distribute the graph
	go distributeGraph(&graph, r.URL.Query())
	w.WriteHeader(http.StatusAccepted)
	//Write id to response
	idBytes, _ := graph.Id.MarshalText()
	w.WriteHeader(http.StatusAccepted)
	w.Write(idBytes)
}

func registerWorker(w http.ResponseWriter, r *http.Request) {
	var newWorker node

	b, _ := ioutil.ReadAll(r.Body)
	err := json.Unmarshal(b, &newWorker)
	if err != nil {
		util.BadRequest(w, "Error unmarshaling body", err)
		return
	}
	newWorker.LastResponseTimestamp = time.Now().Unix()
	newWorker.Healthy = true
	workers = append(workers, &newWorker)
	util.GeneralResponse(w, true, "Worker: "+newWorker.InstanceId+" successfully registered!")
}

func unregisterWorkerRequest(w http.ResponseWriter, r *http.Request) {
	var oldWorker node
	bodyBytes, _ := ioutil.ReadAll(r.Body)
	err := json.Unmarshal(bodyBytes, &oldWorker)
	if err != nil {
		util.BadRequest(w, "Error unmarshalling body", err)
		return
	}
	unregisterWorker(&oldWorker)
	util.GeneralResponse(w, true, "Worker: "+oldWorker.InstanceId+" successfully unregistered!")
}

func unregisterWorker(oldWorker *node) {
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
		TerminateWorkers([]*node{oldWorker})
	}
	//Redistribute graphs this worker was still processing
	for _, task := range oldWorker.TasksProcessing {
		distributeGraph(task.Graph, task.Parameters)
	}
}

func registerNode(w http.ResponseWriter, r *http.Request) {
	var newNode node
	var nodesOfType *NodeCollection
	nodeType := mux.Vars(r)["nodetype"]
	if nodeType == "storage" {
		nodesOfType = &storageNodes
	} else if nodeType == "worker" {
		nodesOfType = &workers
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
	newNode.Healthy = true
	var replaced bool
	for nodeIndex, node := range *nodesOfType {
		if node.Address == node.Address {
			(*nodesOfType)[nodeIndex] = &newNode
			replaced = true
			break
		}
	}

	if !replaced {
		*nodesOfType = append(*nodesOfType, &newNode)
	}
	fmt.Println(nodeType + " node sucessfully registered")

}

func distributeGraph(graph *graphs.Graph, parameters map[string][]string) {
	var activeWorkers = workers.filter(true, true)
	if len(activeWorkers) == 0 {
		fmt.Println("No workers available")
		return
	}
	for {
		// Distribute graph among workers by randomly selecting a worker
		// possible improvement select the worker which has the shortest queue
		var worker = activeWorkers[rand.Intn(len(activeWorkers))]
		err := sendGraphToWorker(*graph, worker, parameters)
		if err == nil {
			worker.TasksProcessing = append(worker.TasksProcessing, task{graph, parameters})
			break
		}
		fmt.Println("Cannot distributes graph to: " + worker.Address)
		//Try again in 10 sec
		time.Sleep(10 * time.Second)
	}
}

func sendGraphToWorker(graph graphs.Graph, worker *node, parameters map[string][]string) error {
	options := grequests.RequestOptions{
		JSON:    graph,
		Headers: map[string]string{"Content-Type": "application/json", "X-Auth": config.ApiKey},
		Params:  paramsMapToRequestParamsMap(parameters),
	}
	resp, err := grequests.Post(worker.Address+"/graph", &options)
	if err != nil {
		return err
	}
	defer resp.Close()
	return nil
}

func getNodesHealth() {
	requestOptions := grequests.RequestOptions{Headers: map[string]string{"X-Auth": config.ApiKey}}
	for {
		for _, node := range append(workers, storageNodes...) {
			_, err := grequests.Get(node.Address+"/health", &requestOptions)
			if err != nil {
				node.Healthy = false
			} else {
				node.Healthy = true
				node.LastResponseTimestamp = time.Now().Unix()
			}
			if time.Now().Unix()-node.LastResponseTimestamp > 60 {
				unregisterWorker(node)
			}
		}
		time.Sleep(30 * time.Second)
	}
}

func scaleWorkers() {
	for {
		time.Sleep(60 * time.Second)

		//Check scaling up
		const queueSizeThreshold = 2
		var activeHealthyWorkers = workers.filter(true, true)
		var inactiveHealthyWorkers = workers.filter(false, true)
		var minQueueLength = 1000000
		for _, worker := range activeHealthyWorkers {
			if minQueueLength > len(worker.TasksProcessing) {
				minQueueLength = len(worker.TasksProcessing)
			}
		}

		if minQueueLength >= queueSizeThreshold {
			if len(inactiveHealthyWorkers) > 0 {
				inactiveHealthyWorkers[0].Active = true
			} else {
				StartNewWorker()
			}
		}
		//Check for scaling down
		for _, worker := range activeHealthyWorkers {
			if len(worker.TasksProcessing) == 0 {
				worker.Active = false
			}
		}

	}
}

func (NodeCollection) filter(active, healthy bool) NodeCollection {
	var result NodeCollection
	for _, node := range workers {
		if node.Active == active && node.Healthy == healthy {
			result = append(result, node)
		}
	}
	return result
}

func getNode(instanceId string) *node {
	for _, worker := range workers {
		if worker.InstanceId == instanceId {
			return worker
		}
	}
	return nil
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
