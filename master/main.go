package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/doriandekoning/IN4392-cloud-computing-lab/graphs"
	"github.com/doriandekoning/IN4392-cloud-computing-lab/metriclogger"
	"github.com/doriandekoning/IN4392-cloud-computing-lab/middleware"
	"github.com/doriandekoning/IN4392-cloud-computing-lab/util"
	"github.com/gorilla/mux"
	"github.com/levigross/grequests"
	uuid "github.com/satori/go.uuid"
	"github.com/vrischmann/envconfig"
)

var config Config

var metricsFile *os.File
var amountLogFiles int

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

type Config struct {
	ApiKey     string
	MaxWorkers int
}

var workers NodeCollection
var storageNodes NodeCollection

var Sess *session.Session

const minWorkers = 1

func main() {

	err := envconfig.Init(&config)
	if err != nil {
		log.Fatal(err)
	}

	go metriclogger.MonitorResourceUsage()

	Sess, err = session.NewSession(&aws.Config{
		Region: aws.String("us-east-1"),
	})

	if err != nil {
		log.Fatal("Error", err)
	}
	metricsFile, err = os.Create("metrics/metrics")
	if err != nil {
		log.Fatal("Error", err)
	}
	defer postMetric()

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
	router.HandleFunc("/metrics", ProcessMetrics).Methods("POST")
	router.HandleFunc("/forcewritemetrics", forceWriteMetrics).Methods("GET")
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
		MaxWorkers    int
		MinWorkers    int
		ActiveWorkers int
		Workers       []*node
	}{
		MaxWorkers:    config.MaxWorkers,
		MinWorkers:    minWorkers,
		ActiveWorkers: len(workers.filter(true, true)),
		Workers:       workers,
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
	processingRequestId, err := uuid.FromString(r.URL.Query().Get("requestID"))
	if err != nil {
		util.BadRequest(w, "Error parsing processingRequestId", err)
		return
	}

	instanceId := r.URL.Query().Get("instanceID")
	var worker = getNode(instanceId)
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
		// Distribute graph among workers
		worker := getLeastBusyWorker()
		if worker == nil {
			return
		}
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

func getLeastBusyWorker() *node {
	activeWorkers := workers.filter(true, true)
	if len(activeWorkers) == 0 {
		return nil
	}
	leastBusyWorker := activeWorkers[0]
	for _, worker := range workers {
		if len(worker.TasksProcessing) < len(leastBusyWorker.TasksProcessing) {
			leastBusyWorker = worker
		}
	}
	return leastBusyWorker
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
		time.Sleep(20 * time.Second)

		//Check scaling up
		const queueSizeThreshold = 2
		var inactiveHealthyWorkers = workers.filter(false, true)
		leastBusyWorker := getLeastBusyWorker()
		if leastBusyWorker == nil {
			return
		}
		if len(leastBusyWorker.TasksProcessing) >= queueSizeThreshold {
			if len(inactiveHealthyWorkers) > 0 {
				inactiveHealthyWorkers[0].Active = true
			} else {
				StartNewWorker()
			}
		}
		//Check for scaling down
		const scaleDownThreshold = 2
		if len(leastBusyWorker.TasksProcessing) < scaleDownThreshold {
			leastBusyWorker.Active = false
		}
		for _, worker := range inactiveHealthyWorkers {
			if len(worker.TasksProcessing) == 0 {
				unregisterWorker(worker)
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

func ProcessMetrics(w http.ResponseWriter, r *http.Request) {

	csvReader := csv.NewReader(r.Body)

	workerAddress := r.URL.Query()["address"][0]

	for {

		line, err := csvReader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatal(err)
		}
		_, err = metricsFile.Write([]byte(fmt.Sprintf("%s, %s, %s, %s, %s\n", workerAddress, line[0], line[1], line[2], line[3])))

		if err != nil {
			fmt.Println("Error writing to file")
			return
		}
	}
	fileStat, err := metricsFile.Stat()
	if err != nil {
		log.Fatal("Error getting logfile stats", err)
		return
	}
	//If file is larger then 10mb post it
	if fileStat.Size() > 10*1000000 {
		postMetric()
	}
}

func postMetric() {
	//TODO get name from config
	err := PostMetrics(metricsFile, "log"+strconv.Itoa(amountLogFiles))
	if err != nil {
		fmt.Println("Error posting metrics", err)
		return
	}
	err = metricsFile.Close()
	if err != nil {
		fmt.Println("Error closing file", err)
		return
	}
	err = os.Remove("metrics/metrics")
	if err != nil {
		fmt.Println("Error removing file", err)
		return
	}
	metricsFile, err = os.Create("metrics/metrics")
	if err != nil {
		fmt.Println("Error opening new metrics file", err)
		return
	}
	amountLogFiles++
}

func forceWriteMetrics(w http.ResponseWriter, r *http.Request) {
	postMetric()
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
