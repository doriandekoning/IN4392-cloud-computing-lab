package main

import (
	"bytes"
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
	Graph      []byte
	ID         uuid.UUID
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
	LogFile    string
}

var metricsFilePath = "/home/ubuntu/metrics/metrics"
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
	metricsFile, err = os.Create(metricsFilePath)
	if err != nil {
		log.Fatal("Error", err)
	}
	defer postMetric()

	router := mux.NewRouter()
	loggingMiddleware := middleware.LoggingMiddleware{InstanceId: "storage"}
	router.Use(loggingMiddleware.Middleware)
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
		if job.ID == processingRequestId {
			worker.TasksProcessing[index] = worker.TasksProcessing[len(worker.TasksProcessing)-1]
			worker.TasksProcessing = worker.TasksProcessing[:len(worker.TasksProcessing)-1]

			// Log stoptime for job with ID so we can measure its processing time.
			metriclogger.Measurement{"master", metriclogger.DoneProcessing, job.ID, 0}.Log()
			break
		}
	}

}

func ProcessGraph(w http.ResponseWriter, r *http.Request) {
	requestID := uuid.Must(uuid.NewV4())
	//Asynchronously distribute the graph
	graph, err := ioutil.ReadAll(r.Body)
	if err != nil {
		util.BadRequest(w, "Error reading file from request", err)
	}

	size, err := strconv.Atoi(r.URL.Query().Get("size"))
	if err != nil {
		util.BadRequest(w, "Size is not a valid number: "+r.URL.Query().Get("size"), nil)
		return
	}

	if (size * size) > len(graph) {
		util.BadRequest(w, fmt.Sprintf("File size too small, actual: %d, expected: %d ", len(graph), (size*size)), nil)
		return
	}
	task := task{Graph: graph, Parameters: r.URL.Query(), ID: requestID}
	go distributeGraph(task)

	//Write id to response
	idBytes, _ := requestID.MarshalText()
	w.WriteHeader(http.StatusAccepted)
	w.Write(idBytes)
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
		distributeGraph(task)
	}

	// Log the number of registered workers after deregistering a worker.
	metriclogger.Measurement{"master", metriclogger.RegisteredWorkers, len(workers), 0}.Log()
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
	newNode.Active = true
	var replaced bool
	for nodeIndex, node := range *nodesOfType {
		if newNode.Address == node.Address {
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

func distributeGraph(task task) {
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
		task.Parameters["requestID"] = []string{task.ID.String()}
		err := sendGraphToWorker(task, worker)
		if err == nil {
			worker.TasksProcessing = append(worker.TasksProcessing, task)
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

func sendGraphToWorker(task task, worker *node) error {
	options := grequests.RequestOptions{
		RequestBody: bytes.NewReader(task.Graph),
		Headers:     map[string]string{"Content-Type": "application/json", "X-Auth": config.ApiKey},
		Params:      paramsMapToRequestParamsMap(task.Parameters),
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
		if len(workers) == 0 {
			StartNewWorker()
			continue
		} else if len(workers) == 1 || len(workers) > config.MaxWorkers {
			continue
		}

		//Check scaling up
		const queueSizeThreshold = 2
		var inactiveHealthyWorkers = workers.filter(false, true)
		leastBusyWorker := getLeastBusyWorker()
		if leastBusyWorker == nil {
			continue
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
	err := PostMetrics(metricsFilePath, config.LogFile+strconv.Itoa(amountLogFiles))
	if err != nil {
		fmt.Println("Error posting metrics", err)
		return
	}
	err = metricsFile.Close()
	if err != nil {
		fmt.Println("Error closing file", err)
		return
	}
	err = os.Remove(metricsFilePath)
	if err != nil {
		fmt.Println("Error removing file", err)
		return
	}
	metricsFile, err = os.Create(metricsFilePath)
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
