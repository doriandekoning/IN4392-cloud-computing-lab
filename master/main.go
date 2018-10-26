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

var g graphs.Graph

type task struct {
	Graph      *graphs.Graph
	Parameters map[string][]string
}

type worker struct {
	Address               string
	InstanceId            string
	LastResponseTimestamp int64
	Healty                bool
	TasksProcessing       []task
}

var workers []*worker

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
	router.HandleFunc("/worker/register", registerWorker).Methods("POST")
	router.HandleFunc("/worker/unregister", unregisterWorkerRequest).Methods("DELETE")

	go scaleWorkers()
	go getWorkersHealth()
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
		Workers              []*worker
	}{
		MaxWorkers:           config.MaxWorkers,
		MinWorkers:           minWorkers,
		RequestsSinceScaling: requestsSinceScaling,
		ActiveWorkers:        len(getActiveWorkers()),
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

	if len(getActiveWorkers()) == 0 {
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
	var newWorker worker

	b, _ := ioutil.ReadAll(r.Body)
	err := json.Unmarshal(b, &newWorker)
	if err != nil {
		util.BadRequest(w, "Error unmarshaling body", err)
		return
	}
	newWorker.LastResponseTimestamp = time.Now().Unix()
	workers = append(workers, &newWorker)
	util.GeneralResponse(w, true, "Worker: "+newWorker.InstanceId+" successfully registered!")
}

func unregisterWorkerRequest(w http.ResponseWriter, r *http.Request) {
	var oldWorker worker
	bodyBytes, _ := ioutil.ReadAll(r.Body)
	err := json.Unmarshal(bodyBytes, &oldWorker)
	if err != nil {
		util.BadRequest(w, "Error unmarshalling body", err)
		return
	}
	unregisterWorker(&oldWorker)
	util.GeneralResponse(w, true, "Worker: "+oldWorker.InstanceId+" successfully unregistered!")
}

func unregisterWorker(oldWorker *worker) {
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
		TerminateWorkers([]*worker{oldWorker})
	}
	//Redistribute graphs this worker was still processing
	for _, task := range oldWorker.TasksProcessing {
		distributeGraph(task.Graph, task.Parameters)
	}
}

func distributeGraph(graph *graphs.Graph, parameters map[string][]string) {
	var activeWorkers = getActiveWorkers()
	for {
		// Distribute graph among workers by randomly selecting a worker
		// possible improvement select the worker which has the shortest queue
		var worker = activeWorkers[rand.Intn(len(activeWorkers))]
		err := sendGraphToWorker(*graph, worker, parameters)
		if err == nil {
			// TODO remove from this list again when processing is done by worker
			worker.TasksProcessing = append(worker.TasksProcessing, task{graph, parameters})
			break
		}
		fmt.Println("Cannot distributes graph to: " + worker.Address)
		//Try again in 10 sec
		time.Sleep(10 * time.Second)
	}
}

func sendGraphToWorker(graph graphs.Graph, worker *worker, parameters map[string][]string) error {
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

func getWorkersHealth() {
	requestOptions := grequests.RequestOptions{Headers: map[string]string{"X-Auth": config.ApiKey}}
	for {
		for _, worker := range workers {
			resp, err := grequests.Get(worker.Address+"/health", &requestOptions)

			if err != nil {
				worker.Healty = false
			} else {
				defer resp.Close()
				worker.Healty = true
				worker.LastResponseTimestamp = time.Now().Unix()
			}
			if time.Now().Unix()-worker.LastResponseTimestamp > 60 {
				unregisterWorker(worker)
			}
		}
		time.Sleep(30 * time.Second)
	}
}

func scaleWorkers() {
	for {
		time.Sleep(60 * time.Second)

		var activeWorkers = getActiveWorkers()
		if len(activeWorkers) < minWorkers || (len(activeWorkers) < config.MaxWorkers && (requestsSinceScaling/len(activeWorkers)) > 3) {
			StartNewWorker()
		} else if len(activeWorkers) > minWorkers && (requestsSinceScaling/len(activeWorkers)) < 2 {
			worker := activeWorkers[0]
			unregisterWorker(worker)
		}
		requestsSinceScaling = 0
	}
}

func getActiveWorkers() []*worker {
	var result []*worker
	for _, worker := range workers {
		if worker.Healty {
			result = append(result, worker)
		}
	}
	return result
}

func paramsMapToRequestParamsMap(original map[string][]string) map[string]string {
	retval := map[string]string{}
	for k, v := range original {
		retval[k] = v[0]
	}
	return retval
}
