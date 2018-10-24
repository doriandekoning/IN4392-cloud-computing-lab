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
	"os"
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
	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/mem"
)

var g graphs.Graph

type worker struct {
	Address              string
	InstanceId           string
	SecondsNotResponding int
	Healty               bool
	Active               bool
}

type HealthResponse struct {
	MaxWorkers           int
	MinWorkers           int
	RequestsSinceScaling int
	ActiveWorkers        int
	Workers              []*worker
}

var workers []*worker

var Sess *session.Session

var maxWorkers int

const minWorkers = 1

var requestsSinceScaling = 0

func main() {

	var err error

	CreateMetricFolder()
	go monitorResourceUsage()

	maxWorkers, err = strconv.Atoi(os.Getenv("MAXWORKERS"))

  Sess, err = session.NewSession(&aws.Config{
		Region: aws.String("us-east-1"),
	})

	if err != nil {
		log.Fatal("Error", err)
	}

	router := mux.NewRouter()
	router.Use(middleware.LoggingMiddleWare)
	router.HandleFunc("/health", GetHealth).Methods("GET")
	router.HandleFunc("/killworkers", KillWorkersRequest).Methods("GET")
	router.HandleFunc("/addworker", AddWorkerRequest).Methods("GET")
	router.HandleFunc("/processgraph", ProcessGraph).Methods("POST")
	router.HandleFunc("/worker/register", registerWorker).Methods("POST")
	router.HandleFunc("/worker/unregister", unregisterWorkerRequest).Methods("DELETE")

	go scaleWorkers()
	go getWorkersHealth()

	log.Fatal(http.ListenAndServe(":8000", router))

}

func GetHealth(w http.ResponseWriter, r *http.Request) {
	var response = HealthResponse{MaxWorkers: maxWorkers, MinWorkers: minWorkers, RequestsSinceScaling: requestsSinceScaling, ActiveWorkers: len(getActiveWorkers()), Workers: workers}
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

	if len(workers) == 0 {
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
	newWorker.Active = true
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
}

func distributeGraph(graph *graphs.Graph, parameters map[string][]string) {
	var activeWorkers = getActiveWorkers()
	// Distribute graph among workers by randomly selecting a worker
	// possible improvement select the worker which has the shortest queue
	var worker = activeWorkers[rand.Intn(len(activeWorkers))]
	err := sendGraphToWorker(*graph, worker, parameters)
	if err != nil {
		fmt.Println("Cannot distributes graph to: " + worker.Address)
	}
}

func sendGraphToWorker(graph graphs.Graph, worker *worker, parameters map[string][]string) error {
	options := grequests.RequestOptions{
		JSON:    graph,
		Headers: map[string]string{"Content-Type": "application/json"},
		Params:  paramsMapToRequestParamsMap(parameters),
	}
	resp, err := grequests.Post(worker.Address+"/graph", &options)
	defer resp.Close()
	if err != nil {
		return err
	}

	return nil
}

func getWorkersHealth() {
	const healthCheckInterval = 30 //seconds
	for {
		for _, worker := range workers {
			resp, err := grequests.Get(worker.Address+"/health", nil)
			defer resp.Close()
			if err != nil {
				worker.Healty = false
				worker.SecondsNotResponding = worker.SecondsNotResponding + healthCheckInterval
			} else {
				worker.Healty = true
				worker.SecondsNotResponding = 0
			}
			if worker.SecondsNotResponding > 60 {
				unregisterWorker(worker)
			}
		}
		time.Sleep(healthCheckInterval * time.Second)
	}
}

func scaleWorkers() {
	for {
		var activeWorkers = getActiveWorkers()
		if len(activeWorkers) < minWorkers || (len(activeWorkers) < maxWorkers && (requestsSinceScaling/len(activeWorkers)) > 3) {
			StartNewWorker()
		} else if len(activeWorkers) > minWorkers && (requestsSinceScaling/len(activeWorkers)) < 2 {
			worker := activeWorkers[0]
			// Set active to false to stop using this worker
			worker.Active = false
			resp, err := grequests.Post(worker.Address+"/unregister", nil)
			defer resp.Close()
			if err != nil {
				return
			}
		}
		requestsSinceScaling = 0
		time.Sleep(60 * time.Second)
	}
}

func monitorResourceUsage() {
	// TODO: This should also be implemented for the workers, which should send log this data once every 5 (?) seconds and send it once every minute (?) to the master.
	var initial = true
	for {
		var err error

		cpuPercent, err := cpu.Percent(0, false)
		memstat, err := mem.VirtualMemory()

		if err == nil {
			// Initially we can log some extra system metrics.
			if initial {
				LogUIntMetric("master", TotalRAM, memstat.Total)
				initial = false
			}

			LogFloatMetric("master", UsedCPUPercent, cpuPercent[0])
			LogUIntMetric("master", AvailableRAM, memstat.Available)

			//TODO: Automation experiment => Monitor in and outgoing packets?
		}

		time.Sleep(5 * time.Second)
	}
}

func getActiveWorkers() []*worker {
	var result []*worker
	for _, worker := range workers {
		if worker.Active && worker.Healty {
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
