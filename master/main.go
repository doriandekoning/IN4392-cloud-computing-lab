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
	Address    string
	InstanceId string
	Healty     bool
	Active     bool
}

var workers []*worker

var Sess *session.Session

const maxWorkers = 3
const minWorkers = 1

var requestsSinceScaling = 0

func main() {

	var err error

	CreateMetricFolder()
	go monitorResourceUsage()

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
	router.HandleFunc("/worker/unregister", unregisterWorker).Methods("DELETE")

	go scaleWorkers()
	go getWorkersHealth()

	log.Fatal(http.ListenAndServe(":8000", router))

}

func GetHealth(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "Registered workers: "+strconv.Itoa(len(workers))+"  Requests in the last minute: "+strconv.Itoa(requestsSinceScaling))
	for _, worker := range workers {
		fmt.Fprintln(w, worker.Address+" -- "+worker.InstanceId+" -- "+strconv.FormatBool(worker.Healty))
	}
}

func KillWorkersRequest(w http.ResponseWriter, r *http.Request) {
	err := TerminateWorkers(workers)
	if err != nil {
		util.InternalServerError(w, "Workers could be terminated", err)
		return
	}

	workers = nil
	fmt.Println("Workers terminated")
}

func AddWorkerRequest(w http.ResponseWriter, r *http.Request) {
	StartNewWorker()
	fmt.Println("New worker started")
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
	fmt.Println("Worker: " + newWorker.InstanceId + " successfully registered!")
}

func unregisterWorker(w http.ResponseWriter, r *http.Request) {
	var oldWorker worker
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
	if oldWorker.InstanceId != "" {
		TerminateWorkers([]*worker{&oldWorker})
	}
	fmt.Println("Worker: " + oldWorker.Address + " successfully unregistered!")
}

func distributeGraph(graph *graphs.Graph, parameters map[string][]string) {
	// Distribute graph among workers
	var workerID int
	//TODO determine to which worker to send node (for now to first free worker)
	//TODO only consider workers that are active/healty and what if there are no workers
	err := sendGraphToWorker(*graph, workers[workerID], parameters)
	if err != nil {
		fmt.Println("Cannot distributes graph to: " + workers[workerID].Address)
	}
}

func sendGraphToWorker(graph graphs.Graph, worker *worker, parameters map[string][]string) error {
	options := grequests.RequestOptions{
		JSON:    graph,
		Headers: map[string]string{"Content-Type": "application/json"},
		Params:  paramsMapToRequestParamsMap(parameters),
	}
	resp, err := grequests.Post(worker.Address+"/graph", &options)
	if err != nil {
		return err
	}
	resp.Close()
	return nil
}

func getWorkersHealth() {
	for {
		for _, worker := range workers {
			resp, err := grequests.Get(worker.Address+"/health", nil)
			if err != nil {
				worker.Healty = false
			} else {
				resp.Close()
				worker.Healty = true
			}
		}
		time.Sleep(15 * time.Second)
	}
}

func scaleWorkers() {
	for {
		if len(workers) < minWorkers || (len(workers) < maxWorkers && (requestsSinceScaling/len(workers)) > 3) {
			StartNewWorker()
		} else if len(workers) > minWorkers && (requestsSinceScaling/len(workers)) < 2 {
			worker := workers[0]
			// Set active to false to stop using this worker
			worker.Active = false
			resp, err := grequests.Post(worker.Address+"/unregister", nil)
			if err != nil {
				fmt.Println("Unable to request to unregister, error:", err)
				return
			}
			resp.Close()
			fmt.Println("Sucessfully did a request to unregister")
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

func paramsMapToRequestParamsMap(original map[string][]string) map[string]string {
	retval := map[string]string{}
	for k, v := range original {
		retval[k] = v[0]
	}
	return retval
}
