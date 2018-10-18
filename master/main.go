package main

import (
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/gorilla/mux"
	"github.com/levigross/grequests"
)

var g Graph

type worker struct {
	Address string
	Healty  bool
}

var workers []*worker

var sess *session.Session
var workerStartupScript string
var logs []string

func main() {

	var err error

	sess, err = session.NewSession(&aws.Config{
		Region:      aws.String("us-east-1"),
		Credentials: credentials.NewSharedCredentials("", "IN4392"),
	})

	if err != nil {
		log.Fatal("Error", err)
	}

	workerStartupScript, err = readUsserDataScriptFileAndEncode()
	if err != nil {
		log.Println(err)
	}

	// TODO only start worker when there is a request? or dynamically start and stop them
	startNewWorker()

	router := mux.NewRouter()
	router.Use(loggingMiddleWare)
	router.HandleFunc("/health", GetHealth).Methods("GET")
	router.HandleFunc("/processgraph", ProcessGraph).Methods("POST")
	router.HandleFunc("/worker/register", registerWorker).Methods("POST")
	router.HandleFunc("/worker/unregister", unregisterWorker).Methods("DELETE")

	go getWorkersHealth()
	log.Fatal(http.ListenAndServe(":8000", router))

}

func startNewWorker() {

	// Create new EC2 client
	svc := ec2.New(sess)

	runResult, err := svc.RunInstances(&ec2.RunInstancesInput{
		ImageId:      aws.String("ami-07d3c94f64ec71332"),
		InstanceType: aws.String("t2.micro"),
		MinCount:     aws.Int64(1),
		MaxCount:     aws.Int64(1),
		UserData:     aws.String(workerStartupScript),
	})

	if err != nil {
		log.Println("Could not create instance", err)
		return
	} else {
		log.Println(runResult)
	}
}

func readUsserDataScriptFileAndEncode() (string, error) {
	f, err := ioutil.ReadFile("user_data.sh")
	if err != nil {
		return "", err
	}
	userDataScript := base64.URLEncoding.EncodeToString(f)
	return userDataScript, nil
}

func GetHealth(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, strings.Join(logs, "\n"))
}

func loggingMiddleWare(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Println("[" + r.RequestURI + "]")
		logs = append(logs, "["+r.RequestURI+"]")
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
