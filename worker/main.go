package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/gorilla/mux"
	"github.com/levigross/grequests"
	"github.com/vrischmann/envconfig"
)

//Config is the main
type Config struct {
	Master struct {
		Port    int
		Address string
	}
	Own struct {
		Port    int
		Address string
	}
}

type worker struct {
	Address string
	Healty  bool
}

var conf Config
var subGraph Graph

func main() {
	err := envconfig.Init(&conf)
	if err != nil {
		log.Fatal(err)
	}

	router := mux.NewRouter()
	router.Use(loggingMiddleWare)
	router.HandleFunc("/health", GetHealth)
	router.HandleFunc("/subgraph", ReceiveSubgraph).Methods("POST")
	router.HandleFunc("/message", ReceiveMessage).Methods("POST")

	register()
	go checkMasterHealth()
	log.Fatal(http.ListenAndServe(":"+strconv.Itoa(conf.Own.Port), router))
	defer unregister()
}

func GetHealth(w http.ResponseWriter, r *http.Request) {

}

func loggingMiddleWare(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Println("[" + r.RequestURI + "]")
		next.ServeHTTP(w, r)
	})
}

func register() {
	for {
		options := grequests.RequestOptions{
			JSON:    map[string]string{"address": getOwnURL()},
			Headers: map[string]string{"Content-Type": "application/json"},
		}
		_, err := grequests.Post(getMasterURL()+"/worker/register", &options)
		if err == nil {
			fmt.Println("Successfully registered")
			break
		}
		fmt.Println("Unable to register", err)
		//Try again in 10 sec
		time.Sleep(10 * time.Second)
	}
}

func unregister() {
	options := grequests.RequestOptions{
		JSON:    map[string]string{"address": getOwnURL()},
		Headers: map[string]string{"Content-Type": "application/json"},
	}
	spew.Dump(options)
	_, err := grequests.Delete(getMasterURL()+"/worker/unregister", &options)
	if err != nil {
		log.Fatal("Unable to unregister: ", err)
	}
	fmt.Println("Sucessfully unregistered")

}

func getSubProblem(w http.ResponseWriter, r *http.Request) {
	bodyBytes, _ := ioutil.ReadAll(r.Body)
	type payload struct {
		workers []worker
		nodes   []Node
	}
	actualPayload := payload{}
	json.Unmarshal(bodyBytes, &actualPayload)
}

func getMasterURL() string {
	return conf.Master.Address + ":" + strconv.Itoa(conf.Master.Port)
}

func getOwnURL() string {
	return conf.Own.Address + ":" + strconv.Itoa(conf.Own.Port)
}

func ReceiveSubgraph(w http.ResponseWriter, r *http.Request) {
	b, _ := ioutil.ReadAll(r.Body)
	err := json.Unmarshal(b, &subGraph)
	if err != nil {
		log.Fatal("Error unmashalling subgraph", err)
	}
	fmt.Println("Received a subraph with:", len(subGraph.Nodes), " nodes")

}

func checkMasterHealth() {
	for {
		_, err := grequests.Get(getMasterURL()+"/health", nil)
		if err != nil {
			fmt.Println("Master seems to be offline")
			register()
		} else {
			fmt.Println("Master seems healty")
		}
		time.Sleep(10 * time.Second)
	}
}

func ReceiveMessage(w http.ResponseWriter, r *http.Request) {

	var message = Message{}
	b, _ := ioutil.ReadAll(r.Body)
	err := json.Unmarshal(b, &message)
	if err != nil {
		log.Fatal("Received bad message: ", err)
	}
	// Add message in the message queue of the corresponding edge
	// TODO improve datastructures keeping messages (maybe a 2d map [sender][receiver] with an array of messages? so we can do this in constant time?)
	for _, node := range subGraph.Nodes {
		if node.Id == message.To {
			node.ReceiveMessage(message)
			break
		}
	}

}
