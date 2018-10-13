package main

import (
	"fmt"
	"log"
	"strconv"
	"time"

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

var conf Config

func main() {
	err := envconfig.Init(&conf)
	if err != nil {
		log.Fatal(err)
	}

	register()
	time.Sleep(5 * time.Second)
	defer unregister()
}

func register() {
	options := grequests.RequestOptions{
		JSON: struct {
			address string
		}{
			conf.Own.Address + ":" + strconv.Itoa(conf.Own.Port),
		},
	}
	_, err := grequests.Post(getMasterURL()+"/worker/register", &options)
	if err != nil {
		log.Fatal("Unable to register", err)
	}
	fmt.Println("Successfully registered")
}

func unregister() {
	options := grequests.RequestOptions{
		JSON: struct {
			address string
		}{
			getOwnURL(),
		},
	}
	_, err := grequests.Delete(getMasterURL()+"/worker/unregister", &options)
	if err != nil {
		log.Fatal("Unable to unregister: ", err)
	}
	fmt.Println("Sucessfully unregistered")

}

func getMasterURL() string {
	return conf.Master.Address + ":" + strconv.Itoa(conf.Master.Port)
}

func getOwnURL() string {
	return conf.Own.Address + ":" + strconv.Itoa(conf.Own.Port)
}
