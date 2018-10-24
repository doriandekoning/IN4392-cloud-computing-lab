package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

var CSVLogPath = "metrics"
var CSVFileName = fmt.Sprintf("metric_%d.csv", int32(time.Now().Unix()))

var CSVLogFile = filepath.Join(CSVLogPath, CSVFileName)

type Metric int

const (
	NumRegisteredWorkers Metric = iota
	UsedCPUPercent
	AvailableRAM
	TotalRAM
)

func (s Metric) String() string {
	return [...]string{"workers_num_registered", "cpu_used_pct", "ram_available", "ram_total"}[s]
}

func CreateMetricFolder() {
	// TODO: Check where we can store this file.
	var err = os.MkdirAll(CSVLogPath, os.ModePerm)

	if err != nil {
		log.Fatal("Error creating metric folder", err)
	}
}

func LogFloatMetric(workerId string, metric Metric, value float64) {
	LogMetric(workerId, metric, strconv.FormatFloat(value, 'f', 6, 64))
}

func LogUIntMetric(workerId string, metric Metric, value uint64) {
	LogMetric(workerId, metric, strconv.FormatUint(value, 10))
}

func LogMetric(workerId string, metric Metric, value string) {
	var timestamp = int(time.Now().Unix())

	file, err := os.OpenFile(CSVLogFile, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)

	if err != nil {
		// TODO: Check if this doesn't happen too often since it kills the master
		// (but also experiments that are useless because they cannot be written to the file.)
		log.Fatal("Unable to write metric to file ", err)
	}

	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	var row = []string{strconv.Itoa(timestamp), workerId, metric.String(), value}
	writer.Write(row)
}
