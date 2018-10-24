package metriclogger

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/mem"
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

func ClearMetrics() {
	var err error

	file, err := os.OpenFile(CSVLogFile, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)

	if err != nil {
		log.Fatal("Error clearing log ", err)
	}

	file.Truncate(0)
	file.Seek(0, 0)
}

func LogFloatMetric(workerId string, metric Metric, value float64) {
	LogMetric(workerId, metric, strconv.FormatFloat(value, 'f', 6, 64))
}

func LogUIntMetric(workerId string, metric Metric, value uint64) {
	LogMetric(workerId, metric, strconv.FormatUint(value, 10))
}

func LogMetric(workerId string, metric Metric, value string) {
	var timestamp = int(time.Now().Unix())
	LogMetricWithTimestamp(strconv.Itoa(timestamp), workerId, metric.String(), value)
}

func LogMetricWithTimestamp(timestamp string, workerId string, metric string, value string) {
	file, err := os.OpenFile(CSVLogFile, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)

	if err != nil {
		// TODO: Check if this doesn't happen too often since it kills the master
		// (but also experiments that are useless because they cannot be written to the file.)
		log.Fatal("Unable to write metric to file ", err)
	}

	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	var row = []string{timestamp, workerId, metric, value}
	writer.Write(row)
}

func MonitorResourceUsage() {
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
