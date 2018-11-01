package metriclogger

import (
	"bytes"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/levigross/grequests"
	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/mem"
)

type Measurement struct {
	WorkerID  string
	Metric    Metric
	Value     interface{}
	Timestamp int64
}

type Metric int

const (
	NumRegisteredWorkers Metric = iota
	UsedCPUPercent
	AvailableRAM
	TotalRAM
	NetworkRequest
	RegisteredWorkers
	WorkerQueue
	StartProcessing
	DoneProcessing
)

var MetricChannel = make(chan Measurement)
var ForceWriteChannel = make(chan bool)

func (measurement Measurement) Log() {
	if measurement.Timestamp == 0 {
		measurement.Timestamp = time.Now().Unix()
	}

	MetricChannel <- measurement
}

func (measurement Measurement) ToCSVString() string {
	var ts = strconv.FormatInt(measurement.Timestamp, 10)
	var wid = measurement.WorkerID
	var metricid = strconv.Itoa(int(measurement.Metric))
	var value = fmt.Sprintf("%v", measurement.Value)

	return fmt.Sprintf("%s,%s,%s,%s\n", ts, wid, metricid, value)
}

func (measurement Measurement) Write(f *os.File) {
	_, err := f.Write([]byte(measurement.ToCSVString()))

	if err != nil {
		fmt.Println("Error writing metric to file")
		return
	}
}

func MonitorResourceUsage(identifier string) {
	var initial = true

	for {
		var err error

		cpuPercent, err := cpu.Percent(0, false)
		memstat, err := mem.VirtualMemory()

		if err == nil {
			// Initially we can log some extra system metrics.
			if initial {
				Measurement{identifier, TotalRAM, memstat.Total, 0}.Log()
				initial = false
			}

			Measurement{identifier, UsedCPUPercent, cpuPercent[0], 0}.Log()
			Measurement{identifier, AvailableRAM, memstat.Available, 0}.Log()
		}

		time.Sleep(1 * time.Second)
	}
}

func SendMetrics(masterUrl string, ownUrl string, apiKey string) {
	var LogBuffer = bytes.NewBufferString("")
	var numWritten = 0

	for {
		metric := <-MetricChannel
		LogBuffer.WriteString(metric.ToCSVString())

		if numWritten == 10 {
			requestOptions := grequests.RequestOptions{
				Headers:     map[string]string{"X-Auth": apiKey},
				RequestBody: LogBuffer,
				Params:      map[string]string{"address": ownUrl},
			}
			resp, err := grequests.Post(masterUrl+"/metrics", &requestOptions)
			defer resp.Close()
			if err != nil {
				fmt.Println("Error sending metrics to master.")
			}

			// Clear buffer
			LogBuffer.Reset()
			numWritten = 0
		} else {
			numWritten++
		}
	}
}
