package metriclogger

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"strconv"
	"time"

	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/mem"
)

type Measurement struct {
	WorkerID  string
	Metric    Metric
	Value     interface{}
	Timestamp int64
}

var LogBuffer *bytes.Buffer
var LogWriter *csv.Writer

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

func (measurement Measurement) Log() {
	if measurement.Timestamp == 0 {
		measurement.Timestamp = time.Now().Unix()
	}

	var row = []string{strconv.FormatInt(measurement.Timestamp, 10), measurement.WorkerID, strconv.Itoa(int(measurement.Metric)), fmt.Sprintf("%v", measurement.Value)}
	LogWriter.Write(row)
}

func MonitorResourceUsage(identifier string) {
	var initial = true
	LogBuffer = bytes.NewBufferString("")
	LogWriter = csv.NewWriter(LogBuffer)
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

			//TODO: Automation experiment => Monitor in and outgoing packets?
		}

		time.Sleep(5 * time.Second)
	}
}
