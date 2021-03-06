/*
http://www.apache.org/licenses/LICENSE-2.0.txt

Copyright 2015 Intel Corporation

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package statistics

import (
	"bytes"
	"encoding/gob"
	"errors"
	log "github.com/Sirupsen/logrus"

	"github.com/intelsdi-x/snap/control/plugin"
	"github.com/intelsdi-x/snap/control/plugin/cpolicy"
	"github.com/intelsdi-x/snap/core/ctypes"
	"github.com/montanaflynn/stats"
)

const (
	pluginName = "statistics"
	version    = 1
	pluginType = plugin.ProcessorPluginType
)

type Plugin struct {
	buffer        map[string][]interface{}
	bufferMaxSize int
	bufferCurSize int
	bufferIndex   int
}

// Meta returns a plugin meta data
func Meta() *plugin.PluginMeta {
	return plugin.NewPluginMeta(
		pluginName,
		version,
		pluginType,
		[]string{plugin.SnapGOBContentType},
		[]string{plugin.SnapGOBContentType})
}

func New() *Plugin {
	buffer := make(map[string][]interface{})
	p := &Plugin{buffer: buffer,
		bufferMaxSize: 100,
		bufferCurSize: 0,
		bufferIndex:   0}
	return p
}

func (p *Plugin) calculateStats(buff interface{}, logger *log.Logger) (map[string]float64, error) {
	result := make(map[string]float64)

	var buffer []float64

	logger.Printf("Buff %v", buff)

	for _, val := range buff.([]interface{}) {
		switch v := val.(type) {
		default:
			logger.Printf("Unknown data received: Type %T", v)
			return nil, errors.New("Unknown data received: Type")
		case int:
			buffer = append(buffer, float64(val.(int)))
		case int32:
			buffer = append(buffer, float64(val.(int32)))
		case int64:
			buffer = append(buffer, float64(val.(int64)))
		case float64:
			buffer = append(buffer, val.(float64))
		case float32:
			buffer = append(buffer, float64(val.(float32)))
		case uint64:
			buffer = append(buffer, float64(val.(uint64)))
		case uint32:
			buffer = append(buffer, float64(val.(uint32)))
		}
	}

	logger.Printf("Buffer %v", buffer)

	val, err := stats.Mean(buffer)
	if err != nil {
		return nil, err
	}

	result["mean"] = val

	val, err = stats.Median(buffer)
	if err != nil {
		return nil, err
	}

	result["median"] = val

	val, err = stats.StandardDeviation(buffer)
	if err != nil {
		return nil, err
	}

	result["stddev"] = val

	val, err = stats.Variance(buffer)
	if err != nil {
		return nil, err
	}

	result["var"] = val

	val, err = stats.Percentile(buffer, 95)
	if err != nil {
		return nil, err
	}

	result["95%-ile"] = val

	val, err = stats.Percentile(buffer, 99)
	if err != nil {
		return nil, err
	}

	result["99%-ile"] = val

	return result, nil
}

func concatNameSpace(namespace []string) string {
	completeNamespace := ""
	for i := 0; i < len(namespace); i++ {
		completeNamespace += namespace[i]

	}
	return completeNamespace

}

func (p *Plugin) insertInToBuffer(val interface{}, ns []string) {

	if p.bufferCurSize == 0 {
		var buff = make([]interface{}, p.bufferMaxSize)
		buff[0] = val
		p.buffer[concatNameSpace(ns)] = buff
	} else {
		p.buffer[concatNameSpace(ns)][p.bufferIndex] = val
	}
}

func (p *Plugin) updateCounters() {
	if p.bufferCurSize < p.bufferMaxSize {
		p.bufferCurSize++
	}

	if p.bufferIndex == p.bufferMaxSize-1 {
		p.bufferIndex = 0
	} else {
		p.bufferIndex++
	}
}

func (p *Plugin) GetConfigPolicy() (*cpolicy.ConfigPolicy, error) {
	cp := cpolicy.New()
	config := cpolicy.NewPolicyNode()

	r1, err := cpolicy.NewIntegerRule("SlidingWindowLength", true)
	if err != nil {
		return nil, err
	}

	r1.Description = "Length for sliding window"
	config.Add(r1)
	cp.Add([]string{""}, config)

	return cp, nil
}

func (p *Plugin) Process(contentType string, content []byte, config map[string]ctypes.ConfigValue) (string, []byte, error) {

	logger := log.New()
	logger.Println("Statistics Processor started")

	var metrics []plugin.MetricType

	if config != nil {
		if config["SlidingWindowLength"].(ctypes.ConfigValueInt).Value > 0 {
			p.bufferMaxSize = config["SlidingWindowLength"].(ctypes.ConfigValueInt).Value
		} else {
			p.bufferMaxSize = 100
		}
	} else {
		p.bufferMaxSize = 100
	}

	//Decodes the content into PluginMetricType
	dec := gob.NewDecoder(bytes.NewBuffer(content))
	if err := dec.Decode(&metrics); err != nil {
		logger.Printf("Error decoding: error=%v content=%v", err, content)
		return "", nil, err
	}

	for i, _ := range metrics {
		logger.Printf("Data received %v", metrics[i].Data())
		p.insertInToBuffer(metrics[i].Data(), metrics[i].Namespace().Strings())
	}

	p.updateCounters()

	for i, _ := range metrics {
		if p.bufferCurSize < p.bufferMaxSize {
			metrics[i].Data_, _ = p.calculateStats(p.buffer[concatNameSpace(metrics[i].Namespace().Strings())][0:p.bufferCurSize], logger)
		} else {
			metrics[i].Data_, _ = p.calculateStats(p.buffer[concatNameSpace(metrics[i].Namespace().Strings())], logger)
		}

		logger.Printf("Statistics %v", metrics[i].Data())
	}

	gob.Register(map[string]float64{})
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(metrics); err != nil {
		return "", nil, err
	}

	return contentType, buf.Bytes(), nil
}
