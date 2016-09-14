/*
http://www.apache.org/licenses/LICENSE-2.0.txt

Copyright 2016 Intel Corporation

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
	"fmt"
	"math"
	"reflect"
	"strings"

	log "github.com/Sirupsen/logrus"
	"github.com/intelsdi-x/snap/control/plugin"
	"github.com/intelsdi-x/snap/control/plugin/cpolicy"
	"github.com/intelsdi-x/snap/core"
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

// New() returns a new instance of this
func New() *Plugin {
	buffer := make(map[string][]interface{})
	p := &Plugin{buffer: buffer,
		bufferMaxSize: 100,
		bufferCurSize: 0,
		bufferIndex:   0}
	return p
}

// calculateStats calaculates the descriptive statistics for buff
func (p *Plugin) calculateStats(buff interface{}, m plugin.MetricType) ([]plugin.MetricType, error) {
	//result := make(map[string][]float64)
	result := make([]plugin.MetricType, 15)
	var buffer []float64

	time := m.Timestamp()
	tag := m.Tags()
	lastTime := m.LastAdvertisedTime()
	unit := m.Unit()
	ns := strings.Join(m.Namespace().Strings(), " ")

	//Need to change so it ranges over the current size of the buffer and not the capacity
	for _, val := range buff.([]interface{}) {
		switch v := val.(type) {
		default:
			log.Printf("Unknown data received: Type %T", v)
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

	count := plugin.MetricType{
		Data_:               float64(len(buffer)),
		Namespace_:          core.NewNamespace(ns, "Count"),
		Timestamp_:          time,
		LastAdvertisedTime_: lastTime,
		Unit_:               unit,
		Tags_:               tag,
	}
	result[0] = count

	val, err := stats.Mean(buffer)
	if err != nil {
		log.Println("Error in mean")
		return nil, err
	}
	mean := plugin.MetricType{
		Data_:               val,
		Namespace_:          core.NewNamespace(ns, "Mean"),
		Timestamp_:          time,
		LastAdvertisedTime_: lastTime,
		Unit_:               unit,
		Tags_:               tag,
	}
	result[1] = mean

	val, err = stats.Median(buffer)
	if err != nil {
		log.Println("Error in Median")
		return nil, err
	}
	median := plugin.MetricType{
		Data_:               val,
		Namespace_:          core.NewNamespace(ns, "Median"),
		Timestamp_:          time,
		LastAdvertisedTime_: lastTime,
		Unit_:               unit,
		Tags_:               tag,
	}
	result[2] = median

	val, err = stats.StandardDeviation(buffer)
	if err != nil {
		log.Println("Error in Standard Dev.")
		return nil, err
	}
	standarddev := plugin.MetricType{
		Data_:               val,
		Namespace_:          core.NewNamespace(ns, "Standard Deviation"),
		Timestamp_:          time,
		LastAdvertisedTime_: lastTime,
		Unit_:               unit,
		Tags_:               tag,
	}
	result[3] = standarddev

	val, err = stats.Variance(buffer)
	if err != nil {
		log.Println("Error in Variance")
		return nil, err
	}
	variance := plugin.MetricType{
		Data_:               val,
		Namespace_:          core.NewNamespace(ns, "Variance"),
		Timestamp_:          time,
		LastAdvertisedTime_: lastTime,
		Unit_:               unit,
		Tags_:               tag,
	}
	result[4] = variance

	val, err = stats.Percentile(buffer, 95)
	if err != nil {
		log.Printf("Error in 95%%")
		return nil, err
	}
	pct95 := plugin.MetricType{
		Data_:               val,
		Namespace_:          core.NewNamespace(ns, "95%-ile"),
		Timestamp_:          time,
		LastAdvertisedTime_: lastTime,
		Unit_:               unit,
		Tags_:               tag,
	}
	result[5] = pct95

	val, err = stats.Percentile(buffer, 99)
	if err != nil {
		log.Printf("Error in 99%%")
		return nil, err
	}
	pct99 := plugin.MetricType{
		Data_:               val,
		Namespace_:          core.NewNamespace(ns, "99%-ile"),
		Timestamp_:          time,
		LastAdvertisedTime_: lastTime,
		Unit_:               unit,
		Tags_:               tag,
	}
	result[6] = pct99

	val, err = stats.Min(buffer)
	if err != nil {
		log.Println("Error in min")
		return nil, err
	}
	min := plugin.MetricType{
		Data_:               val,
		Namespace_:          core.NewNamespace(ns, "minimum"),
		Timestamp_:          time,
		LastAdvertisedTime_: lastTime,
		Unit_:               unit,
		Tags_:               tag,
	}
	result[7] = min

	minval := val

	val, err = stats.Max(buffer)
	if err != nil {
		log.Println("Error in max")
		return nil, err
	}
	max := plugin.MetricType{
		Data_:               val,
		Namespace_:          core.NewNamespace(ns, "maximum"),
		Timestamp_:          time,
		LastAdvertisedTime_: lastTime,
		Unit_:               unit,
		Tags_:               tag,
	}
	result[8] = max

	rangeval := plugin.MetricType{
		Data_:               val - minval,
		Namespace_:          core.NewNamespace(ns, "rangeval"),
		Timestamp_:          time,
		LastAdvertisedTime_: lastTime,
		Unit_:               unit,
		Tags_:               tag,
	}
	result[9] = rangeval

	var valArr []float64
	valArr, err = stats.Mode(buffer)
	if err != nil {
		log.Println("Error in mode")
		return nil, err
	}
	modeval := plugin.MetricType{
		Data_:               valArr,
		Namespace_:          core.NewNamespace(ns, "mode"),
		Timestamp_:          time,
		LastAdvertisedTime_: lastTime,
		Unit_:               unit,
		Tags_:               tag,
	}
	result[10] = modeval

	val, err = stats.Sum(buffer)
	if err != nil {
		log.Println("Error in sum")
		return nil, err
	}
	sumval := plugin.MetricType{
		Data_:               val,
		Namespace_:          core.NewNamespace(ns, "sum"),
		Timestamp_:          time,
		LastAdvertisedTime_: lastTime,
		Unit_:               unit,
		Tags_:               tag,
	}
	result[11] = sumval

	kurtosis := plugin.MetricType{
		Data_:               p.Kurtosis(buffer),
		Namespace_:          core.NewNamespace(ns, "kurtosis"),
		Timestamp_:          time,
		LastAdvertisedTime_: lastTime,
		Unit_:               unit,
		Tags_:               tag,
	}
	result[12] = kurtosis

	skewness := plugin.MetricType{
		Data_:               p.Skewness(buffer),
		Namespace_:          core.NewNamespace(ns, "skewness"),
		Timestamp_:          time,
		LastAdvertisedTime_: lastTime,
		Unit_:               unit,
		Tags_:               tag,
	}
	result[13] = skewness

	val, err = stats.Trimean(buffer)
	if err != nil {
		log.Println("Error in trimean")
		return nil, err
	}
	trimean := plugin.MetricType{
		Data_:               val,
		Namespace_:          core.NewNamespace(ns, "trimean"),
		Timestamp_:          time,
		LastAdvertisedTime_: lastTime,
		Unit_:               unit,
		Tags_:               tag,
	}
	result[14] = trimean

	return result, nil
}

//Calculates the population skewness from buffer
func (p *Plugin) Skewness(buffer []float64) []float64 {
	if len(buffer) == 0 {
		log.Printf("Buffer does not contain any data.")
		return []float64{}
	}
	var skew float64
	var mean float64
	var stdev float64

	mean, err := stats.Mean(buffer)
	if err != nil {
		log.Fatal(err)
	}
	stdev, err = stats.StandardDeviation(buffer)
	if err != nil {
		log.Fatal(err)
	}

	for _, val := range buffer {
		skew += math.Pow((val-mean)/stdev, 3)
	}

	return []float64{(1 / float64(len(buffer)) * skew)}

}

//Calculates the population kurtosis from buffer
func (p *Plugin) Kurtosis(buffer []float64) []float64 {
	if len(buffer) == 0 {
		log.Printf("Buffer does not contain any data.")
		return []float64{}
	}
	var kurt float64
	var stdev float64
	var mean float64

	mean, err := stats.Mean(buffer)
	if err != nil {
		log.Fatal(err)
	}

	stdev, err = stats.StandardDeviation(buffer)
	if err != nil {
		log.Fatal(err)
	}

	for _, val := range buffer {
		kurt += math.Pow((val-mean)/stdev, 4)
	}
	return []float64{(1 / float64(len(buffer)) * kurt)}
}

// concatNameSpace combines an array of namespces into a single string
func concatNameSpace(namespace []string) string {
	completeNamespace := strings.Join(namespace, "/")
	return completeNamespace
}

// insertInToBuffer adds a new value into this' buffer object
func (p *Plugin) insertInToBuffer(val interface{}, ns []string) {

	if p.bufferCurSize == 0 {
		var buff = make([]interface{}, p.bufferMaxSize)
		buff[0] = val
		p.buffer[concatNameSpace(ns)] = buff
	} else {
		p.buffer[concatNameSpace(ns)][p.bufferIndex] = val
	}
}

// updateCounters updates the meta informaiton (current size and index) of this' buffer object
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

// GetConfigPolicy returns the config policy
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

// Process processes the data, inputs the data into this' buffer and calls the descriptive statistics method
func (p *Plugin) Process(contentType string, content []byte, config map[string]ctypes.ConfigValue) (string, []byte, error) {
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
		log.Printf("Error decoding: error=%v content=%v", err, content)
		return "", nil, err
	}
	var results []plugin.MetricType

	for _, metric := range metrics {
		switch reflect.ValueOf(metric.Data()).Kind() {
		default:
			st := fmt.Sprintf("Unkown data received: Type %T", reflect.ValueOf(metric.Data()).Kind())
			log.Printf(st)
			return "", nil, errors.New(st)
		case reflect.Slice:
			s := reflect.ValueOf(metric.Data())
			for i := 0; i < s.Len(); i++ {
				p.insertInToBuffer(s.Index(i).Interface(), metric.Namespace().Strings())
				p.updateCounters()
			}
		case reflect.Float64:
			s := reflect.ValueOf(metric.Data())
			p.insertInToBuffer(s.Interface(), metric.Namespace().Strings())
			p.updateCounters()
		}

		var err error
		var stats []plugin.MetricType
		if p.bufferCurSize < p.bufferMaxSize {
			stats, err = p.calculateStats(p.buffer[concatNameSpace(metric.Namespace().Strings())][0:p.bufferCurSize], metric)
			if err != nil {
				log.Printf("Error occured in calculating Statistics: %s", err)
				return "", nil, err
			}
		} else {
			stats, err = p.calculateStats(p.buffer[concatNameSpace(metric.Namespace().Strings())], metric)
			if err != nil {
				log.Printf("Error occurred in calculating Statistics: %s", err)
				return "", nil, err
			}
		}

		results = append(results, stats...)
	}

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(results); err != nil {
		return "", nil, err
	}

	return contentType, buf.Bytes(), nil
}
