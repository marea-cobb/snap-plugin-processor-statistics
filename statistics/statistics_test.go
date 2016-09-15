//
// +build unit

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
	"log"
	"math/rand"
	"testing"
	"time"

	"github.com/intelsdi-x/snap/control/plugin"
	"github.com/intelsdi-x/snap/control/plugin/cpolicy"
	"github.com/intelsdi-x/snap/core"
	"github.com/intelsdi-x/snap/core/ctypes"
	. "github.com/smartystreets/goconvey/convey"
)

//Random number generator
func randInt(min int, max int) int {
	return min + rand.Intn(max-min)
}

func TestStatisticsProcessor(t *testing.T) {
	meta := Meta()
	Convey("Meta should return metadata for the plugin", t, func() {
		Convey("So meta.Name should equal statistics", func() {
			So(meta.Name, ShouldEqual, "statistics")
		})
		Convey("So meta.Version should equal version", func() {
			So(meta.Version, ShouldEqual, version)
		})
		Convey("So meta.Type should be of type plugin.ProcessorPluginType", func() {
			So(meta.Type, ShouldResemble, plugin.ProcessorPluginType)
		})
	})

	proc := New()
	Convey("Create statistics processor", t, func() {
		Convey("So proc should not be nil", func() {
			So(proc, ShouldNotBeNil)
		})
		Convey("So proc should be of type statisticsProcessor", func() {
			So(proc, ShouldHaveSameTypeAs, &Plugin{})
		})
		Convey("proc.GetConfigPolicy should return a config policy", func() {
			configPolicy, _ := proc.GetConfigPolicy()
			Convey("So config policy should be a cpolicy.ConfigPolicy", func() {
				So(configPolicy, ShouldHaveSameTypeAs, &cpolicy.ConfigPolicy{})
			})
			testConfig := make(map[string]ctypes.ConfigValue)
			testConfig["SlidingWindowLength"] = ctypes.ConfigValueInt{Value: 23}
			cfg, errs := configPolicy.Get([]string{""}).Process(testConfig)
			Convey("So config policy should process testConfig and return a config", func() {
				So(cfg, ShouldNotBeNil)
			})
			Convey("So testConfig processing should return no errors", func() {
				So(errs.HasErrors(), ShouldBeFalse)
			})
		})
	})
}

func TestStatisticsProcessorMetrics(t *testing.T) {
	Convey("Statistics Processor tests", t, func() {
		metrics := make([]plugin.MetricType, 10)
		data := [10]float64{5, 12, 7, 9, 33, 53, 24, 16, 18, 1}
		config := make(map[string]ctypes.ConfigValue)

		config["SlidingWindowLength"] = ctypes.ConfigValueInt{Value: 5}

		Convey("Statistics for float64 data", func() {
			for i := range metrics {
				metrics[i] = plugin.MetricType{
					Data_:      data[i],
					Namespace_: core.NewNamespace("foo", "bar"),
					Timestamp_: time.Now(),
				}
			}

			var buf bytes.Buffer
			enc := gob.NewEncoder(&buf)
			enc.Encode(metrics)

			statisticsObj := New()
			_, stats, err := statisticsObj.Process("snap.gob", buf.Bytes(), config)

			if err != nil {
				log.Fatal(err)
			}

			var results []plugin.MetricType
			dec := gob.NewDecoder(bytes.NewBuffer(stats))
			err = dec.Decode(&results)

			if err != nil {
				log.Fatal("decode", err)
			}

			for _, m := range results {
				log.Println(m)
			}

			for _, m := range results {
				ns := m.Namespace().Strings()[1]

				switch {
				case ns == "Count":
					So(m.Data(), ShouldAlmostEqual, 10, 0.01)
				case ns == "Mean":
					So(m.Data(), ShouldAlmostEqual, 17.8, 0.01)
				case ns == "Median":
					So(m.Data(), ShouldAlmostEqual, 14, 0.01)
				case ns == "Trimean":
					So(m.Data(), ShouldAlmostEqual, 14.75, 0.01)
				case ns == "Range":
					So(m.Data(), ShouldAlmostEqual, 52, 0.01)
				case ns == "Sum":
					So(m.Data(), ShouldAlmostEqual, 178, 0.01)
				case ns == "Kurtosis":
					So(m.Data(), ShouldAlmostEqual, 3.595, 0.01)
				case ns == "Skewness":
					So(m.Data(), ShouldAlmostEqual, 1.187, 0.01)
				case ns == "Standard Deviation":
					So(m.Data(), ShouldAlmostEqual, 14.783, 0.01)
				case ns == "Variance":
					So(m.Data(), ShouldAlmostEqual, 218.56, 0.01)
				case ns == "Maximum":
					So(m.Data(), ShouldAlmostEqual, 53, 0.01)
				case ns == "Minimum":
					So(m.Data(), ShouldAlmostEqual, 1, 0.01)
				case ns == "99%-ile":
					So(m.Data(), ShouldAlmostEqual, 53, 0.01)
				case ns == "95%-ile":
					So(m.Data(), ShouldAlmostEqual, 53, 0.01)
				case ns == "Mode":
					So(m.Data(), ShouldResemble, []float64(nil))
				default:
					log.Println("Raw metric found")
					log.Println("Data: %v", ns)
				}
			}

			var metricsNew []plugin.MetricType
			So(metrics, ShouldNotResemble, metricsNew)
		})

		Convey("Statistics for unknown data type", func() {
			for i := range metrics {

				data := "I am an unknow data Type"
				metrics[i] = plugin.MetricType{
					Data_:      data,
					Namespace_: core.NewNamespace("foo", "bar"),
					Timestamp_: time.Now(),
				}
			}
			var buf bytes.Buffer
			enc := gob.NewEncoder(&buf)
			enc.Encode(metrics)

			statisticObj := New()
			_, receivedData, _ := statisticObj.Process("snap.gob", buf.Bytes(), config)

			var metricsNew []plugin.MetricType

			//Decodes the content into MetricType
			dec := gob.NewDecoder(bytes.NewBuffer(receivedData))
			dec.Decode(&metricsNew)
			So(metrics, ShouldNotResemble, metricsNew)
		})

	})
}
