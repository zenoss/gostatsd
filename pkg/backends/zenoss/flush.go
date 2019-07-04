package zenoss

import (
	"fmt"

	proto "github.com/zenoss/zenoss-protobufs/go/cloud/data_receiver"
)

// flush represents a send operation.
type flush struct {
	ts               *timeSeries
	timestamp        int64
	flushIntervalSec float64
	metricsPerBatch  int
	cb               func(*timeSeries)
}

// timeSeries represents a time series data structure.
type timeSeries struct {
	MetricsSeries *proto.Metrics
	Modeler       *Modeler
}

func (f *flush) addMetricf(value float64, timestamp int64, tagTypes *TagTypes, nameFormat string, a ...interface{}) {
	f.addMetric(value, timestamp, tagTypes, fmt.Sprintf(nameFormat, a...))
}

func (f *flush) addMetric(value float64, timestamp int64, tagTypes *TagTypes, name string) {
	f.ts.MetricsSeries.Metrics = append(
		f.ts.MetricsSeries.Metrics,
		&proto.Metric{
			Metric:         name,
			Timestamp:      timestamp,
			Dimensions:     tagTypes.MetricDimensionTags,
			MetadataFields: tagTypes.MetricMetadataTags,
			Value:          value,
		},
	)
}

func (f *flush) maybeFlush() {
	if len(f.ts.MetricsSeries.Metrics)+len(f.ts.Modeler.buffer)+20 >= f.metricsPerBatch { // flush before it reaches max size and grows the slice
		f.cb(f.ts)
		f.ts = &timeSeries{
			MetricsSeries: &proto.Metrics{
				DetailedResponse: true,
				Metrics:          make([]*proto.Metric, 0, f.metricsPerBatch),
			},
			Modeler: &Modeler{
				buffer: make(map[uint32]*proto.Model),
			},
		}
	}
}

func (f *flush) finish() {
	if len(f.ts.MetricsSeries.Metrics) > 0 || len(f.ts.Modeler.buffer) > 0 {
		f.cb(f.ts)
	}
}
