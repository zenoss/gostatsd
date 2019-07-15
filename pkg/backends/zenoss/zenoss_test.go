package zenoss

import (
	"context"
	"fmt"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/atlassian/gostatsd"
	_struct "github.com/golang/protobuf/ptypes/struct"
	"github.com/stretchr/testify/assert"
	proto "github.com/zenoss/zenoss-protobufs/go/cloud/data_receiver"
)

// mocking connection object, to process requests locally
type connectionMock struct {
	putMetricsErrors int
	putModelsErrors  int

	putMetricsRequests int
	putModelsRequests  int

	metricsBody *proto.Metrics
	modelsBody  *proto.Models
}

// make request and check it for errors
func makeRequest(t *testing.T, c *Client, cb func() *gostatsd.MetricMap) {
	res := make(chan []error, 1)
	c.SendMetricsAsync(context.Background(), cb(), func(errs []error) {
		res <- errs
	})
	errs := <-res
	for _, err := range errs {
		assert.NoError(t, err)
	}
}

func TestMetricsRetries(t *testing.T) {
	requestsWithError := 3
	client, conn := newTestClient(requestsWithError, defaultMetricsPerBatch)
	makeRequest(t, 	&client, func() *gostatsd.MetricMap { return twoCounters() })

	expectedRequests := 4
	assert.Equal(t, expectedRequests, conn.putMetricsRequests)
}

func TestModelsRetries(t *testing.T) {
	requestsWithError := 3
	client, conn := newTestClient(requestsWithError, defaultMetricsPerBatch)
	makeRequest(t, &client, func() *gostatsd.MetricMap { return twoCounters() })

	expectedRequests := 4
	assert.Equal(t, expectedRequests, conn.putModelsRequests)
}

func TestMetricsSendFailed(t *testing.T) {
	requestsWithError := 4
	client, conn := newTestClient(requestsWithError, defaultMetricsPerBatch)
	res := make(chan []error, 1)
	client.SendMetricsAsync(context.Background(), twoCounters(), func(errs []error) {
		res <- errs
	})
	<-res
	expectedRequests := 4
	assert.Equal(t, expectedRequests, conn.putMetricsRequests)
}

func TestModelsSendFailed(t *testing.T) {
	requestsWithError := 4
	client, conn := newTestClient(requestsWithError, defaultMetricsPerBatch)
	res := make(chan []error, 1)
	client.SendMetricsAsync(context.Background(), twoCounters(), func(errs []error) {
		res <- errs
	})
	<-res
	expectedRequests := 4
	assert.Equal(t, expectedRequests, conn.putModelsRequests)
}

func TestSendMetricsInMultipleBatches(t *testing.T) {
	requestsWithError := 0
	metricsPerBatch := 1
	client, conn := newTestClient(requestsWithError, metricsPerBatch)
	makeRequest(t, &client, func() *gostatsd.MetricMap { return twoCounters() })

	expectedRequests := 2
	assert.Equal(t, expectedRequests, conn.putMetricsRequests)
}

func TestSendModelsInMultipleBatches(t *testing.T) {
	requestsWithError := 0
	metricsPerBatch := 1
	client, conn := newTestClient(requestsWithError, metricsPerBatch)
	makeRequest(t, &client, func() *gostatsd.MetricMap { return twoCounters() })

	expectedRequests := 2
	assert.Equal(t, expectedRequests, conn.putModelsRequests)
}

func TestSendMetrics(t *testing.T) {
	requestsWithError := 0
	client, conn := newTestClient(requestsWithError, defaultMetricsPerBatch)
	makeRequest(t, &client, func() *gostatsd.MetricMap { return metricsOneOfEach() })

	expected := "c1.rate 1.1\n" +
		"c1.count 5.0\n" +
		"t1.lower 0.0\n" +
		"t1.upper 1.0\n" +
		"t1.count 1.0\n" +
		"t1.count_ps 1.1\n" +
		"t1.mean 0.5\n" +
		"t1.median 0.5\n" +
		"t1.std 0.1\n" +
		"t1.sum 1.0\n" +
		"t1.sum_squares 1.0\n" +
		"t1.count_90 0.1\n" +
		"g1 3.0\n" +
		"users 3.0\n"

	result := conn.metricsBody
	str_result := ""
	for _, metric := range result.GetMetrics() {
		str_result += metric.GetMetric() + fmt.Sprintf(" %.1f\n", metric.GetValue())
	}
	assert.Equal(t, expected, str_result)
}

func TestSendMetricsDimensionTags(t *testing.T) {
	requestsWithError := 0
	client, conn := newTestClient(requestsWithError, defaultMetricsPerBatch)
	makeRequest(t, &client, func() *gostatsd.MetricMap { return oneCounter() })

	result := conn.metricsBody.GetMetrics()[0].GetDimensions()
	assert.Equal(t, "com.zenoss", result["source"])
	assert.Equal(t, "com.zenoss.app-examples", result["contextUUID"])
}

func TestSendModelsDimensionTags(t *testing.T) {
	requestsWithError := 0
	client, conn := newTestClient(requestsWithError, defaultMetricsPerBatch)
	makeRequest(t, &client, func() *gostatsd.MetricMap { return oneCounter() })

	result := conn.modelsBody.GetModels()[0].GetDimensions()
	assert.Equal(t, "com.zenoss", result["source"])
	assert.Equal(t, "com.zenoss.app-examples", result["contextUUID"])
}

func TestSendMetricsMetadataTags(t *testing.T) {
	requestsWithError := 0
	client, conn := newTestClient(requestsWithError, defaultMetricsPerBatch)
	makeRequest(t, &client, func() *gostatsd.MetricMap { return oneCounter() })

	result := conn.metricsBody.GetMetrics()[0].GetMetadataFields().GetFields()
	assert.Equal(t, "com.atlassian.gostatsd", result["source-type"].GetKind().(*_struct.Value_StringValue).StringValue)
	assert.Equal(t, "gostatsd.app-examples", result["name"].GetKind().(*_struct.Value_StringValue).StringValue)
}

func TestSendModelsMetadataTags(t *testing.T) {
	requestsWithError := 0
	client, conn := newTestClient(requestsWithError, defaultMetricsPerBatch)
	makeRequest(t, &client, func() *gostatsd.MetricMap { return oneCounter() })

	result := conn.modelsBody.GetModels()[0].GetMetadataFields().GetFields()
	assert.Equal(t, "com.atlassian.gostatsd", result["source-type"].GetKind().(*_struct.Value_StringValue).StringValue)
	assert.Equal(t, "gostatsd.app-examples", result["name"].GetKind().(*_struct.Value_StringValue).StringValue)
}

func (c *connectionMock) PutMetrics(ctx context.Context, in *proto.Metrics, _ ...grpc.CallOption) (*proto.StatusResult, error) {
	c.metricsBody = in
	c.putMetricsRequests++
	out := new(proto.StatusResult)
	if c.putMetricsErrors <= 0 {
		return out, nil
	}
	c.putMetricsErrors--
	return nil, fmt.Errorf("Request failed")
}

func (c *connectionMock) PutModels(ctx context.Context, in *proto.Models, _ ...grpc.CallOption) (*proto.ModelStatusResult, error) {
	c.modelsBody = in
	c.putModelsRequests++
	out := new(proto.ModelStatusResult)
	if c.putModelsErrors <= 0 {
		return out, nil
	}
	c.putModelsErrors--
	return nil, fmt.Errorf("Request failed")
}

func (c *connectionMock) PutMetric(_ context.Context, _ ...grpc.CallOption) (proto.DataReceiverService_PutMetricClient, error) {
	return nil, nil
}

func newTestClient(requestsWithError int, metricsPerBatch int) (Client, *connectionMock) {
	metricsSem := make(chan struct{}, defaultMaxRequests)
	for i := uint(0); i < defaultMaxRequests; i++ {
		metricsSem <- struct{}{}
	}
	conn := &connectionMock{
		putMetricsErrors: requestsWithError,
		putModelsErrors:  requestsWithError,

		putMetricsRequests: 0,
		putModelsRequests:  0,
	}

	c := Client{
		client:              conn,
		apiKey:              "apiKey123",
		maxRetries:          defaultMaxRetries,
		metricsPerBatch:     metricsPerBatch,
		metricsSem:          metricsSem,
		metricDimensionTags: NewSetFromStrings([]string{"source", "contextUUID"}),
		metricMetadataTags:  NewSetFromStrings([]string{"source-type", "name"}),
		modelDimensionTags:  NewSetFromStrings([]string{"source", "contextUUID"}),
		modelMetadataTags:   NewSetFromStrings([]string{"source-type", "name"}),
		requestDelay:        0.0,
		tweaks:              NewSetFromStrings([]string{}),
		now:                 time.Now,
		flushInterval:       1 * time.Second,
		disabledSubtypes:    gostatsd.TimerSubtypes{},
	}

	return c, c.client.(*connectionMock)
}

func oneCounter() *gostatsd.MetricMap {
	return &gostatsd.MetricMap{
		Counters: gostatsd.Counters{
			"stat1": map[string]gostatsd.Counter{
				"tag1": gostatsd.NewCounter(
					0,
					5,
					"",
					gostatsd.Tags{
						"source:com.zenoss",
						"contextUUID:com.zenoss.app-examples",
						"source-type:com.atlassian.gostatsd",
						"name:gostatsd.app-examples",
					},
				),
			},
		},
	}
}

// twoCounters returns two counters.
func twoCounters() *gostatsd.MetricMap {
	return &gostatsd.MetricMap{
		Counters: gostatsd.Counters{
			"stat1": map[string]gostatsd.Counter{
				"tag1": gostatsd.NewCounter(0, 5, "", gostatsd.Tags{"source:com.zenoss", "contextUUID:com.zenoss.app-examples"}),
			},
			"stat2": map[string]gostatsd.Counter{
				"tag2": gostatsd.NewCounter(0, 50, "", gostatsd.Tags{"source:test", "contextUUID:com.zenoss.services"}),
			},
		},
	}
}

func metricsOneOfEach() *gostatsd.MetricMap {
	return &gostatsd.MetricMap{
		Counters: gostatsd.Counters{
			"c1": map[string]gostatsd.Counter{
				"tag1": {PerSecond: 1.1, Value: 5, Timestamp: 0, Hostname: "h1", Tags: gostatsd.Tags{"tag1"}},
			},
		},
		Timers: gostatsd.Timers{
			"t1": map[string]gostatsd.Timer{
				"tag2": {
					Count:      1,
					PerSecond:  1.1,
					Mean:       0.5,
					Median:     0.5,
					Min:        0,
					Max:        1,
					StdDev:     0.1,
					Sum:        1,
					SumSquares: 1,
					Values:     []float64{0, 1},
					Percentiles: gostatsd.Percentiles{
						gostatsd.Percentile{Float: 0.1, Str: "count_90"},
					},
					Timestamp: 0,
					Hostname:  "h2",
					Tags:      gostatsd.Tags{"tag2"},
				},
			},
		},
		Gauges: gostatsd.Gauges{
			"g1": map[string]gostatsd.Gauge{
				"tag3": {Value: 3, Timestamp: 0, Hostname: "h3", Tags: gostatsd.Tags{"source:test", "contextUUID:com.zenoss.services"}},
			},
		},
		Sets: gostatsd.Sets{
			"users": map[string]gostatsd.Set{
				"tag4": {
					Values: map[string]struct{}{
						"joe":  {},
						"bob":  {},
						"john": {},
					},
					Timestamp: 0,
					Hostname:  "h4",
					Tags:      gostatsd.Tags{"source:com.zenoss", "contextUUID:com.zenoss.app-examples"},
				},
			},
		},
	}
}
