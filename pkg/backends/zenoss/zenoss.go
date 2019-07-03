package zenoss

import (
	"context"
	"crypto/tls"
	"fmt"
	"math"
	"runtime"
	"strings"
	"time"

	"github.com/atlassian/gostatsd"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"

	proto "github.com/zenoss/zenoss-protobufs/go/cloud/data_receiver"
)

const (
	// BackendName is required by gostatsd.
	BackendName = "zenoss"

	defaultDisableTLS      = false
	defaultInsecureTLS     = false
	defaultMaxRetries      = 3
	defaultMetricsPerBatch = 1000
	defaultModelInterval   = 1 * time.Minute
	defaultModelsPerBatch  = 100
	defaultRequestDelay    = 2.0

	// gostatsd parameters.
	paramDisabledSubMetrics = "disabled-sub-metrics"

	// zenoss parameters.
	paramAddress             = "address"
	paramDisableTLS          = "disable-tls"
	paramInsecureTLS         = "insecure-tls"
	paramAPIKey              = "api-key"
	paramMaxRequests         = "max_requests"
	paramMaxRetries          = "max-retries"
	paramMetricsPerBatch     = "metrics-per-batch"
	paramMetricDimensionTags = "metric-dimension-tags"
	paramMetricMetadataTags  = "metric-metadata-tags"
	paramModelDimensionTags  = "model-dimension-tags"
	paramModelMetadataTags   = "model-metadata-tags"
	paramModelsPerBatch      = "models-per-batch"
	paramRequestDelay        = "request-delay"
	paramTweaks              = "tweaks"

	// zenoss tweaks for non-standard and testing behavior.
	tweakNoModels      = "no-models"
	tweakTaggedMetrics = "tagged-metrics"
	tweakUsePutMetric  = "use-PutMetric"
)

var defaultMaxRequests = uint(2 * runtime.NumCPU())

// Name returns the name of the backend.
func (*Client) Name() string {
	return BackendName
}

// Client is used to send data to Zenoss.
type Client struct {
	client proto.DataReceiverServiceClient

	// zenoss options
	apiKey              string
	maxRetries          int
	metricsPerBatch     int
	metricsSem          chan struct{}
	metricDimensionTags *Set
	metricMetadataTags  *Set
	modelDimensionTags  *Set
	modelMetadataTags   *Set
	modelsPerBatch      int
	requestDelay        float64
	tweaks              *Set

	// gostatsd options
	disabledSubtypes gostatsd.TimerSubtypes
	flushInterval    time.Duration
	now              func() time.Time
}

// NewClientFromViper returns a new Zenoss client.
func NewClientFromViper(v *viper.Viper) (gostatsd.Backend, error) {
	z := getSubViper(v, "zenoss")
	z.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	z.SetEnvPrefix(BackendName)
	z.SetTypeByDefaultValue(true)
	z.AutomaticEnv()

	z.SetDefault(paramDisableTLS, defaultDisableTLS)
	z.SetDefault(paramInsecureTLS, defaultInsecureTLS)
	z.SetDefault(paramMaxRequests, defaultMaxRequests)
	z.SetDefault(paramMaxRetries, defaultMaxRetries)
	z.SetDefault(paramMetricsPerBatch, defaultMetricsPerBatch)
	z.SetDefault(paramMetricDimensionTags, []string{})
	z.SetDefault(paramMetricMetadataTags, []string{})
	z.SetDefault(paramModelDimensionTags, []string{})
	z.SetDefault(paramModelMetadataTags, []string{})
	z.SetDefault(paramModelsPerBatch, defaultModelsPerBatch)
	z.SetDefault(paramRequestDelay, defaultRequestDelay)
	z.SetDefault(paramTweaks, []string{})

	return NewClient(
		z.GetString(paramAddress),
		z.GetString(paramAPIKey),
		z.GetBool(paramDisableTLS),
		z.GetBool(paramInsecureTLS),
		uint(z.GetInt(paramMaxRequests)),
		z.GetInt(paramMaxRetries),
		z.GetInt(paramMetricsPerBatch),
		z.GetStringSlice(paramMetricDimensionTags),
		z.GetStringSlice(paramMetricMetadataTags),
		z.GetStringSlice(paramModelDimensionTags),
		z.GetStringSlice(paramModelMetadataTags),
		z.GetInt(paramModelsPerBatch),
		z.GetFloat64(paramRequestDelay),
		z.GetStringSlice(paramTweaks),
		v.GetDuration("flush-interval"), // Main viper, not sub-viper
		gostatsd.DisabledSubMetrics(v),
	)
}

// NewClient returns a new Zenoss client.
func NewClient(
	address string,
	apiKey string,
	disableTLS bool,
	insecureTLS bool,
	maxRequests uint,
	maxRetries int,
	metricsPerBatch int,
	metricDimensionTags []string,
	metricMetadataTags []string,
	modelDimensionTags []string,
	modelMetadataTags []string,
	modelsPerBatch int,
	requestDelay float64,
	tweaks []string,
	flushInterval time.Duration,
	disabledSubtypes gostatsd.TimerSubtypes) (*Client, error) {

	zlogWithFields(log.Fields{
		paramAddress:             address,
		paramDisableTLS:          disableTLS,
		paramInsecureTLS:         insecureTLS,
		paramMetricsPerBatch:     metricsPerBatch,
		paramMetricDimensionTags: metricDimensionTags,
		paramMetricMetadataTags:  metricMetadataTags,
		paramModelDimensionTags:  modelDimensionTags,
		paramModelMetadataTags:   modelMetadataTags,
		paramModelsPerBatch:      modelsPerBatch,
		paramTweaks:              tweaks,
		paramDisabledSubMetrics:  disabledSubtypes,
	}).Info("creating client")

	if metricsPerBatch <= 0 {
		return nil, fmt.Errorf("[%s] %s must be positive", BackendName, paramMetricsPerBatch)
	}
	if modelsPerBatch <= 0 {
		return nil, fmt.Errorf("[%s] %s must be positive", BackendName, paramModelsPerBatch)
	}
	if address == "" {
		return nil, fmt.Errorf("[%s] %s must be specified", BackendName, paramAddress)
	}
	if apiKey == "" {
		return nil, fmt.Errorf("[%s] %s must be specified", BackendName, paramAPIKey)
	}

	var opt grpc.DialOption
	if disableTLS {
		opt = grpc.WithInsecure()
	} else {
		opt = grpc.WithTransportCredentials(
			credentials.NewTLS(&tls.Config{InsecureSkipVerify: insecureTLS}))
	}

	conn, err := grpc.Dial(address, opt)
	if err != nil {
		return nil, fmt.Errorf("[%s] failed to connect: %s", BackendName, err)
	}

	metricsSem := make(chan struct{}, maxRequests)
	for i := uint(0); i < maxRequests; i++ {
		metricsSem <- struct{}{}
	}

	return &Client{
		client:              proto.NewDataReceiverServiceClient(conn),
		apiKey:              apiKey,
		maxRetries:          maxRetries,
		metricsPerBatch:     metricsPerBatch,
		metricsSem:          metricsSem,
		metricDimensionTags: NewSetFromStrings(metricDimensionTags),
		metricMetadataTags:  NewSetFromStrings(metricMetadataTags),
		modelDimensionTags:  NewSetFromStrings(modelDimensionTags),
		modelMetadataTags:   NewSetFromStrings(modelMetadataTags),
		modelsPerBatch:      modelsPerBatch,
		requestDelay:        requestDelay,
		tweaks:              NewSetFromStrings(tweaks),
		now:                 time.Now,
		flushInterval:       flushInterval,
		disabledSubtypes:    disabledSubtypes,
	}, nil
}

// SendEvent not yet supported. Writes events to stdout.
func (c *Client) SendEvent(ctx context.Context, e *gostatsd.Event) (retErr error) {
	zlog().Infof("event: %v", e)
	return nil
}

// SendMetricsAsync flushes the metrics to the Graphite server, preparing payload synchronously but doing the send asynchronously.
func (z *Client) SendMetricsAsync(ctx context.Context, metrics *gostatsd.MetricMap, cb gostatsd.SendCallback) {
	counter := 0
	results := make(chan []error)
	ctx = metadata.AppendToOutgoingContext(ctx, "zenoss-api-key", z.apiKey)

	zlogWithFields(log.Fields{
		"counters": len(metrics.Counters),
		"gauges":   len(metrics.Gauges),
		"sets":     len(metrics.Sets),
		"timers":   len(metrics.Timers),
	}).Debug("processing metrics")

	z.processMetrics(metrics, func(ts *timeSeries) {
		go func() {
			select {
			case <-ctx.Done():
				return
			case <-z.metricsSem:
				defer func() {
					z.metricsSem <- struct{}{}
				}()
				errs := z.postData(ctx, ts)
				select {
				case <-ctx.Done():
				case results <- errs:
				}
			}
		}()
		counter++
	})
	go func() {
		errs := make([]error, 0, counter)
	loop:
		for c := 0; c < counter; c++ {
			select {
			case <-ctx.Done():
				errs = append(errs, ctx.Err())
				break loop
			case err := <-results:
				errs = append(errs, err...)
			}
		}
		cb(errs)
	}()
}

func (c *Client) postData(ctx context.Context, ts *timeSeries) []error {
	models := ts.Modeler.GetModels()
	var errs = []error{}

	if len(models) > 0 {
		if c.tweaks.Has(tweakNoModels) {
			zlogRPC("PutModels").Debugf("skipping models due to %s tweak", tweakNoModels)
		} else {
			zlogRPCWithField("PutModels", "count", len(models)).Debug("sending model batch")
			cb := func() error {
				return c.putModels(ctx, c.getPutModels(models))
			}
			errs = append(errs, c.processRequest(cb))
		}
	}
	if len(ts.MetricsSeries.Metrics) > 0 {
		cb := func() error {
			return c.putMetrics(ctx, ts)
		}
		errs = append(errs, c.processRequest(cb))
	}

	return errs
}

func (c *Client) processRequest(cb func() error) error {
	attempts := 0
	for {
		var err = cb()

		if err != nil {
			zlog().Warn("Fail to send data, retrying")
			attempts++
		} else {
			return err
		}

		if attempts >= c.maxRetries {
			zlog().Error("Fail to deliver data, dropping package")
			zlogWithError(err)
			return err
		}
		waitTime := time.Duration(math.Pow(c.requestDelay, float64(attempts))) * time.Second
		time.Sleep(waitTime)
	}
}

func (z *Client) processMetrics(metrics *gostatsd.MetricMap, cb func(*timeSeries)) {
	var tagTypes *TagTypes
	timestamp := z.now().UnixNano() / 1e6
	fl := flush{
		ts: &timeSeries{
			MetricsSeries: &proto.Metrics{
				DetailedResponse: true,
				Metrics:          make([]*proto.Metric, 0, z.metricsPerBatch),
			},
			Modeler: &Modeler{
				buffer: make(map[uint32]*proto.Model),
			},
		},
		timestamp:        timestamp,
		flushIntervalSec: z.flushInterval.Seconds(),
		metricsPerBatch:  z.metricsPerBatch,
		cb:               cb,
	}

	metrics.Counters.Each(func(key, tagsKey string, counter gostatsd.Counter) {
		tagTypes = z.getTags(counter.Tags)
		fl.addMetricf(float64(counter.PerSecond), timestamp, tagTypes, "%s.rate", key)
		fl.addMetricf(float64(counter.Value), timestamp, tagTypes, "%s.count", key)
		fl.ts.Modeler.AddDimensions(timestamp, tagTypes)
		fl.maybeFlush()
	})

	metrics.Timers.Each(func(key, tagsKey string, timer gostatsd.Timer) {
		tagTypes = z.getTags(timer.Tags)
		if !z.disabledSubtypes.Lower {
			fl.addMetricf(timer.Min, timestamp, tagTypes, "%s.lower", key)
		}
		if !z.disabledSubtypes.Upper {
			fl.addMetricf(timer.Max, timestamp, tagTypes, "%s.upper", key)
		}
		if !z.disabledSubtypes.Count {
			fl.addMetricf(float64(timer.Count), timestamp, tagTypes, "%s.count", key)
		}
		if !z.disabledSubtypes.CountPerSecond {
			fl.addMetricf(timer.PerSecond, timestamp, tagTypes, "%s.count_ps", key)
		}
		if !z.disabledSubtypes.Mean {
			fl.addMetricf(timer.Mean, timestamp, tagTypes, "%s.mean", key)
		}
		if !z.disabledSubtypes.Median {
			fl.addMetricf(timer.Median, timestamp, tagTypes, "%s.median", key)
		}
		if !z.disabledSubtypes.StdDev {
			fl.addMetricf(timer.StdDev, timestamp, tagTypes, "%s.std", key)
		}
		if !z.disabledSubtypes.Sum {
			fl.addMetricf(timer.Sum, timestamp, tagTypes, "%s.sum", key)
		}
		if !z.disabledSubtypes.SumSquares {
			fl.addMetricf(timer.SumSquares, timestamp, tagTypes, "%s.sum_squares", key)
		}
		for _, pct := range timer.Percentiles {
			fl.addMetricf(pct.Float, timestamp, tagTypes, "%s.%s", key, pct.Str)
		}
		fl.ts.Modeler.AddDimensions(timestamp, tagTypes)
		fl.maybeFlush()
	})

	metrics.Gauges.Each(func(key, tagsKey string, gauge gostatsd.Gauge) {
		tagTypes = z.getTags(gauge.Tags)
		fl.addMetric(float64(gauge.Value), timestamp, tagTypes, key)
		fl.ts.Modeler.AddDimensions(timestamp, tagTypes)
		fl.maybeFlush()
	})

	metrics.Sets.Each(func(key, tagsKey string, set gostatsd.Set) {
		tagTypes = z.getTags(set.Tags)
		fl.addMetric(float64(len(set.Values)), timestamp, tagTypes, key)
		fl.ts.Modeler.AddDimensions(timestamp, tagTypes)
		fl.maybeFlush()
	})

	fl.finish()
}

func (c *Client) putModels(ctx context.Context, models *proto.Models) error {
	const rpc = "PutModels"

	zlogRPCWithField(rpc, "count", len(models.Models)).Debug("sending model batch")
	putStatus, err := c.client.PutModels(ctx, models)
	if err == nil {
		zlogRPCWithFields(rpc, log.Fields{
			"message":   putStatus.GetMessage(),
			"succeeded": putStatus.GetSucceeded(),
			"failed":    putStatus.GetFailed(),
		}).Debug("sent model batch")
	}

	return err
}

func (c *Client) getPutModels(inModels []*proto.Model) *proto.Models {
	return &proto.Models{
		DetailedResponse: true,
		Models:           inModels,
	}
}

func (c *Client) putMetrics(ctx context.Context, ts *timeSeries) error {
	const rpc = "PutMetrics"

	zlogRPCWithField(rpc, "count", len(ts.MetricsSeries.Metrics)).Debug("sending metric batch")

	if c.tweaks.Has(tweakTaggedMetrics) {
		taggedMetrics := make([]*proto.TaggedMetric, c.metricsPerBatch)
		for i, inMetric := range ts.MetricsSeries.Metrics {
			taggedMetrics[i] = taggedMetricFromCanonical(inMetric)
		}
		ts.MetricsSeries.TaggedMetrics = taggedMetrics
		ts.MetricsSeries.Metrics = make([]*proto.Metric, 0, c.metricsPerBatch)
	}

	putStatus, err := c.client.PutMetrics(ctx, ts.MetricsSeries)
	if err == nil {
		zlogRPCWithFields(rpc, log.Fields{
			"message":   putStatus.GetMessage(),
			"succeeded": putStatus.GetSucceeded(),
			"failed":    putStatus.GetFailed(),
		}).Debug("sent metric batch")
	}

	return err
}

func taggedMetricFromCanonical(canonical *proto.Metric) *proto.TaggedMetric {
	return &proto.TaggedMetric{
		Metric:    canonical.Metric,
		Timestamp: canonical.Timestamp,
		Value:     canonical.Value,
		Tags:      canonical.Dimensions,
	}
}

func getSubViper(v *viper.Viper, key string) *viper.Viper {
	n := v.Sub(key)
	if n == nil {
		n = viper.New()
	}

	return n
}
