package zenoss

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"time"

	"github.com/atlassian/gostatsd"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"

	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/any"
	"github.com/golang/protobuf/ptypes/wrappers"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"

	proto "github.com/zenoss/zing-proto/go/cloud/data_receiver"
	"github.com/zenoss/zing-proto/go/model"
)

const (
	// BackendName is required by gostatsd.
	BackendName = "zenoss"

	defaultDisableTLS       = false
	defaultInsecureTLS      = false
	defaultDisableStreaming = false
	defaultMetricsPerBatch  = 1000
	defaultModelInterval    = 1 * time.Minute

	// gostatsd parameters.
	paramDisabledSubMetrics = "disabled-sub-metrics"

	// zenoss parameters.
	paramAddress             = "address"
	paramDisableTLS          = "disable-tls"
	paramInsecureTLS         = "insecure-tls"
	paramAPIKey              = "api-key"
	paramMetricsPerBatch     = "metrics-per-batch"
	paramMetricDimensionTags = "metric-dimension-tags"
	paramMetricMetadataTags  = "metric-metadata-tags"
	paramModelDimensionTags  = "model-dimension-tags"
	paramModelMetadataTags   = "model-metadata-tags"
	paramTweaks              = "tweaks"

	// zenoss tweaks for non-standard and testing behavior.
	tweakNoModels          = "no-models"
	tweakTaggedMetrics     = "tagged-metrics"
	tweakUsePublishMetrics = "use-PublishMetrics"
	tweakUsePutMetric      = "use-PutMetric"
)

// Name returns the name of the backend.
func (*Client) Name() string {
	return BackendName
}

// Client is used to send data to Zenoss.
type Client struct {
	client proto.DataReceiverServiceClient

	// zenoss options
	apiKey              string
	metricsPerBatch     int
	metricDimensionTags *Set
	metricMetadataTags  *Set
	modelDimensionTags  *Set
	modelMetadataTags   *Set
	tweaks              *Set

	// gostatsd options
	disabledSubtypes gostatsd.TimerSubtypes
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
	z.SetDefault(paramMetricsPerBatch, defaultMetricsPerBatch)
	z.SetDefault(paramMetricDimensionTags, []string{})
	z.SetDefault(paramMetricMetadataTags, []string{})
	z.SetDefault(paramModelDimensionTags, []string{})
	z.SetDefault(paramModelMetadataTags, []string{})
	z.SetDefault(paramTweaks, []string{})

	return NewClient(
		z.GetString(paramAddress),
		z.GetString(paramAPIKey),
		z.GetBool(paramDisableTLS),
		z.GetBool(paramInsecureTLS),
		z.GetInt(paramMetricsPerBatch),
		z.GetStringSlice(paramMetricDimensionTags),
		z.GetStringSlice(paramMetricMetadataTags),
		z.GetStringSlice(paramModelDimensionTags),
		z.GetStringSlice(paramModelMetadataTags),
		z.GetStringSlice(paramTweaks),
		gostatsd.DisabledSubMetrics(v),
	)
}

// NewClient returns a new Zenoss client.
func NewClient(
	address string,
	apiKey string,
	disableTLS bool,
	insecureTLS bool,
	metricsPerBatch int,
	metricDimensionTags []string,
	metricMetadataTags []string,
	modelDimensionTags []string,
	modelMetadataTags []string,
	tweaks []string,
	disabledSubtypes gostatsd.TimerSubtypes) (*Client, error) {

	zlogWithFields(log.Fields{
		paramAddress:             address,
		paramDisableTLS:          disableTLS,
		paramInsecureTLS:         insecureTLS,
		paramMetricsPerBatch:     metricsPerBatch,
		paramMetricDimensionTags: metricDimensionTags,
		paramMetricMetadataTags:  metricMetadataTags,
		paramModelDimensionTags:  modelDimensionTags,
		paramTweaks:              tweaks,
		paramDisabledSubMetrics:  disabledSubtypes,
	}).Info("creating client")

	if metricsPerBatch <= 0 {
		return nil, fmt.Errorf("[%s] %s must be positive", BackendName, paramMetricsPerBatch)
	}
	if address == "" {
		return nil, fmt.Errorf("[%s] %s must be specified", BackendName, paramAddress)
	}
	if apiKey == "" {
		return nil, fmt.Errorf("[%s] %s must be specified", BackendName, paramAPIKey)
	}

	tweakSet := NewSetFromStrings(tweaks)
	if tweakSet.Has(tweakUsePublishMetrics) && tweakSet.Has(tweakUsePutMetric) {
		return nil, fmt.Errorf(
			"[%s] %s and %s tweaks are mutually exclusive",
			BackendName,
			tweakUsePublishMetrics,
			tweakUsePutMetric)
	}

	// The default PutMetrics service only supports canonical metrics.
	apiSupportsTags :=
		tweakSet.Has(tweakUsePublishMetrics) ||
			tweakSet.Has(tweakUsePutMetric)

	if tweakSet.Has(tweakTaggedMetrics) && !apiSupportsTags {
		return nil, fmt.Errorf(
			"[%s] %s requires that either %s or %s be set",
			BackendName,
			tweakTaggedMetrics,
			tweakUsePublishMetrics,
			tweakUsePublishMetrics)
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

	return &Client{
		client:              proto.NewDataReceiverServiceClient(conn),
		apiKey:              apiKey,
		metricsPerBatch:     metricsPerBatch,
		metricDimensionTags: NewSetFromStrings(metricDimensionTags),
		metricMetadataTags:  NewSetFromStrings(metricMetadataTags),
		modelDimensionTags:  NewSetFromStrings(modelDimensionTags),
		modelMetadataTags:   NewSetFromStrings(modelMetadataTags),
		tweaks:              tweakSet,
		disabledSubtypes:    disabledSubtypes,
	}, nil
}

// SendEvent not yet supported. Writes events to stdout.
func (c *Client) SendEvent(ctx context.Context, e *gostatsd.Event) (retErr error) {
	zlog().Infof("event: %v", e)
	return nil
}

// SendMetricsAsync flushes the metrics to the Graphite server, preparing payload synchronously but doing the send asynchronously.
func (c *Client) SendMetricsAsync(ctx context.Context, metrics *gostatsd.MetricMap, cb gostatsd.SendCallback) {
	timestamp := time.Now().UnixNano() / 1e6
	modeler := NewModeler()

	zlogWithFields(log.Fields{
		"counters": len(metrics.Counters),
		"gauges":   len(metrics.Gauges),
		"sets":     len(metrics.Sets),
		"timers":   len(metrics.Timers),
	}).Debug("processing metrics")

	zmetrics := c.processMetrics(timestamp, metrics, modeler)
	if len(zmetrics) < 1 {
		cb([]error{})
		return
	}

	modelBatches := modeler.GetModelBatches(100)

	go func() {
		var errs = []error{}
		defer cb(errs)

		ctx = metadata.AppendToOutgoingContext(ctx, "zenoss-api-key", c.apiKey)

		if len(modelBatches) > 0 {
			errs = append(errs, c.publishModels(ctx, modelBatches)...)
		}

		if len(zmetrics) > 0 {
			if c.tweaks.Has(tweakUsePublishMetrics) {
				errs = append(errs, c.publishMetrics(ctx, timestamp, zmetrics)...)
			} else if c.tweaks.Has(tweakUsePutMetric) {
				errs = append(errs, c.putMetricStream(ctx, timestamp, zmetrics)...)
			} else {
				errs = append(errs, c.putMetrics(ctx, timestamp, zmetrics)...)
			}
		}
	}()
}

func (c *Client) processMetrics(timestamp int64, metrics *gostatsd.MetricMap, modeler *Modeler) []*proto.Metric {
	zmetrics := []*proto.Metric{}
	var tagTypes *TagTypes

	metrics.Gauges.Each(func(key, tagsKey string, gauge gostatsd.Gauge) {
		tagTypes = c.getTags(gauge.Tags)
		zmetrics = c.appendMetric(zmetrics, float64(gauge.Value), timestamp, tagTypes, key)
		modeler.AddDimensions(timestamp, tagTypes)
	})

	metrics.Counters.Each(func(key, tagsKey string, counter gostatsd.Counter) {
		tagTypes = c.getTags(counter.Tags)
		zmetrics = c.appendMetricf(zmetrics, float64(counter.PerSecond), timestamp, tagTypes, "%s.rate", key)
		zmetrics = c.appendMetricf(zmetrics, float64(counter.Value), timestamp, tagTypes, "%s.count", key)
		modeler.AddDimensions(timestamp, tagTypes)
	})

	metrics.Timers.Each(func(key, tagsKey string, timer gostatsd.Timer) {
		tagTypes = c.getTags(timer.Tags)
		modeler.AddDimensions(timestamp, tagTypes)

		if !c.disabledSubtypes.Lower {
			zmetrics = c.appendMetricf(zmetrics, timer.Min, timestamp, tagTypes, "%s.lower", key)
		}
		if !c.disabledSubtypes.Upper {
			zmetrics = c.appendMetricf(zmetrics, timer.Max, timestamp, tagTypes, "%s.upper", key)
		}
		if !c.disabledSubtypes.Count {
			zmetrics = c.appendMetricf(zmetrics, float64(timer.Count), timestamp, tagTypes, "%s.count", key)
		}
		if !c.disabledSubtypes.CountPerSecond {
			zmetrics = c.appendMetricf(zmetrics, timer.PerSecond, timestamp, tagTypes, "%s.count_ps", key)
		}
		if !c.disabledSubtypes.Mean {
			zmetrics = c.appendMetricf(zmetrics, timer.Mean, timestamp, tagTypes, "%s.mean", key)
		}
		if !c.disabledSubtypes.Median {
			zmetrics = c.appendMetricf(zmetrics, timer.Median, timestamp, tagTypes, "%s.median", key)
		}
		if !c.disabledSubtypes.StdDev {
			zmetrics = c.appendMetricf(zmetrics, timer.StdDev, timestamp, tagTypes, "%s.std", key)
		}
		if !c.disabledSubtypes.Sum {
			zmetrics = c.appendMetricf(zmetrics, timer.Sum, timestamp, tagTypes, "%s.sum", key)
		}
		if !c.disabledSubtypes.SumSquares {
			zmetrics = c.appendMetricf(zmetrics, timer.SumSquares, timestamp, tagTypes, "%s.sum_squares", key)
		}

		for _, pct := range timer.Percentiles {
			zmetrics = c.appendMetricf(zmetrics, pct.Float, timestamp, tagTypes, "%s.%s", key, pct.Str)
		}
	})

	metrics.Sets.Each(func(key, tagsKey string, set gostatsd.Set) {
		tagTypes = c.getTags(set.Tags)
		zmetrics = c.appendMetric(zmetrics, float64(len(set.Values)), timestamp, tagTypes, key)
		modeler.AddDimensions(timestamp, tagTypes)
	})

	return zmetrics
}

// TagTypes TODO
type TagTypes struct {
	MetricDimensionTags map[string]string
	MetricMetadataTags  map[string]*model.AnyArray
	ModelDimensionTags  map[string]string
	ModelMetadataTags   map[string]string
}

func (c *Client) getTags(tags gostatsd.Tags) *TagTypes {
	tagTypes := &TagTypes{
		MetricDimensionTags: map[string]string{},
		MetricMetadataTags:  map[string]*model.AnyArray{},
		ModelDimensionTags:  map[string]string{},
		ModelMetadataTags:   map[string]string{},
	}

	tagKey := ""
	tagValue := ""

	for _, tag := range tags {
		if strings.Contains(tag, ":") {
			parts := strings.SplitN(tag, ":", 2)
			tagKey = parts[0]
			tagValue = parts[1]
		} else {
			tagKey = tag
			tagValue = "true"
		}

		if c.metricDimensionTags.Has(tagKey) {
			tagTypes.MetricDimensionTags[tagKey] = tagValue
		}

		if c.metricMetadataTags.Has(tagKey) {
			tagTypes.MetricMetadataTags[tagKey] = toAnyArray(tagValue)

			// When the tagged-metrics tweak is used, we want to send all
			// metric-metadata-tags and metric-dimension-tags as tags. So
			// we'll stash them all in tagTypes.MetricDimensionTags.
			if c.tweaks.Has(tweakTaggedMetrics) {
				tagTypes.MetricDimensionTags[tagKey] = tagValue
			}
		}

		if c.modelDimensionTags.Has(tagKey) {
			tagTypes.ModelDimensionTags[tagKey] = tagValue
		}

		if c.modelMetadataTags.Has(tagKey) {
			tagTypes.ModelMetadataTags[tagKey] = tagValue
		}
	}

	return tagTypes
}

func toAnyArray(s string) *model.AnyArray {
	av, err := ptypes.MarshalAny(&wrappers.StringValue{Value: s})
	if err != nil {
		return &model.AnyArray{}
	}

	return &model.AnyArray{Value: []*any.Any{av}}
}

func (c *Client) appendMetricf(metrics []*proto.Metric, value float64, timestamp int64, tagTypes *TagTypes, nameFormat string, a ...interface{}) []*proto.Metric {
	return c.appendMetric(metrics, value, timestamp, tagTypes, fmt.Sprintf(nameFormat, a...))
}

func (c *Client) appendMetric(metrics []*proto.Metric, value float64, timestamp int64, tagTypes *TagTypes, name string) []*proto.Metric {
	return append(
		metrics,
		&proto.Metric{
			Metric:     name,
			Timestamp:  timestamp,
			Dimensions: tagTypes.MetricDimensionTags,
			Metadata:   tagTypes.MetricMetadataTags,
			Value:      value,
		},
	)
}

func (c *Client) publishModels(ctx context.Context, modelBatches []*proto.ModelBatch) []error {
	const rpc = "PublishModels"
	errs := []error{}

	if c.tweaks.Has(tweakNoModels) {
		zlogRPC(rpc).Debugf("skipping models due to %s tweak", tweakNoModels)
		return errs
	}

	for _, b := range modelBatches {
		zlogRPCWithField(rpc, "count", len(b.Models)).Debug("sending model batch")
		_, err := c.client.PublishModels(ctx, b)
		if err != nil {
			zlogRPCWithError(rpc, err).Error("error sending model batch")
			errs = append(errs, err)
		} else {
			zlogRPC(rpc).Debug("sent model batch")
		}
	}

	return errs
}

func (c *Client) publishMetrics(ctx context.Context, timestamp int64, metrics []*proto.Metric) []error {
	const rpc = "PublishMetrics"
	errs := []error{}

	for _, b := range c.getPublishMetricsBatches(metrics) {
		zlogRPCWithField(rpc, "count", len(b.Metrics)).Debug("sending metric batch")

		_, err := c.client.PublishMetrics(ctx, b)
		if err != nil {
			zlogRPCWithError(rpc, err).Error("error sending metric batch")
			errs = append(errs, err)
		} else {
			zlogRPC(rpc).Debug("sent metric batch")
		}
	}

	return errs
}

func (c *Client) getPublishMetricsBatches(inMetrics []*proto.Metric) []*proto.MetricBatch {
	batchSize := c.metricsPerBatch
	batches := make([]*proto.MetricBatch, 0, int(len(inMetrics)%batchSize)+1)

	var metricWrapper *proto.MetricWrapper

	for len(inMetrics) > 0 {
		if len(inMetrics) < batchSize {
			batchSize = len(inMetrics)
		}

		metricWrappers := make([]*proto.MetricWrapper, 0, batchSize)
		for _, inMetric := range inMetrics[:batchSize] {
			if c.tweaks.Has(tweakTaggedMetrics) {
				metricWrapper = &proto.MetricWrapper{
					MetricType: &proto.MetricWrapper_Tagged{
						Tagged: &proto.TaggedMetric{
							Metric:    inMetric.Metric,
							Timestamp: inMetric.Timestamp,
							Value:     inMetric.Value,
							Tags:      inMetric.Dimensions,
						},
					},
				}
			} else {
				metricWrapper = &proto.MetricWrapper{
					MetricType: &proto.MetricWrapper_Canonical{
						Canonical: inMetric,
					},
				}
			}

			metricWrappers = append(metricWrappers, metricWrapper)
		}

		batches = append(
			batches,
			&proto.MetricBatch{Metrics: metricWrappers})

		inMetrics = inMetrics[batchSize:]
	}

	return batches
}

func (c *Client) putMetrics(ctx context.Context, timestamp int64, metrics []*proto.Metric) []error {
	const rpc = "PutMetrics"
	errs := []error{}

	for _, b := range c.getPutMetricsBatches(metrics) {
		zlogRPCWithField(rpc, "count", len(b.Metrics)).Debug("sending metric batch")

		putStatus, err := c.client.PutMetrics(ctx, b)
		if err != nil {
			zlogRPCWithError(rpc, err).Error("error sending metric batch")
			errs = append(errs, err)
		} else {
			zlogRPCWithFields(rpc, log.Fields{
				"message":   putStatus.GetMessage(),
				"succeeded": putStatus.GetSucceeded(),
				"failed":    putStatus.GetFailed(),
			}).Debug("sent metric batch")
		}
	}

	return errs
}

func (c *Client) getPutMetricsBatches(inMetrics []*proto.Metric) []*proto.Metrics {
	batchSize := c.metricsPerBatch
	batches := make([]*proto.Metrics, 0, int(len(inMetrics)%batchSize)+1)

	for len(inMetrics) > 0 {
		if len(inMetrics) < batchSize {
			batchSize = len(inMetrics)
		}

		batches = append(
			batches,
			&proto.Metrics{
				DetailedResponse: true,
				Metrics:          inMetrics[:batchSize],
			},
		)

		inMetrics = inMetrics[batchSize:]
	}

	return batches
}

func (c *Client) putMetricStream(ctx context.Context, timestamp int64, metrics []*proto.Metric) []error {
	const rpc = "PutMetric"
	errs := []error{}

	zlogRPCWithField(rpc, "count", len(metrics)).Debug("streaming metrics")
	stream, err := c.client.PutMetric(ctx)
	if err != nil {
		zlogRPCWithError(rpc, err).Error("failed opening stream")
		errs = append(errs, err)
		return errs
	}

	var metricWrapper *proto.MetricWrapper

	for _, metric := range metrics {
		if c.tweaks.Has(tweakTaggedMetrics) {
			metricWrapper = &proto.MetricWrapper{
				MetricType: &proto.MetricWrapper_Tagged{
					Tagged: &proto.TaggedMetric{
						Metric:    metric.Metric,
						Timestamp: metric.Timestamp,
						Value:     metric.Value,
						Tags:      metric.Dimensions,
					},
				},
			}
		} else {
			metricWrapper = &proto.MetricWrapper{
				MetricType: &proto.MetricWrapper_Canonical{
					Canonical: metric,
				},
			}
		}

		err := stream.Send(metricWrapper)
		if err != nil {
			zlogRPCWithError(rpc, err).Error("failed stream send")
			errs = append(errs, err)
		}
	}

	_, err = stream.CloseAndRecv()
	if err != nil {
		zlogRPCWithError(rpc, err).Error("failed stream close")
		errs = append(errs, err)
	}

	return errs
}

func getSubViper(v *viper.Viper, key string) *viper.Viper {
	n := v.Sub(key)
	if n == nil {
		n = viper.New()
	}

	return n
}
