package zenoss

import (
	"regexp"
	"strconv"

	"github.com/atlassian/gostatsd"
	structpb "github.com/golang/protobuf/ptypes/struct"
)

var tagRegexp = regexp.MustCompile(`([^\.\:]+)(?:\.(\d+))?(?:\:(.+)?)`)

// TagParts TODO
type TagParts struct {
	Key     string
	Indexed bool
	Index   int64
	Value   string
}

// TagTypes TODO
type TagTypes struct {
	MetricDimensionTags map[string]string
	MetricMetadataTags  *structpb.Struct
	ModelDimensionTags  map[string]string
	ModelMetadataTags   *structpb.Struct
}

func (c *Client) getTags(tags gostatsd.Tags) *TagTypes {
	tt := &TagTypes{
		MetricDimensionTags: map[string]string{},
		MetricMetadataTags:  &structpb.Struct{},
		ModelDimensionTags:  map[string]string{},
		ModelMetadataTags:   &structpb.Struct{},
	}

	var tp *TagParts

	// Support *.# indexed tags into lists.
	metricMetadataFields := make(map[string]*structpb.Value, len(*c.metricMetadataTags))
	metricMetadataLists := make(map[string][]string, len(*c.metricMetadataTags))
	modelMetadataFields := make(map[string]*structpb.Value, len(*c.modelMetadataTags))
	modelMetadataLists := make(map[string][]string, len(*c.modelMetadataTags))

	for _, tag := range tags {
		tp = parseTag(tag)

		if c.metricDimensionTags.Has(tp.Key) {
			tt.MetricDimensionTags[tp.Key] = tp.Value
		}

		if c.metricMetadataTags.Has(tp.Key) {
			if tp.Indexed {
				metricMetadataLists[tp.Key] = append(
					metricMetadataLists[tp.Key],
					tp.Value)
			} else {
				metricMetadataFields[tp.Key] = valueFromString(tp.Value)
			}

			// When the tagged-metrics tweak is used, we want to send all
			// metric-metadata-tags and metric-dimension-tags as tags. So
			// we'll stash them all in tt.MetricDimensionTags.
			if c.tweaks.Has(tweakTaggedMetrics) {
				tt.MetricDimensionTags[tp.Key] = tp.Value
			}
		}

		if c.modelDimensionTags.Has(tp.Key) {
			tt.ModelDimensionTags[tp.Key] = tp.Value
		}

		if c.modelMetadataTags.Has(tp.Key) {
			if tp.Indexed {
				modelMetadataLists[tp.Key] = append(
					modelMetadataLists[tp.Key],
					tp.Value)
			} else {
				modelMetadataFields[tp.Key] = valueFromString(tp.Value)
			}
		}
	}

	for k, v := range metricMetadataLists {
		metricMetadataFields[k] = valueFromStringSlice(v)
	}

	tt.MetricMetadataTags.Fields = metricMetadataFields

	for k, v := range modelMetadataLists {
		modelMetadataFields[k] = valueFromStringSlice(v)
	}

	tt.ModelMetadataTags.Fields = modelMetadataFields

	return tt
}

func parseTag(tag string) *TagParts {
	tagParts := &TagParts{}

	match := tagRegexp.FindStringSubmatch(tag)
	if match == nil {
		return tagParts
	}

	tagParts.Key = match[1]

	if len(match[2]) > 0 {
		index, err := strconv.ParseInt(match[2], 0, 64)
		if err == nil {
			tagParts.Indexed = true
			tagParts.Index = index
		}
	}

	if len(match[3]) > 0 {
		tagParts.Value = match[3]
	} else {
		tagParts.Value = "true"
	}

	return tagParts
}

func valueFromString(s string) *structpb.Value {
	return &structpb.Value{
		Kind: &structpb.Value_StringValue{
			StringValue: s,
		},
	}
}

func valueFromStringSlice(ss []string) *structpb.Value {
	stringValues := make([]*structpb.Value, len(ss))
	for i, s := range ss {
		stringValues[i] = valueFromString(s)
	}
	return &structpb.Value{
		Kind: &structpb.Value_ListValue{
			ListValue: &structpb.ListValue{
				Values: stringValues,
			},
		},
	}
}
