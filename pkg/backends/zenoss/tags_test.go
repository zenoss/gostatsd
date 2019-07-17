package zenoss

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/atlassian/gostatsd"
	structpb "github.com/golang/protobuf/ptypes/struct"
	"github.com/stretchr/testify/assert"
)

func TestGetTags(t *testing.T) {

	t.Run("comparing values from requests", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		}))
		defer server.Close()

		client, err := NewClient(server.URL, "apiKey123", false, false, uint(2), int(2), int(2), []string{"test:", "go"}, []string{"go", "lang"}, []string{"go", "stats"}, []string{"test:", "test"}, float64(0.2), []string{"a", "b"}, 1*time.Second, gostatsd.TimerSubtypes{})

		request1 := client.getTags([]string{"go:test:test", ""})
		request2 := client.getTags([]string{"go:", ""})
		request3 := client.getTags([]string{"failed request", ""})
		request4 := client.getTags([]string{"", ""})

		test_map := make(map[string]string)

		assert.Nil(t, err)
		assert.Equal(t, "test:test", request1.MetricDimensionTags["go"])
		assert.Equal(t, "true", request2.MetricDimensionTags["go"])
		assert.Equal(t, test_map, request3.MetricDimensionTags)
		assert.Equal(t, test_map, request4.MetricDimensionTags)
	})
}

func TestParseTag(t *testing.T) {

	t.Run("checking empty result", func(t *testing.T) {
		result := parseTag("")
		assert.Empty(t, result)
	})

	t.Run("comparing values explicitly", func(t *testing.T) {
		result := parseTag("test:")
		assert.Equal(t, "test", result.Key)
		assert.Equal(t, false, result.Indexed)
		assert.Equal(t, int64(0), result.Index)
		assert.Equal(t, "true", result.Value)
	})

	t.Run("full object comparison", func(t *testing.T) {
		tests := []struct {
			input    string
			expected TagParts
		}{
			{"test_tag", TagParts{Key: "", Indexed: false, Index: 0, Value: ""}},
			{"test:", TagParts{Key: "test", Indexed: false, Index: 0, Value: "true"}},
			{"test:test_tag", TagParts{Key: "test", Indexed: false, Index: 0, Value: "test_tag"}},
			{"", TagParts{Key: "", Indexed: false, Index: 0, Value: ""}},
		}

		for _, test := range tests {
			result := parseTag(test.input)
			assert.EqualValues(t, test.expected, *result)
		}
	})

}

func TestValueFromString(t *testing.T) {

	t.Run("cheking not empty", func(t *testing.T) {
		pointer := valueFromString("test_input")
		assert.NotEmpty(t, pointer)
	})

	t.Run("full object comparison", func(t *testing.T) {
		tests := []struct {
			input    string
			expected structpb.Value
		}{
			{"test_tag", structpb.Value{Kind: &structpb.Value_StringValue{StringValue: "test_tag"}}},
			{"test_input", structpb.Value{Kind: &structpb.Value_StringValue{StringValue: "test_input"}}},
			{"", structpb.Value{Kind: &structpb.Value_StringValue{StringValue: ""}}},
		}

		for _, test := range tests {
			result := valueFromString(test.input)
			assert.EqualValues(t, test.expected, *result)
		}
	})

}

func TestValueFromStringSlice(t *testing.T) {

	t.Run("cheking not empty", func(t *testing.T) {
		pointer := valueFromStringSlice([]string{"a", "b", "c"})
		assert.NotEmpty(t, pointer)
	})

	t.Run("full object comparison", func(t *testing.T) {
		ss := []string{"A", "B", "C"}
		stringValues := make([]*structpb.Value, len(ss))
		for i, s := range ss {
			stringValues[i] = valueFromString(s)
		}
		tests := []struct {
			input    []string
			expected structpb.Value
		}{
			{[]string{"A", "B", "C"}, structpb.Value{Kind: &structpb.Value_ListValue{ListValue: &structpb.ListValue{Values: stringValues}}}},
		}

		for _, test := range tests {
			result := valueFromStringSlice(test.input)
			assert.EqualValues(t, test.expected, *result)
		}
	})
}
