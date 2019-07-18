package zenoss

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"fmt"
	"github.com/atlassian/gostatsd"
	structpb "github.com/golang/protobuf/ptypes/struct"
	"github.com/stretchr/testify/assert"
	proto "github.com/zenoss/zenoss-protobufs/go/cloud/data_receiver"
)

func TestModeler(t *testing.T) {

	t.Run("test modeler, dimensions, getting models", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		}))
		defer server.Close()

		client, err := NewClient(server.URL, "apiKey123", false, false, uint(2), int(2), int(2), []string{"test:", "go"}, []string{"go", "lang"}, []string{"go", "stats"}, []string{"test:", "test"}, float64(0.2), []string{"a", "b"}, 1*time.Second, gostatsd.TimerSubtypes{})
		test_tag_type := client.getTags([]string{"go:", "go"})
		testModeler := &Modeler{buffer: make(map[uint32]*proto.Model)}
		timestamp := int64(12345)
		testModeler.AddDimensions(timestamp, test_tag_type)
		testModel := testModeler.GetModels()
		assert.Nil(t, err)
		assert.Equal(t, timestamp, testModel[0].Timestamp)
		assert.Equal(t, testModel[0].Dimensions["go"], "true")
	})

}

func TestMaphash(t *testing.T) {

	t.Run("comparing values", func(t *testing.T) {
		ss := []string{"A", "B", "C"}
		test_map := make(map[string]string, len(ss))
		for i, s := range ss {
			key := fmt.Sprintf("test_key %s", strconv.Itoa(i))
			test_map[key] = s
		}
		request := maphash(test_map)
		assert.Equal(t, uint32(1711912109), request)
	})

}

func TestMetadataFromStringMap(t *testing.T) {

	t.Run("checking empty result", func(t *testing.T) {
		ss := []string{"A", "B", "C"}
		test_map := make(map[string]string, len(ss))
		for i, s := range ss {
			key := fmt.Sprintf("test_key %s", strconv.Itoa(i))
			test_map[key] = s
		}
		fields := make(map[string]*structpb.Value, len(ss))
		for k, v := range ss {
			key := fmt.Sprintf("test_key %s", strconv.Itoa(k))
			fields[key] = &structpb.Value{
				Kind: &structpb.Value_StringValue{
					StringValue: v,
				},
			}
		}
		request := metadataFromStringMap(test_map)
		fmt.Println(request)
		assert.Equal(t, fields, request.Fields)
	})

}

