package zenoss

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"sort"

	proto "github.com/zenoss/zing-proto/go/cloud/data_receiver"
)

// Modeler TODO
type Modeler struct {
	buffer map[uint32]*proto.Model
}

// NewModeler TODO
func NewModeler() *Modeler {
	return &Modeler{
		buffer: make(map[uint32]*proto.Model),
	}
}

// AddDimensions TODO
func (m *Modeler) AddDimensions(timestamp int64, dimensions map[string]string) {
	if len(dimensions) < 1 {
		return
	}

	m.buffer[maphash(dimensions)] = &proto.Model{
		Timestamp:  timestamp,
		Dimensions: dimensions,
	}
}

// GetModels TODO
func (m *Modeler) GetModels() []*proto.Model {
	models := make([]*proto.Model, len(m.buffer))
	i := 0
	for _, model := range m.buffer {
		models[i] = model
		i++
	}
	return models
}

// GetModelBatches TODO
func (m *Modeler) GetModelBatches(batchSize int) []*proto.ModelBatch {
	models := m.GetModels()
	batchCount := (len(models) % batchSize) + 1
	batches := make([]*proto.ModelBatch, 0, batchCount)
	for len(models) > 0 {
		if len(models) < batchSize {
			batchSize = len(models)
		}

		batches = append(
			batches,
			&proto.ModelBatch{
				Models: models[:batchSize],
			},
		)

		models = models[batchSize:]
	}

	return batches
}

func maphash(m map[string]string) uint32 {
	keys := make([]string, len(m))
	i := 0
	for k := range m {
		keys[i] = k
		i++
	}

	sort.Strings(keys)
	var buf bytes.Buffer
	for _, k := range keys {
		buf.WriteString(fmt.Sprintf("%s:%s,", k, m[k]))
	}

	return crc32.ChecksumIEEE(buf.Bytes())
}
