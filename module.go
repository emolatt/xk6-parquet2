package parquet

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"

	"github.com/xitongsys/parquet-go-source/buffer"
	"github.com/xitongsys/parquet-go/reader"

	"go.k6.io/k6/js/common"
	"go.k6.io/k6/js/modules"
	"go.k6.io/k6/experimental/js"
)

type RootModule struct{}
type ParquetModule struct{}

func init() {
	modules.Register("k6/x/parquet", New())
}

func New() modules.Module {
	return &RootModule{}
}

func (r *RootModule) NewModuleInstance(vu modules.VU) modules.Instance {
	return &ParquetModule{}
}

func (p *ParquetModule) Exports() modules.Exports {
	return modules.Exports{
		Default: p,
		Named: map[string]interface{}{
			"readParquetFromBytes": p.ReadParquetFromBytes,
		},
	}
}

// 🎯 Ez az exportált függvény JS-ből: readParquetFromBytes(Uint8Array)
func (p *ParquetModule) ReadParquetFromBytes(data js.Value) (interface{}, error) {
	// Típusellenőrzés
	if data.Type() != js.TypeObject || !data.InstanceOf(js.Global().Get("Uint8Array")) {
		return nil, errors.New("expected Uint8Array")
	}

	// Uint8Array -> Go []byte
	length := data.Get("length").Int()
	raw := make([]byte, length)
	js.CopyBytesToGo(raw, data)

	// Olvasás bufferből
	buf := bytes.NewReader(raw)
	fr, err := buffer.NewBufferFile(buf)
	if err != nil {
		return nil, err
	}
	pr, err := reader.NewParquetReader(fr, nil, 1)
	if err != nil {
		return nil, err
	}
	defer pr.ReadStop()

	num := int(pr.GetNumRows())
	rows, err := pr.ReadByNumber(num)
	if err != nil {
		return nil, err
	}

	// Sorokat konvertáljuk JS objektummá
	result, err := json.Marshal(rows)
	if err != nil {
		return nil, err
	}

	var output interface{}
	if err := json.Unmarshal(result, &output); err != nil {
		return nil, err
	}

	return output, nil
}
