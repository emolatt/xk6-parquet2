package parquet

import (
	"bytes"
	"encoding/json"

	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go-source/buffer"

	"go.k6.io/k6/js/modules"
	"github.com/dop251/goja"
)

type (
	RootModule    struct{}
	ParquetModule struct{}
)

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

// 📦 Uint8Array -> Go []byte -> Parquet olvasás -> JS Object
func (p *ParquetModule) ReadParquetFromBytes(call goja.FunctionCall) goja.Value {
	rt := call.This.Runtime()

	if len(call.Arguments) < 1 {
		panic(rt.NewTypeError("missing argument: Uint8Array"))
	}

	arg := call.Arguments[0]

	// Ellenőrizd hogy tényleg Uint8Array
	obj := arg.ToObject(rt)
	if obj.ClassName() != "Uint8Array" {
		panic(rt.NewTypeError("expected Uint8Array"))
	}

	length := obj.Get("length").ToInteger()
	data := make([]byte, length)

	for i := int64(0); i < length; i++ {
		v := obj.Get(i)
		data[i] = byte(v.ToInteger())
	}

	buf := bytes.NewReader(data)
	fr, err := buffer.NewBufferFile(buf)
	if err != nil {
		panic(rt.NewGoError(err))
	}
	pr, err := reader.NewParquetReader(fr, nil, 1)
	if err != nil {
		panic(rt.NewGoError(err))
	}
	defer pr.ReadStop()

	num := int(pr.GetNumRows())
	rows, err := pr.ReadByNumber(num)
	if err != nil {
		panic(rt.NewGoError(err))
	}

	jsonBytes, err := json.Marshal(rows)
	if err != nil {
		panic(rt.NewGoError(err))
	}

	var result interface{}
	if err := json.Unmarshal(jsonBytes, &result); err != nil {
		panic(rt.NewGoError(err))
	}

	return rt.ToValue(result)
}
