package parquet

import (
	"encoding/json"
	"strconv"

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

func (p *ParquetModule) ReadParquetFromBytes(ctxPtrPtr interface{}, args ...interface{}) interface{} {
	modCtx := modules.GetContext(ctxPtrPtr)
	rt := modCtx.VU.Runtime()

	if len(args) < 1 {
		panic(rt.NewTypeError("missing argument: Uint8Array"))
	}

	val := args[0]
	obj, ok := val.(*goja.Object)
	if !ok || obj.ClassName() != "Uint8Array" {
		panic(rt.NewTypeError("expected Uint8Array"))
	}

	lengthVal := obj.Get("length")
	length := lengthVal.ToInteger()
	data := make([]byte, length)

	for i := int64(0); i < length; i++ {
		val := obj.Get(strconv.FormatInt(i, 10))
		data[i] = byte(val.ToInteger())
	}

	fr, err := buffer.NewBufferFile(data)
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

	return result
}
