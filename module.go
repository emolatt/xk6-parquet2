package parquet

import (
//	"go.k6.io/k6/js/common"
	"go.k6.io/k6/js/modules"
)

type RootModule struct{}

type ParquetModule struct{}

func init() {
	modules.Register("k6/x/parquet", New())
}

func New() modules.Module {
	return &RootModule{}
}

// Ez a példány lesz JS-ből elérhető
func (r *RootModule) NewModuleInstance(vu modules.VU) modules.Instance {
	return &ParquetModule{}
}

func (p *ParquetModule) Exports() modules.Exports {
	return modules.Exports{
		Default: p,
		Named: map[string]interface{}{
			"Hello": p.Hello,
		},
	}
}

// 🔹 Ez az 1 függvény amit most tesztelni fogunk
func (p *ParquetModule) Hello(name string) string {
	return "Szia, " + name + "!"
}

