package xk6parquet

import (
	"go.k6.io/k6/js/modules"
	"go.k6.io/k6/experimental/sobek"
)

// Ez a struktúra fogja tartalmazni az exportált JS függvényeket.
type RootModule struct{}

type Instance struct {
	rt *sobek.Runtime
}

func init() {
	modules.Register("k6/x/xk6-parquet2", NewModule)
}

func NewModule() modules.Module {
	return &RootModule{}
}

func (m *RootModule) NewModuleInstance(rt *sobek.Runtime) (modules.Instance, error) {
	return &Instance{rt: rt}, nil
}

func (i *Instance) Exports() map[string]any {
	return map[string]any{
		"hello": i.hello,
	}
}

func (i *Instance) hello() sobek.Value {
	return i.rt.ToValue("Szia a sobek-alapú modulból!")
}

