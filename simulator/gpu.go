package simulator

import "fmt"

type GPUType string
type GPUID int

type GPU struct {
	GPUID   GPUID   `json:"gpu_id"`
	GPUType GPUType `json:"gpu_type"`
}

func NewGPU(gpuID GPUID, gpuType GPUType) *GPU {
	return &GPU{
		GPUID:   gpuID,
		GPUType: gpuType,
	}
}

func (g *GPU) ID() GPUID {
	return g.GPUID
}

func (g *GPU) Type() GPUType {
	return g.GPUType
}

func (g *GPU) String() string {
	return fmt.Sprintf("gpu=[ID=%d, Type=%s]", g.ID(), g.Type())
}
