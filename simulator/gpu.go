package simulator

import "fmt"

type GPUType string
type GPUID int

type GPU struct {
	gpuID   GPUID
	gpuType GPUType
}

func NewGPU(gpuID GPUID, gpuType GPUType) *GPU {
	return &GPU{
		gpuID:   gpuID,
		gpuType: gpuType,
	}
}

func (g *GPU) ID() GPUID {
	return g.gpuID
}

func (g *GPU) Type() GPUType {
	return g.gpuType
}

func (g *GPU) String() string {
	return fmt.Sprintf("gpu=[ID=%d, Type=%s]", g.ID(), g.Type())
}
