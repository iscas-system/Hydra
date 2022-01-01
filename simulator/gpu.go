package simulator

import (
	"DES-go/schedulers/types"
	"fmt"
)

//type GPUType string
//type GPUID int

type GPU struct {
	gpuID   types.GPUID
	gpuType types.GPUType
}

func NewGPU(gpuID types.GPUID, gpuType types.GPUType) *GPU {
	return &GPU{
		gpuID:   gpuID,
		gpuType: gpuType,
	}
}

func (g *GPU) ID() types.GPUID {
	return g.gpuID
}

func (g *GPU) Type() types.GPUType {
	return g.gpuType
}

func (g *GPU) String() string {
	return fmt.Sprintf("gpu=[ID=%d, Type=%s]", g.ID(), g.Type())
}
