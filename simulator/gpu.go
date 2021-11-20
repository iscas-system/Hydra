package simulator

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
