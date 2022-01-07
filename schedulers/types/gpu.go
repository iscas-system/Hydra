package types

type GPUType string
type GPUID int

type GPU interface {
	ID() GPUID
	Type() GPUType
	String() string
}
