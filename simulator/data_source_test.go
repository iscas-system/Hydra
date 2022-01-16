package simulator

import (
	"DES-go/schedulers/types"
	"testing"
)

func Test_datasource(t *testing.T) {
	initDataSource("/Users/purchaser/PycharmProjects/DES/cases/case_200.csv", nil)
	dataSourceInstance.IterBySubmitTime(func(indices []int, meta []types.JobMeta) {

	})
}
