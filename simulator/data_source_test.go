package simulator

import (
	"testing"
)

func Test_datasource(t *testing.T) {
	InitDataSource("/Users/purchaser/PycharmProjects/DES/cases/case_200.csv")
	dataSourceInstance.IterBySubmitTime(func(indices []int, meta []*JobMeta) {

	})
}
