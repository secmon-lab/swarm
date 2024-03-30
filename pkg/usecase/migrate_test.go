package usecase_test

import (
	"context"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/m-mizutani/gt"
	"github.com/m-mizutani/swarm/pkg/domain/model"
	"github.com/m-mizutani/swarm/pkg/infra"
	"github.com/m-mizutani/swarm/pkg/infra/bq"
	"github.com/m-mizutani/swarm/pkg/usecase"
)

func TestMigrate(t *testing.T) {
	src := model.BigQueryDest{
		Dataset: "src_dataset",
		Table:   "src_table",
	}
	dst := model.BigQueryDest{
		Dataset: "dst_dataset",
		Table:   "dst_table",
	}

	testCases := map[string]struct {
		metadata   []*bigquery.TableMetadata
		queryCount int
		isErr      bool
		testMock   func(*bq.GeneralMock)
	}{
		"migrate to new table": {
			metadata: []*bigquery.TableMetadata{
				{
					Schema: []*bigquery.FieldSchema{
						{Name: "name", Type: bigquery.StringFieldType},
						{Name: "age", Type: bigquery.IntegerFieldType},
					},
				},
			},
			queryCount: 1,
			isErr:      false,
			testMock: func(mock *bq.GeneralMock) {
				gt.A(t, mock.CreatedTable).Length(1)
				gt.Equal(t, mock.CreatedTable[0].Dataset, "dst_dataset")
				gt.Equal(t, mock.CreatedTable[0].Table, "dst_table")
				schema := mock.CreatedTable[0].MD.Schema
				gt.A(t, schema).Length(2).At(0, func(t testing.TB, v *bigquery.FieldSchema) {
					gt.Equal(t, v.Name, "name")
					gt.Equal(t, v.Type, bigquery.StringFieldType)
				}).At(1, func(t testing.TB, v *bigquery.FieldSchema) {
					gt.Equal(t, v.Name, "age")
					gt.Equal(t, v.Type, bigquery.IntegerFieldType)
				})
			},
		},
		"migrate to existing table, schema is same": {
			metadata: []*bigquery.TableMetadata{
				{
					Schema: []*bigquery.FieldSchema{
						{Name: "name", Type: bigquery.StringFieldType},
						{Name: "age", Type: bigquery.IntegerFieldType},
					},
				},
				{
					Schema: []*bigquery.FieldSchema{
						{Name: "name", Type: bigquery.StringFieldType},
						{Name: "age", Type: bigquery.IntegerFieldType},
					},
				},
			},
			queryCount: 1,
			isErr:      false,
			testMock: func(mock *bq.GeneralMock) {
				gt.A(t, mock.UpdatedTable).Length(0)
			},
		},
		"migrate to existing table, schema is different": {
			metadata: []*bigquery.TableMetadata{
				{
					Schema: []*bigquery.FieldSchema{
						{Name: "name", Type: bigquery.StringFieldType},
						{Name: "age", Type: bigquery.IntegerFieldType},
					},
				},

				{
					Schema: []*bigquery.FieldSchema{
						{Name: "name", Type: bigquery.StringFieldType},
						{Name: "address", Type: bigquery.StringFieldType},
					},
					ETag: "xxx",
				},
			},
			queryCount: 1,
			isErr:      false,
			testMock: func(mock *bq.GeneralMock) {
				gt.A(t, mock.UpdatedTable).Length(1)
				gt.Equal(t, mock.UpdatedTable[0].Dataset, "dst_dataset")
				gt.Equal(t, mock.UpdatedTable[0].Table, "dst_table")
				gt.Equal(t, mock.UpdatedTable[0].ETag, "xxx")

				schema := mock.UpdatedTable[0].MD.Schema
				gt.A(t, schema).Length(3).At(0, func(t testing.TB, v *bigquery.FieldSchema) {
					gt.Equal(t, v.Name, "name")
					gt.Equal(t, v.Type, bigquery.StringFieldType)
				}).At(1, func(t testing.TB, v *bigquery.FieldSchema) {
					gt.Equal(t, v.Name, "address")
					gt.Equal(t, v.Type, bigquery.StringFieldType)
				}).At(2, func(t testing.TB, v *bigquery.FieldSchema) {
					gt.Equal(t, v.Name, "age")
					gt.Equal(t, v.Type, bigquery.IntegerFieldType)
				})
			},
		},
		"conflict schema": {
			metadata: []*bigquery.TableMetadata{
				{
					Schema: []*bigquery.FieldSchema{
						{Name: "name", Type: bigquery.StringFieldType},
						{Name: "age", Type: bigquery.IntegerFieldType},
					},
				},
				{
					Schema: []*bigquery.FieldSchema{
						{Name: "name", Type: bigquery.StringFieldType},
						{Name: "age", Type: bigquery.StringFieldType},
					},
				},
			},
			queryCount: 0,
			isErr:      true,
		},
	}

	for title, tc := range testCases {
		t.Run(title, func(t *testing.T) {
			mock := bq.NewGeneralMock()
			mock.Metadata = tc.metadata

			uc := usecase.New(infra.New(infra.WithBigQuery(mock)))
			ctx := context.Background()

			err := uc.Migrate(ctx, &src, &dst, "SELECT * FROM src_table")
			if tc.isErr {
				gt.Error(t, err)
				return
			}

			gt.A(t, mock.Queries).Length(tc.queryCount)
			if tc.testMock != nil {
				tc.testMock(mock)
			}
		})
	}
}
