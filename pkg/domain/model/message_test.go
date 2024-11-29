package model_test

import (
	_ "embed"
	"encoding/json"
	"testing"

	"github.com/m-mizutani/gt"
	"github.com/secmon-lab/swarm/pkg/domain/model"
)

//go:embed testdata/cloud_storage_event.json
var cloudStorageEventRaw []byte

func TestCloudStorageEvent(t *testing.T) {
	var ev model.CloudStorageEvent

	gt.NoError(t, json.Unmarshal(cloudStorageEventRaw, &ev))
	obj := ev.ToObject()
	gt.Equal(t, obj.CS.Bucket, "mztn-sample-bucket")
	gt.Equal(t, obj.CS.Name, "mydir/GA1ZivRbQAAAyXs.jpg")
	gt.Equal(t, *obj.Size, int64(434358))
	gt.Equal(t, *obj.CreatedAt, int64(1708130907))
	gt.A(t, obj.Digests).Must().Length(1).At(0, func(t testing.TB, v model.Digest) {
		gt.Equal(t, v.Alg, "md5")
		gt.Equal(t, v.Value, "eb9b8a4296628acbbd90ff20065fb9d1")
	})
}
