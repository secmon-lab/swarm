package types_test

import (
	"testing"

	"github.com/m-mizutani/gt"
	"github.com/secmon-lab/swarm/pkg/domain/types"
)

func TestCSUrl_Parse(t *testing.T) {
	tests := []struct {
		name     string
		url      types.CSUrl
		expected types.CSBucket
		object   types.CSObjectID
		wantErr  bool
	}{
		{
			name:     "Valid URL",
			url:      "gs://my-bucket/my-object",
			expected: "my-bucket",
			object:   "my-object",
			wantErr:  false,
		},
		{
			name:     "Valid URL with sub directory",
			url:      "gs://my-bucket/my-object/sub-dir",
			expected: "my-bucket",
			object:   "my-object/sub-dir",
			wantErr:  false,
		},
		{
			name:     "Invalid prefix",
			url:      "http://my-bucket/my-object",
			expected: "",
			object:   "",
			wantErr:  true,
		},
		{
			name:     "Invalid prefix format 1",
			url:      "gs:/my-bucket/my-object",
			expected: "",
			object:   "",
			wantErr:  true,
		},
		{
			name:     "Invalid prefix format 2",
			url:      "gs:///my-bucket",
			expected: "",
			object:   "",
			wantErr:  true,
		},
		{
			name:     "no object",
			url:      "gs://my-bucket",
			expected: "",
			object:   "",
			wantErr:  true,
		},
		{
			name:     "Invalid URL",
			url:      "invalid-url",
			expected: "",
			object:   "",
			wantErr:  true,
		},
		// Add more test cases here if needed
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bucket, object, err := tt.url.Parse()

			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if bucket != tt.expected {
				t.Errorf("Parse() bucket = %v, expected %v", bucket, tt.expected)
			}

			if object != tt.object {
				t.Errorf("Parse() object = %v, expected %v", object, tt.object)
			}
		})
	}
}

func TestNewLogIDIdempotent(t *testing.T) {
	testCases := map[string]struct {
		data any
	}{
		"map data": {
			data: map[string]string{
				"color":  "red",
				"number": "1",
			},
		},
		"string data": {
			data: "hello",
		},
		"array data": {
			data: []string{"a", "b", "c"},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			var ids []types.LogID
			for i := 0; i < 100; i++ {
				result := gt.R1(types.NewLogID(tc.data)).NoError(t)
				ids = append(ids, result)
			}

			for i := 1; i < len(ids); i++ {
				gt.V(t, ids[i]).Equal(ids[0])
			}
		})
	}
}
