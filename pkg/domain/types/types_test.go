package types

import "testing"

func TestCSUrl_Parse(t *testing.T) {
	tests := []struct {
		name     string
		url      CSUrl
		expected CSBucket
		object   CSObjectID
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

func TestNewLogID(t *testing.T) {
	baseID := NewLogID("bucket1", "object1", 0)

	testCases := map[string]struct {
		bucket CSBucket
		objID  CSObjectID
		idx    int
		match  bool
	}{
		"test case 1": {
			bucket: "bucket1",
			objID:  "object1",
			idx:    0,
			match:  true,
		},
		"test case 2": {
			bucket: "bucket2",
			objID:  "object2",
			idx:    1,
			match:  false,
		},
		"test case 3": {
			bucket: "bucket1",
			objID:  "object1",
			idx:    1,
			match:  false,
		},
		"test case 4": {
			bucket: "bucket2",
			objID:  "object1",
			idx:    0,
			match:  false,
		},
		// Add more test cases as needed
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			result := NewLogID(tc.bucket, tc.objID, tc.idx)
			if tc.match && result != baseID {
				t.Errorf("unexpected result")
			}
		})
	}
}
