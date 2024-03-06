package usecase_test

import (
	"encoding/json"
	"testing"

	"github.com/m-mizutani/gt"
	"github.com/m-mizutani/swarm/pkg/usecase"
)

func TestClone(t *testing.T) {
	testCases := map[string]struct {
		src    any
		expect any
	}{
		"with nil field": {
			src: map[string]interface{}{
				"field": nil,
				"color": "blue",
			},
			expect: map[string]interface{}{
				"color": "blue",
			},
		},
		"nested nil field": {
			src: map[string]interface{}{
				"nested": map[string]interface{}{
					"sub": nil,
				},
				"color": "blue",
			},
			expect: map[string]interface{}{
				"nested": map[string]interface{}{},
				"color":  "blue",
			},
		},
		"empty slice": {
			src: map[string]interface{}{
				"array": []interface{}{},
				"color": "blue",
			},
			expect: map[string]interface{}{
				"color": "blue",
			},
		},
		"array with empty": {
			src:    [2]any{nil, "blue"},
			expect: [2]any{nil, "blue"},
		},
		"slice with empty": {
			src:    []any{nil, "blue"},
			expect: []any{"blue"},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			dst := usecase.CloneWithoutNil(tc.src)
			gt.Equal(t, dst, tc.expect)
		})
	}
}

func TestCloneFromJson(t *testing.T) {
	raw := `{
		"str": "blue",
		"field": null,
		"empty": []
	}`

	var src any
	gt.NoError(t, json.Unmarshal([]byte(raw), &src))
	dst := usecase.CloneWithoutNil(src)
	m := gt.Cast[map[string]any](t, dst)
	gt.M(t, m).HaveKey("str").NotHaveKey("field").NotHaveKey("empty")
}
