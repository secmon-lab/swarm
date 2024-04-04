package usecase

import (
	"bytes"
	"encoding/json"
	"reflect"
	"unsafe"

	"cloud.google.com/go/bigquery"
	"github.com/m-mizutani/goerr"
	"github.com/m-mizutani/swarm/pkg/domain/types"
)

func cloneWithoutNil(src interface{}) interface{} {
	resp, _ := clone("", reflect.ValueOf(src))
	return resp.Interface()
}

func clone(fieldName string, src reflect.Value) (reflect.Value, bool) {
	if src.Kind() == reflect.Ptr && src.IsNil() {
		return reflect.New(src.Type()).Elem(), true
	}

	switch src.Kind() {
	case reflect.String:
		dst := reflect.New(src.Type())
		dst.Elem().SetString(src.String())
		return dst.Elem(), true

	case reflect.Struct:
		dst := reflect.New(src.Type())
		t := src.Type()

		if t.NumField() == 0 {
			return src, false
		}

		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			srcValue := src.Field(i)
			dstValue := dst.Elem().Field(i)

			if srcValue.Kind() == reflect.Ptr && srcValue.IsNil() {
				continue
			}

			if !srcValue.CanInterface() {
				dstValue = reflect.NewAt(dstValue.Type(), unsafe.Pointer(dstValue.UnsafeAddr())).Elem()

				if !srcValue.CanAddr() {
					switch {
					case srcValue.CanInt():
						dstValue.SetInt(srcValue.Int())
					case srcValue.CanUint():
						dstValue.SetUint(srcValue.Uint())
					case srcValue.CanFloat():
						dstValue.SetFloat(srcValue.Float())
					case srcValue.CanComplex():
						dstValue.SetComplex(srcValue.Complex())
					case srcValue.Kind() == reflect.Bool:
						dstValue.SetBool(srcValue.Bool())
					}

					continue
				}

				srcValue = reflect.NewAt(srcValue.Type(), unsafe.Pointer(srcValue.UnsafeAddr())).Elem()
			}

			if copied, ok := clone(f.Name, srcValue); ok {
				dstValue.Set(copied)
			}
		}
		return dst.Elem(), true

	case reflect.Map:
		if src.Len() == 0 {
			return src, false
		}

		dst := reflect.MakeMap(src.Type())
		keys := src.MapKeys()
		for i := 0; i < src.Len(); i++ {
			mValue := src.MapIndex(keys[i])
			fieldName := keys[i].String()
			if mValue.IsNil() {
				continue
			}
			if v, ok := clone(fieldName, mValue); ok {
				dst.SetMapIndex(keys[i], v)
			}
		}
		return dst, true

	case reflect.Slice:
		var arr []reflect.Value
		for i := 0; i < src.Len(); i++ {
			if v, ok := clone(fieldName, src.Index(i)); ok {
				arr = append(arr, v)
			}
		}

		if len(arr) == 0 {
			return src, false
		}
		dst := reflect.MakeSlice(src.Type(), len(arr), len(arr))
		for i, v := range arr {
			dst.Index(i).Set(v)
		}

		return dst, true

	case reflect.Array:
		if src.Len() == 0 {
			return src, false
		}

		dst := reflect.New(src.Type()).Elem()
		var count int
		for i := 0; i < src.Len(); i++ {
			v, ok := clone(fieldName, src.Index(i))
			if ok {
				count++
				dst.Index(i).Set(v)
			}
		}

		if count == 0 {
			return src, false
		}
		return dst, true

	case reflect.Ptr, reflect.UnsafePointer:
		dst := reflect.New(src.Elem().Type())
		copied, ok := clone(fieldName, src.Elem())
		if !ok {
			return src, false
		}
		dst.Elem().Set(copied)
		return dst, true

	case reflect.Interface:
		return clone(fieldName, src.Elem())

	case reflect.Invalid:
		return src, false

	default:
		typ := src.Type()
		dst := reflect.New(typ)
		dst.Elem().Set(src)
		return dst.Elem(), true
	}
}

func schemaToJSON(schema bigquery.Schema) (string, error) {
	jsonSchema, err := schema.ToJSONFields()
	if err != nil {
		return "", goerr.Wrap(err, "failed to convert schema to JSON")
	}

	var out bytes.Buffer
	if err := json.Compact(&out, jsonSchema); err != nil {
		return "", goerr.Wrap(err, "failed to compact JSON")
	}

	return out.String(), nil
}

func buildBQMetadata(schema bigquery.Schema, pt types.BQPartition) (*bigquery.TableMetadata, error) {
	tpMap := map[types.BQPartition]bigquery.TimePartitioningType{
		types.BQPartitionHour:  bigquery.HourPartitioningType,
		types.BQPartitionDay:   bigquery.DayPartitioningType,
		types.BQPartitionMonth: bigquery.MonthPartitioningType,
		types.BQPartitionYear:  bigquery.YearPartitioningType,
	}

	md := &bigquery.TableMetadata{
		Schema: schema,
	}

	if pt != types.BQPartitionNone {
		if t, ok := tpMap[pt]; ok {
			md.TimePartitioning = &bigquery.TimePartitioning{
				Field: "timestamp",
				Type:  t,
			}
		} else {
			return nil, goerr.Wrap(types.ErrInvalidPolicyResult, "invalid time unit").With("Partition", pt)
		}
	}

	return md, nil
}
