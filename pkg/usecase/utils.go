package usecase

import (
	"bytes"
	"encoding/json"
	"reflect"
	"unsafe"

	"cloud.google.com/go/bigquery"
	"github.com/m-mizutani/goerr"
)

func cloneWithoutNil(src interface{}) interface{} {
	return clone("", reflect.ValueOf(src)).Interface()
}

func clone(fieldName string, src reflect.Value) reflect.Value {
	if src.Kind() == reflect.Ptr && src.IsNil() {
		return reflect.New(src.Type()).Elem()
	}

	switch src.Kind() {
	case reflect.String:
		dst := reflect.New(src.Type())
		dst.Elem().SetString(src.String())
		return dst.Elem()

	case reflect.Struct:
		dst := reflect.New(src.Type())
		t := src.Type()

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

			copied := clone(f.Name, srcValue)
			dstValue.Set(copied)
		}
		return dst.Elem()

	case reflect.Map:
		dst := reflect.MakeMap(src.Type())
		keys := src.MapKeys()
		for i := 0; i < src.Len(); i++ {
			mValue := src.MapIndex(keys[i])
			fieldName := keys[i].String()
			if mValue.IsNil() {
				continue
			}
			dst.SetMapIndex(keys[i], clone(fieldName, mValue))
		}
		return dst

	case reflect.Slice:
		dst := reflect.MakeSlice(src.Type(), src.Len(), src.Cap())
		for i := 0; i < src.Len(); i++ {
			dst.Index(i).Set(clone(fieldName, src.Index(i)))
		}
		return dst

	case reflect.Array:
		if src.Len() == 0 {
			return src // can not access to src.Index(0)
		}

		dst := reflect.New(src.Type()).Elem()
		for i := 0; i < src.Len(); i++ {
			dst.Index(i).Set(clone(fieldName, src.Index(i)))
		}
		return dst

	case reflect.Ptr:
		dst := reflect.New(src.Elem().Type())
		copied := clone(fieldName, src.Elem())
		dst.Elem().Set(copied)
		return dst

	case reflect.Interface:
		return clone(fieldName, src.Elem())

	default:
		dst := reflect.New(src.Type())
		dst.Elem().Set(src)
		return dst.Elem()
	}
}

func schemaToJSON(schema bigquery.Schema) (string, error) {
	jsonSchema, err := schema.ToJSONFields()
	if err != nil {
		return "", goerr.Wrap(err, "failed to convert schema to JSON").With("schema", schema)
	}

	var out bytes.Buffer
	if err := json.Compact(&out, jsonSchema); err != nil {
		return "", goerr.Wrap(err, "failed to compact JSON").With("schema", schema).With("json", string(jsonSchema))
	}

	return out.String(), nil
}
