package etf

import (
	"fmt"
	"reflect"
	"strings"
)

type cacheFlag struct {
	isNew      bool
	segmentIdx uint8
}

type atomCacheRef struct {
	idx  uint8
	text *string
}

type Context struct {
	atomCache    [2048]*string
	currentCache []*string
}

type Term interface{}
type Tuple []Term
type List []Term
type Atom string
type Map map[Term]Term

type Pid struct {
	Node     Atom
	Id       uint32
	Serial   uint32
	Creation byte
}

type Port struct {
	Node     Atom
	Id       uint32
	Creation byte
}

type Ref struct {
	Node     Atom
	Creation byte
	Id       []uint32
}

type Function struct {
	Arity     byte
	Unique    [16]byte
	Index     uint32
	Free      uint32
	Module    Atom
	OldIndex  uint32
	OldUnique uint32
	Pid       Pid
	FreeVars  []Term
}

var (
	MapType = reflect.TypeOf(Map{})
)

func StringTerm(t Term) (s string, ok bool) {
	ok = true
	switch x := t.(type) {
	case Atom:
		s = string(x)
	case string:
		s = x
	case []byte:
		s = string(x)
	default:
		ok = false
	}

	return
}

type Export struct {
	Module   Atom
	Function Atom
	Arity    byte
}

// Erlang external term tags.
const (
	ettAtom          = 'd'
	ettAtomUTF8      = 'v' // this is beyond retarded
	ettBinary        = 'm'
	ettBitBinary     = 'M'
	ettCachedAtom    = 'C'
	ettCacheRef      = 'R'
	ettExport        = 'q'
	ettFloat         = 'c'
	ettFun           = 'u'
	ettInteger       = 'b'
	ettLargeBig      = 'o'
	ettLargeTuple    = 'i'
	ettList          = 'l'
	ettNewCache      = 'N'
	ettNewFloat      = 'F'
	ettNewFun        = 'p'
	ettNewRef        = 'r'
	ettNil           = 'j'
	ettPid           = 'g'
	ettPort          = 'f'
	ettRef           = 'e'
	ettSmallAtom     = 's'
	ettSmallAtomUTF8 = 'w' // this is beyond retarded
	ettSmallBig      = 'n'
	ettSmallInteger  = 'a'
	ettSmallTuple    = 'h'
	ettString        = 'k'
	ettMap           = 't'
)

const (
	// Erlang external term format version
	EtVersion = byte(131)
)

const (
	// Erlang distribution header
	EtDist = byte('D')
)

var tagNames = map[byte]string{
	ettAtom:          "ATOM_EXT",
	ettAtomUTF8:      "ATOM_UTF8_EXT",
	ettBinary:        "BINARY_EXT",
	ettBitBinary:     "BIT_BINARY_EXT",
	ettCachedAtom:    "ATOM_CACHE_REF",
	ettExport:        "EXPORT_EXT",
	ettFloat:         "FLOAT_EXT",
	ettFun:           "FUN_EXT",
	ettInteger:       "INTEGER_EXT",
	ettLargeBig:      "LARGE_BIG_EXT",
	ettLargeTuple:    "LARGE_TUPLE_EXT",
	ettList:          "LIST_EXT",
	ettMap:           "MAP_EXT",
	ettNewCache:      "NEW_CACHE_EXT",
	ettNewFloat:      "NEW_FLOAT_EXT",
	ettNewFun:        "NEW_FUN_EXT",
	ettNewRef:        "NEW_REFERENCE_EXT",
	ettNil:           "NIL_EXT",
	ettPid:           "PID_EXT",
	ettPort:          "PORT_EXT",
	ettRef:           "REFERENCE_EXT",
	ettSmallAtom:     "SMALL_ATOM_EXT",
	ettSmallAtomUTF8: "SMALL_ATOM_UTF8_EXT",
	ettSmallBig:      "SMALL_BIG_EXT",
	ettSmallInteger:  "SMALL_INTEGER_EXT",
	ettSmallTuple:    "SMALL_TUPLE_EXT",
	ettString:        "STRING_EXT",
}

func (t Tuple) Element(i int) Term {
	return t[i-1]
}

func tagName(t byte) (name string) {
	name = tagNames[t]
	if name == "" {
		name = fmt.Sprintf("%d", t)
	}
	return
}

func TermIntoStruct(term Term, dest interface{}) error {
	v := reflect.Indirect(reflect.ValueOf(dest))
	return termIntoStruct(term, v)

}

func termIntoStruct(term Term, destV reflect.Value) error {
	destType := destV.Type()

	switch x := term.(type) {
	case Atom:
		if destType.Kind() != reflect.String && (x == "" || x == "nil") {
			return intSwitch(0, destV, destType)
		}
		if destType.Kind() != reflect.String {
			return NewInvalidTypesError(destType, term)
		}

		destV.SetString(string(x))
	case []byte:
		if destType.Kind() == reflect.String {
			destV.SetString(string(x))
		} else if destType == reflect.SliceOf(reflect.TypeOf(byte(1))) {
			destV.Set(reflect.ValueOf(x))
		} else {
			return NewInvalidTypesError(destType, term)
		}
	case Map:
		return setMapField(x, destV, destType)
	case List:
		return setListField(x, destV, destType)
	default:
		return intSwitch(term, destV, destType)
	}

	return nil
}
func intSwitch(term Term, destV reflect.Value, destType reflect.Type) error {
	switch x := term.(type) {
	case int:
		return setIntField(int64(x), destV, destType)
	case int64:
		return setIntField(x, destV, destType)
	case uint:
		return setUIntField(uint64(x), destV, destType)
	case uint64:
		return setUIntField(x, destV, destType)
	default:
		return fmt.Errorf("Unknown term %s, %v", reflect.TypeOf(term).Name(), x)
	}
}

func setListField(v List, field reflect.Value, t reflect.Type) error {
	if t.Kind() != reflect.Slice {
		return NewInvalidTypesError(t, v)
	}

	sliceV := reflect.MakeSlice(t, len(v), len(v))
	for i, elem := range v {
		sliceElement := sliceV.Index(i)
		err := termIntoStruct(elem, sliceElement)
		if err != nil {
			return err
		}
	}
	field.Set(sliceV)
	return nil
}

func setMapField(v Map, field reflect.Value, t reflect.Type) error {
	if t.Kind() == reflect.Map {
		return setMapMapField(v, field, t)
	} else if t.Kind() == reflect.Struct {
		return setMapStructField(v, field, t)
	} else if t.Kind() == reflect.Interface {
		// TODO... do this a better way
		field.Set(reflect.ValueOf(v))
		return nil
	}

	return NewInvalidTypesError(t, v)
}

func setMapStructField(v Map, st reflect.Value, t reflect.Type) error {
	numField := t.NumField()
	fields := make([]reflect.StructField, numField)
	for i, _ := range fields {
		fields[i] = t.Field(i)
	}

	for key, val := range v {
		fName, ok := StringTerm(key)
		if !ok {
			return &InvalidStructKeyError{Term: key}
		}
		index, _ := findStructField(fields, fName)
		if index == -1 {
			continue
		}

		err := termIntoStruct(val, st.Field(index))
		if err != nil {
			return err
		}
	}

	return nil
}

func findStructField(fields []reflect.StructField, key string) (index int, structField reflect.StructField) {
	index = -1
	for i, f := range fields {
		tag := f.Tag.Get("json")
		split := strings.Split(tag, ",")
		if len(split) > 0 && split[0] != "" {
			if split[0] == key {
				return i, f
			}
		} else {
			if strings.EqualFold(f.Name, key) {
				structField = f
				index = i
			}
		}
	}

	return
}

func setMapMapField(v Map, field reflect.Value, t reflect.Type) error {
	for key, val := range v {
		field.SetMapIndex(reflect.ValueOf(key), reflect.ValueOf(val))
	}
	return nil
}

func setIntField(v int64, field reflect.Value, t reflect.Type) error {

	switch t.Kind() {
	case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int:
		field.SetInt(int64(v))
	case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint:
		field.SetUint(uint64(v))
	default:
		return NewInvalidTypesError(field.Type(), v)
	}

	return nil
}

func setUIntField(v uint64, field reflect.Value, t reflect.Type) error {

	switch t.Kind() {
	case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int:
		field.SetInt(int64(v))
	case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint:
		field.SetUint(uint64(v))
	default:
		return NewInvalidTypesError(field.Type(), v)
	}

	return nil
}

type StructPopulatorError struct {
	Type reflect.Type
	Term Term
}

func (s *StructPopulatorError) Error() string {
	return fmt.Sprintf("Cannot put %v into go value of type %s", s.Term, s.Type.Kind().String())
}

func NewInvalidTypesError(t reflect.Type, term Term) error {
	return &StructPopulatorError{
		Type: t,
		Term: term,
	}
}

type InvalidStructKeyError struct {
	Term Term
}

func (s *InvalidStructKeyError) Error() string {
	return fmt.Sprintf("Cannot use %s as struct field name", reflect.TypeOf(s.Term).Name())
}
