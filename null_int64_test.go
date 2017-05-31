package dosa

import (
	"testing"

	"encoding/json"

	"github.com/stretchr/testify/assert"
)

func TestNullInt(t *testing.T) {
	v := NewNullInt64(10)
	actual, err := v.Get()
	assert.NoError(t, err)
	assert.Equal(t, int64(10), actual)

	v.Set(20)
	actual, err = v.Get()
	assert.NoError(t, err)
	assert.Equal(t, int64(20), actual)

	v.Nullify()
	actual, err = v.Get()
	assert.Error(t, err, "Value is null")
}

func TestNullInt_MarshalText(t *testing.T) {
	v := NewNullInt64(10)
	bytes, err := v.MarshalText()
	assert.NoError(t, err)
	assert.Equal(t, "10", string(bytes))

	v.Nullify()
	bytes, err = v.MarshalText()
	assert.NoError(t, err)
	assert.Equal(t, "", string(bytes))
}

func TestNullInt_UnmarshalText(t *testing.T) {
	var v NullInt64
	err := v.UnmarshalText([]byte("10"))
	assert.NoError(t, err)

	value, err := v.Get()
	assert.NoError(t, err)
	assert.Equal(t, int64(10), value)

	err = v.UnmarshalText([]byte(""))
	assert.NoError(t, err)
	_, err = v.Get()
	assert.Equal(t, ErrNullValue, err)

	err = v.UnmarshalText([]byte("null"))
	assert.NoError(t, err)
	_, err = v.Get()
	assert.Equal(t, ErrNullValue, err)
}

func TestNullInt_MarshalJSON(t *testing.T) {
	v := NewNullInt64(-123)
	bytes, err := v.MarshalJSON()
	assert.NoError(t, err)
	assert.Equal(t, "-123", string(bytes))

	v.Nullify()
	bytes, err = v.MarshalJSON()
	assert.NoError(t, err)
	assert.Equal(t, "null", string(bytes))
}

func TestNullInt_UnmarshalJSON(t *testing.T) {
	var v NullInt64
	err := v.UnmarshalJSON([]byte("-12"))
	assert.NoError(t, err)
	value, err := v.Get()
	assert.NoError(t, err)
	assert.Equal(t, int64(-12), value)

	err = v.UnmarshalJSON([]byte("null"))
	assert.NoError(t, err)
	value, err = v.Get()
	assert.EqualError(t, err, "Value is null")

	// Invalid case
	badData, err := json.Marshal(float64(3.14))
	err = v.UnmarshalJSON(badData)
	assert.Error(t, err)
}