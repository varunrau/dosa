// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package blobref_test


import (
	"context"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/dosa"
	"github.com/uber-go/dosa/connectors/blobref"
	"github.com/uber-go/dosa/mocks"
	"testing"
	"bytes"
	"net/url"
	"errors"
)

var testSchemaRef = dosa.SchemaRef{
	Scope:      "scope1",
	NamePrefix: "namePrefix",
	EntityName: "eName",
	Version:    12345,
}

var noEblobEi = &dosa.EntityInfo{
	Ref: &testSchemaRef,
	Def: &dosa.EntityDefinition{
		Columns: []*dosa.ColumnDefinition{
			{Name: "p1", Type: dosa.String},
			{Name: "c1", Type: dosa.Int64},
		},
		Key: &dosa.PrimaryKey{
			PartitionKeys: []string{"p1"},
		},
		Name: "t1",
		Indexes: map[string]*dosa.IndexDefinition{
			"i1": {Key: &dosa.PrimaryKey{PartitionKeys: []string{"c1"}}}},
	},
}

var eBlobEi = &dosa.EntityInfo{
	Ref: &testSchemaRef,
	Def: &dosa.EntityDefinition{
		Columns: []*dosa.ColumnDefinition{
			{Name: "p1", Type: dosa.String},
			{Name: "c1", Type: dosa.EBlob},
		},
		Key: &dosa.PrimaryKey{
			PartitionKeys: []string{"p1"},
		},
		Name: "t2",
	},
}

func getMockedConnector(ctrl *gomock.Controller) *blobref.Connector {

	store := NewMockBlobStore(ctrl)
	downstream := mocks.NewMockConnector(ctrl)
	return blobref.NewConnector(downstream, store)
}

func TestConnector_Read_passthrough(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	sut := getMockedConnector(ctrl)
	sut.Next.(*mocks.MockConnector).EXPECT().Read(context.TODO(), noEblobEi, map[string]dosa.FieldValue{"p1": "test"}, dosa.All()).Return(nil, nil)

	res, err := sut.Read(context.TODO(), noEblobEi, map[string]dosa.FieldValue{"p1": "test"}, dosa.All())
	assert.NoError(t, err)
	assert.Nil(t, res)
}

func TestConnector_Read(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	sut := getMockedConnector(ctrl)

	testUrl, _ := url.Parse("tpath://test/example/example")

	sut.Next.(*mocks.MockConnector).EXPECT().Read(context.TODO(), gomock.Not(eBlobEi), map[string]dosa.FieldValue{"p1": "test"}, dosa.All()).
	Do(func(_ context.Context, ei *dosa.EntityInfo, _ map[string]dosa.FieldValue, _ []string) {
		assert.Equal(t, dosa.String, ei.Def.ColumnMap()["c1"].Type)
	}).Return(map[string]dosa.FieldValue{"c1": testUrl.String()}, nil)

	sut.Store.(*MockBlobStore).EXPECT().Read(context.TODO(), gomock.Eq(testUrl)).Return(bytes.NewReader([]byte{'a'}), nil)

	res, err := sut.Read(context.TODO(), eBlobEi, map[string]dosa.FieldValue{"p1": "test"}, dosa.All())
	assert.NoError(t, err)
	c1res := res["c1"].(dosa.BlobRef)

	assert.Equal(t, *testUrl, c1res.Reference) // this was the returned reference
	data := []byte{0}
	_, err = c1res.Channel.Read(data) // read the bytes from the channel
	assert.NoError(t, err)
	assert.Equal(t, []byte{'a'}, data) // should be the 'a' we passed in earlier
}

func TestConnector_Read_BadURL(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	sut := getMockedConnector(ctrl)
	sut.Next.(*mocks.MockConnector).EXPECT().Read(context.TODO(), gomock.Not(eBlobEi), map[string]dosa.FieldValue{"p1": "test"}, dosa.All()).
		Do(func(_ context.Context, ei *dosa.EntityInfo, _ map[string]dosa.FieldValue, _ []string) {
		assert.Equal(t, dosa.String, ei.Def.ColumnMap()["c1"].Type)
	}).Return(map[string]dosa.FieldValue{"c1": "%20://badscheme"}, nil)

	res, err := sut.Read(context.TODO(), eBlobEi, map[string]dosa.FieldValue{"p1": "test"}, dosa.All())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Invalid URL")
	assert.Contains(t, err.Error(), "%20://badscheme")
	assert.Contains(t, err.Error(), "p1")
	assert.Contains(t, err.Error(), "test")

	assert.Nil(t, res)
}

func TestConnector_Read_ExternalFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	sut := getMockedConnector(ctrl)
	sut.Next.(*mocks.MockConnector).EXPECT().Read(context.TODO(), gomock.Not(eBlobEi), map[string]dosa.FieldValue{"p1": "test"}, dosa.All()).
		Do(func(_ context.Context, ei *dosa.EntityInfo, _ map[string]dosa.FieldValue, _ []string) {
		assert.Equal(t, dosa.String, ei.Def.ColumnMap()["c1"].Type)
	}).Return(map[string]dosa.FieldValue{"c1": "tpath://thisisfine"}, nil)

	sut.Store.(*MockBlobStore).EXPECT().Read(context.TODO(), gomock.Any()).Return(nil, errors.New("external failure"))

	_, err := sut.Read(context.TODO(), eBlobEi, map[string]dosa.FieldValue{"p1": "test"}, dosa.All())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "external failure")
}

func TestConnector_Shutdown(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	sut := getMockedConnector(ctrl)
	sut.Store.(*MockBlobStore).EXPECT().Shutdown().Return(nil)

	err := sut.Shutdown()
	assert.NoError(t, err)
}

func TestConnector_Panics(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	sut := getMockedConnector(ctrl)

	assert.Panics(t, func() { sut.MultiRead(context.TODO(), nil, nil, nil)})
	assert.Panics(t, func() { sut.MultiRemove(context.TODO(), nil, nil)})
	assert.Panics(t, func() { sut.MultiUpsert(context.TODO(), nil, nil)})
}

func TestConnector_Upsert(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	sut := getMockedConnector(ctrl)
	testUrl, _ := url.Parse("tblob://url")
	sut.Store.(*MockBlobStore).EXPECT().Write(context.TODO(), gomock.Any(), gomock.Any(), gomock.Any(), nil).Return(testUrl, nil)
	sut.Next.(*mocks.MockConnector).EXPECT().Upsert(context.TODO(), gomock.Any(), map[string]dosa.FieldValue{"p1": "test", "c1": testUrl.String()})

	err := sut.Upsert(context.TODO(), eBlobEi, map[string]dosa.FieldValue{"p1": "test", "c1": dosa.BlobRef{}})
	assert.NoError(t, err)
}
func TestConnector_Upsert_Failure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	sut := getMockedConnector(ctrl)
	sut.Store.(*MockBlobStore).EXPECT().Write(context.TODO(), gomock.Any(), gomock.Any(), gomock.Any(), nil).Return(nil, errors.New("error"))

	err := sut.Upsert(context.TODO(), eBlobEi, map[string]dosa.FieldValue{"p1": "test", "c1": dosa.BlobRef{}})
	assert.Error(t, err)
	assert.Contains(t, "error", err.Error())
}

func TestConnector_Remove(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	sut := getMockedConnector(ctrl)
	sut.Store.(*MockBlobStore).EXPECT().Remove(context.TODO(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	sut.Next.(*mocks.MockConnector).EXPECT().Remove(context.TODO(), gomock.Any(), gomock.Any())

	err := sut.Remove(context.TODO(), eBlobEi, map[string]dosa.FieldValue{"p1": "test"})
	assert.NoError(t, err)
}

func TestConnector_RemoveRange(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	sut := getMockedConnector(ctrl)

	err := sut.RemoveRange(context.TODO(), eBlobEi, map[string][]*dosa.Condition{})
	assert.Error(t, err)
    assert.Contains(t, err.Error(), "not supported")
}

func TestConnector_CreateIfNotExists(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	sut := getMockedConnector(ctrl)

	err := sut.CreateIfNotExists(context.TODO(), eBlobEi, map[string]dosa.FieldValue{"p1": "test", "c1": dosa.BlobRef{}})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "blob")
}

func TestConnector_Range(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	sut := getMockedConnector(ctrl)

	sut.Next.(*mocks.MockConnector).EXPECT().Range(context.TODO(), gomock.Any(), gomock.Any(), dosa.All(), "", 1).Return(nil, "", nil)


	data, token, err := sut.Range(context.TODO(), eBlobEi, map[string][]*dosa.Condition{"c1": []*dosa.Condition{}}, dosa.All(), "", 1)
    assert.NoError(t, err)
    assert.Nil(t, data)
    assert.Empty(t, token)
}

func TestConnector_Scan(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	sut := getMockedConnector(ctrl)

	sut.Next.(*mocks.MockConnector).EXPECT().Scan(context.TODO(), gomock.Any(), dosa.All(), "", 1).Return(nil, "", nil)


	data, token, err := sut.Scan(context.TODO(), eBlobEi, dosa.All(), "", 1)
	assert.NoError(t, err)
	assert.Nil(t, data)
	assert.Empty(t, token)

}