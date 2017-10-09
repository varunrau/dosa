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

package blobref

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/uber-go/dosa"
	"github.com/uber-go/dosa/connectors/base"
	"io"
	"net/url"
)

//go:generate mockgen -destination=blobref_mocks_test.go -package=blobref_test github.com/uber-go/dosa/connectors/blobref BlobStore

func NewConnector(downstream dosa.Connector, store BlobStore) *Connector {
	return &Connector{Connector: base.Connector{downstream}, Store: store}
}

type Connector struct {
	base.Connector
	Store BlobStore
}

type BlobStore interface {
	Write(ctx context.Context, ei *dosa.EntityInfo, cd *dosa.ColumnDefinition, keys map[string]dosa.FieldValue, reader io.Reader) (*url.URL, error)
	Read(ctx context.Context, url *url.URL) (io.Reader, error)
	Remove(ctx context.Context, ei *dosa.EntityInfo, cd *dosa.ColumnDefinition, keys map[string]dosa.FieldValue) error
	Shutdown() error
}

// change all external blob types into strings, returning a new ei
// and TRUE if there were blobs, false otherwise

func scrubEi(ei *dosa.EntityInfo) (*dosa.EntityInfo, bool) {

	blobOffset := -1
	// pass 1: any blobs?
	for i, col := range ei.Def.Columns {
		if col.Type == dosa.EBlob {
			blobOffset = i
			break
		}
	}
	// all done if there are no blobs
	if blobOffset == -1 {
		return ei, false
	}

	// pass 2: change the type from EBlob to String for the downstream calls
	newEi := &dosa.EntityInfo{Ref: ei.Ref, Def: ei.Def.Clone()}
	for i := blobOffset; i < len(newEi.Def.Columns); i++ {
		if newEi.Def.Columns[i].Type == dosa.EBlob {
			newEi.Def.Columns[i].Type = dosa.String
		}
	}
	return newEi, true
}

func blobs(ei *dosa.EntityInfo) []*dosa.ColumnDefinition {
	var res []*dosa.ColumnDefinition
	for _, col := range ei.Def.Columns {
		if col.Type == dosa.EBlob {
			res = append(res, col)
		}
	}
	return res
}

// resolveBlobs changes the values in the map to a dosa.BlobRef structure, which gives a reader back to the caller
// Used by read calls (Read, Range, Scan) on the returned data
func (c *Connector) resolveBlobs(ctx context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue) error {
	for _, blob := range blobs(ei) {
		if blobValue, ok := values[blob.Name]; ok {
			blobref := blobValue.(string)
			bloburl, err := url.Parse(blobref)
			if err != nil {
				return errors.Wrapf(err, "Invalid URL %q", blobref)
			}
			reader, err := c.Store.Read(ctx, bloburl)
			if err != nil {
				return errors.Wrapf(err, "External blob read failure for URL %q", blobref)
			}
			values[blob.Name] = dosa.BlobRef{Reference: *bloburl, Channel: reader}
		}
	}
	return nil
}

func (c *Connector) Read(ctx context.Context, ei *dosa.EntityInfo, keys map[string]dosa.FieldValue, minimumFields []string) (values map[string]dosa.FieldValue, err error) {
	newEi, hasBlobs := scrubEi(ei)
	values, err = c.Next.Read(ctx, newEi, keys, minimumFields)
	if err == nil && hasBlobs {
		err = c.resolveBlobs(ctx, ei, values)
		if err != nil {
			return nil, errors.Wrapf(err, "Could not resolve blobs for key %q", keys)
		}
	}
	return
}

func (c *Connector) MultiRead(ctx context.Context, ei *dosa.EntityInfo, keys []map[string]dosa.FieldValue, minimumFields []string) (results []*dosa.FieldValuesOrError, err error) {
	panic("not implemented")
}

func (c *Connector) Upsert(ctx context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue) error {
	newEi, hasBlobs := scrubEi(ei)
	if hasBlobs {
		for _, col := range ei.Def.Columns {
			if col.Type == dosa.EBlob {
				if blob, ok := values[col.Name]; ok {
					// blob upsert requested, might be new, so lets add it first and get the URL
					url, err := c.Store.Write(ctx, ei, col, values, blob.(dosa.BlobRef).Channel)
					if err != nil {
						return err
					}
					values[col.Name] = url.String()
				}
			}
		}
	}
	err := c.Next.Upsert(ctx, newEi, values)
	// TODO: an error here means that any blob updates should be reverted, but we can't be sure what the
	// value of the blob was before
	return err
}

func (c *Connector) CreateIfNotExists(ctx context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue) error {
	newEi, hasBlob := scrubEi(ei)
	if hasBlob {
		for _, col := range ei.Def.Columns {
			if col.Type == dosa.EBlob {
				return fmt.Errorf("Cannot specify a blob column on CreateIfNotExists; Upsert the blob after creation")
			}
		}
	}
	return c.Next.CreateIfNotExists(ctx, newEi, values)
}

func (c *Connector) MultiUpsert(ctx context.Context, ei *dosa.EntityInfo, multiValues []map[string]dosa.FieldValue) (result []error, err error) {
	panic("not implemented")
}

func (c *Connector) Remove(ctx context.Context, ei *dosa.EntityInfo, keys map[string]dosa.FieldValue) error {
	newEi, hasBlob := scrubEi(ei)
	if hasBlob {
		for _, col := range ei.Def.Columns {
			if col.Type == dosa.EBlob {
				err := c.Store.Remove(ctx, ei, col, keys)
				if err != nil {
					return err
				}
			}
		}
	}
	return c.Next.Remove(ctx, newEi, keys)
}

func (c *Connector) RemoveRange(ctx context.Context, ei *dosa.EntityInfo, columnConditions map[string][]*dosa.Condition) error {
	_, hasBlobs := scrubEi(ei)
	if hasBlobs {
		return fmt.Errorf("Entity %q contains an external blob; RemoveRange is not supported", ei.Def.Name)
	}
	return c.Next.RemoveRange(ctx, ei, columnConditions)
}

func (c *Connector) MultiRemove(ctx context.Context, ei *dosa.EntityInfo, multiKeys []map[string]dosa.FieldValue) (result []error, err error) {
	panic("not implemented")
}

func (c *Connector) Range(ctx context.Context, ei *dosa.EntityInfo, columnConditions map[string][]*dosa.Condition, minimumFields []string, token string, limit int) ([]map[string]dosa.FieldValue, string, error) {
	_, hasBlobs := scrubEi(ei)
	values, token, err := c.Next.Range(ctx, ei, columnConditions, minimumFields, token, limit)
	if err == nil && hasBlobs {
		for _, value := range values {
			err = c.resolveBlobs(ctx, ei, value)
			if err != nil {
				return nil, "", errors.Wrap(err, "Unable to resolve blobs in range")
			}
		}
	}
	return values, token, err
}

func (c *Connector) Scan(ctx context.Context, ei *dosa.EntityInfo, minimumFields []string, token string, limit int) (multiValues []map[string]dosa.FieldValue, nextToken string, err error) {
	_, hasBlobs := scrubEi(ei)
	values, token, err := c.Next.Scan(ctx, ei, minimumFields, token, limit)
	if err == nil && hasBlobs {
		for _, value := range values {
			err = c.resolveBlobs(ctx, ei, value)
			if err != nil {
				return nil, "", errors.Wrap(err, "Unable to resolve blobs in scan")
			}
		}
	}
	return values, token, err
}

func (c *Connector) Shutdown() error {
	return c.Store.Shutdown()
}
