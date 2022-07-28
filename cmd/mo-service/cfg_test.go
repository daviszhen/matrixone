// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/stretchr/testify/assert"
)

func TestParseDNConfig(t *testing.T) {
	data := `
		service-type = "DN"

		[log]
		level = "debug"
		format = "json"
		max-size = 512
		
		[dn]
		# storage directory for local data. Include DNShard metadata and TAE data.
		data-dir = ""
		
		[dn.Txn.Storage]
		# txn storage backend implementation. [TAE|MEM]
		backend = "MEM"
	`
	cfg, err := parseFromString(data)
	assert.NoError(t, err)
	assert.Equal(t, "MEM", cfg.DN.Txn.Storage.Backend)
}

func TestFileServiceFactory(t *testing.T) {
	c := &Config{}
	c.FileServices = append(c.FileServices, fileservice.Config{
		Name:    "a",
		Backend: "MEM",
	})

	fs, err := c.createFileService("A")
	assert.NoError(t, err)
	assert.NotNil(t, fs)

	fs, err = c.createFileService("B")
	assert.Error(t, err)
	assert.Nil(t, fs)
}
