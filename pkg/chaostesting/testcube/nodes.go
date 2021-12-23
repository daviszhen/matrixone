// Copyright 2021 Matrix Origin
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
	"fmt"
	"net"
	"path/filepath"

	crdbpebble "github.com/cockroachdb/pebble"
	"github.com/lni/vfs"
	"github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/raftstore"
	"github.com/matrixorigin/matrixcube/server"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/storage/executor/simple"
	"github.com/matrixorigin/matrixcube/storage/kv"
	"github.com/matrixorigin/matrixcube/storage/kv/pebble"
	fz "github.com/matrixorigin/matrixone/pkg/chaostesting"
	"go.uber.org/zap"
)

type Nodes = []*Node

func (_ Def) Nodes(
	numNodes fz.NumNodes,
	defaultConfig DefaultCubeConfig,
	randomizeConfig RandomizeCubeConfig,
	tempDir fz.TempDir,
) (nodes Nodes) {

	var endpoints []string

	for i := fz.NumNodes(0); i < numNodes; i++ {
		endpoints = append(endpoints, "http://"+net.JoinHostPort("localhost", randPort()))
	}

	for nodeID := fz.NodeID(0); nodeID < fz.NodeID(numNodes); nodeID++ {

		node := &Node{
			Endpoint: endpoints[nodeID],
		}

		conf := defaultConfig(nodeID)
		randomizeConfig(conf)
		node.Config = conf

		loggerConfig := zap.Config{
			Level:    zap.NewAtomicLevel(),
			Encoding: "json",
			OutputPaths: []string{
				fmt.Sprintf("node-%d.log", nodeID),
			},
		}
		loggerConfig.Level.SetLevel(zap.DebugLevel)
		logger, err := loggerConfig.Build()
		ce(err)
		defer logger.Sync()
		node.Logger = logger

		fs := vfs.Default
		conf.FS = fs

		conf.RaftAddr = net.JoinHostPort("127.0.0.1", randPort())
		conf.ClientAddr = net.JoinHostPort("127.0.0.1", randPort())
		conf.DataPath = filepath.Join(string(tempDir), fmt.Sprintf("data-%d", nodeID))
		conf.Logger = logger

		conf.Prophet.DataDir = filepath.Join(string(tempDir), fmt.Sprintf("prophet-%d", nodeID))
		conf.Prophet.RPCAddr = net.JoinHostPort("127.0.0.1", randPort())
		if nodeID > 0 {
			conf.Prophet.EmbedEtcd.Join = endpoints[0]
		}
		conf.Prophet.EmbedEtcd.ClientUrls = "http://" + net.JoinHostPort("localhost", randPort())
		conf.Prophet.EmbedEtcd.PeerUrls = endpoints[nodeID]

		conf.Storage = func() config.StorageConfig {
			kvStorage, err := pebble.NewStorage(
				fs.PathJoin(string(tempDir), fmt.Sprintf("storage-%d", nodeID)),
				logger,
				&crdbpebble.Options{},
			)
			ce(err)
			base := kv.NewBaseStorage(kvStorage, fs)
			dataStorage := kv.NewKVDataStorage(base, simple.NewSimpleKVExecutor(kvStorage))
			return config.StorageConfig{
				DataStorageFactory: func(group uint64) storage.DataStorage {
					return dataStorage
				},
				ForeachDataStorageFunc: func(fn func(storage.DataStorage)) {
					fn(dataStorage)
				},
			}
		}()

		store := raftstore.NewStore(conf)
		node.RaftStore = store

		app := server.NewApplication(server.Cfg{
			Store: store,
		})
		node.App = app

		nodes = append(nodes, node)
	}

	return
}
