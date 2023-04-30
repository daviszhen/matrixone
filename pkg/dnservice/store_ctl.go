// Copyright 2023 Matrix Origin
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

package dnservice

import (
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/ctlservice"
)

func (s *store) initCtlService() error {
	cs, err := ctlservice.NewCtlService(s.cfg.UUID, s.cfg.Ctl.Address.ListenAddress, s.cfg.RPC)
	if err != nil {
		return err
	}
	s.ctlservice = cs
	s.initCtlCommandHandler()
	runtime.ProcessLevelRuntime().SetGlobalVariables(runtime.CtlService, s.ctlservice)
	return nil
}

func (s *store) initCtlCommandHandler() {

}
