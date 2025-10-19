// Copyright (c) 2025 EFramework Innovation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package XOrm

import (
	"strings"

	"github.com/beego/beego/v2/client/orm"
	"github.com/eframework-io/Go.Utility/XEnv"
	"github.com/eframework-io/Go.Utility/XLog"
	"github.com/eframework-io/Go.Utility/XPrefs"
	"github.com/eframework-io/Go.Utility/XString"

	_ "github.com/go-sql-driver/mysql"
)

const (
	preferencesOrmAddr = "Addr"
	preferencesOrmPool = "Pool"
	preferencesOrmConn = "Conn"
)

func init() {
	initializeOrm(XPrefs.Asset())
}

func initializeOrm(preferences XPrefs.IBase) {
	if preferences == nil {
		XLog.Panic("XOrm.initializeOrm: preferences is nil.")
		return
	}

	for _, key := range preferences.Keys() {
		if !strings.HasPrefix(key, "Orm/Source") {
			continue
		}
		parts := strings.Split(key, "/")
		if len(parts) < 4 {
			XLog.Panic("XOrm.initializeOrm: invalid preferences key %v.", key)
			return
		}

		ormType := strings.ToLower(parts[2])
		ormAlias := parts[3]

		if base := preferences.Get(key).(XPrefs.IBase); base != nil {
			ormAddr := XString.Eval(base.GetString(preferencesOrmAddr), XEnv.Vars())
			ormPool := base.GetInt(preferencesOrmPool)
			ormConn := base.GetInt(preferencesOrmConn)
			if err := orm.RegisterDataBase(ormAlias, ormType, ormAddr,
				orm.MaxIdleConnections(ormPool),
				orm.MaxOpenConnections(ormConn)); err != nil {
				XLog.Panic("XOrm.initializeOrm: register database %v failed, err: %v", ormAlias, err)
				return
			}
		} else {
			XLog.Error("XOrm.initializeOrm: invalid config for %v", key)
			continue
		}
	}
}
