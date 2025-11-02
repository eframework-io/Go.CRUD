// Copyright (c) 2025 EFramework Innovation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package XOrm

import (
	"testing"

	"github.com/beego/beego/v2/client/orm"
	"github.com/eframework-io/Go.Utility/XPrefs"
)

func TestOrmSource(t *testing.T) {
	tests := []struct {
		name        string
		preferences XPrefs.IBase
		panic       bool
	}{
		{
			name: "Single",
			preferences: XPrefs.New().Set("XOrm/Source/MySQL/myalias", XPrefs.New().
				Set(preferencesOrmSourceAddr, "root:123456@tcp(127.0.0.1:3306)/mysql?charset=utf8mb4&loc=Local").
				Set(preferencesOrmSourcePool, 10).
				Set(preferencesOrmSourceConn, 100)),
			panic: false,
		},
		{
			name: "Multiple",
			preferences: XPrefs.New().
				Set("XOrm/Source/MySQL/myalias1", XPrefs.New().
					Set(preferencesOrmSourceAddr, "root:123456@tcp(127.0.0.1:3306)/mysql?charset=utf8mb4").
					Set(preferencesOrmSourcePool, 10).
					Set(preferencesOrmSourceConn, 100)).
				Set("XOrm/Source/MySQL/myalias2", XPrefs.New().
					Set(preferencesOrmSourceAddr, "root:123456@tcp(127.0.0.1:3306)/information_schema?charset=utf8mb4").
					Set(preferencesOrmSourcePool, 20).
					Set(preferencesOrmSourceConn, 200)),
			panic: false,
		},
		{
			name: "Invalid",
			preferences: XPrefs.New().
				Set("XOrm/Source/MySQL/myalias3", XPrefs.New().
					Set(preferencesOrmSourceAddr, "root:wrongpass@tcp(127.0.0.1:3306)/mysql?charset=utf8mb4").
					Set(preferencesOrmSourcePool, 10).
					Set(preferencesOrmSourceConn, 100)),
			panic: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Reset database cache
			orm.ResetModelCache()

			if test.preferences == nil {
				defer func() {
					if r := recover(); r == nil && test.panic {
						t.Errorf("setup() expected error")
					}
				}()
				initOrmSource(test.preferences)
				return
			}

			// For invalid config test, expect panic
			if test.panic {
				defer func() {
					if r := recover(); r == nil {
						t.Errorf("setup() expected error")
					}
				}()
				initOrmSource(test.preferences)
				return
			}

			// Normal config test
			initOrmSource(test.preferences)

			// Test multiple database connections
			aliases := []string{}
			switch test.name {
			case "single_db_test":
				aliases = append(aliases, "myalias")
			case "multiple_db_test":
				aliases = append(aliases, "myalias1", "myalias2")
			}

			for _, alias := range aliases {
				// Try to get database connection
				db, err := orm.GetDB(alias)
				if err != nil {
					t.Errorf("failed to get database connection [%s]: %v", alias, err)
					return
				}

				// Test database connection
				if err := db.Ping(); err != nil {
					t.Errorf("failed to ping database [%s]: %v", alias, err)
					return
				}
			}
		})
	}
}
