// Copyright (c) 2025 EFramework Innovation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package XOrm

import (
	"fmt"
	"testing"

	"github.com/petermattis/goid"
	"github.com/stretchr/testify/assert"
)

// TestContextClear 测试清除操作。
func TestContextClear(t *testing.T) {
	tests := []struct {
		cache    bool
		writable bool
	}{
		{true, true},
		{true, false},
		{false, true},
		{false, false},
	}

	defer ResetContext()
	defer ResetBaseTest()

	model := NewTestBaseModel()

	for _, test := range tests {
		ResetContext()
		ResetBaseTest()
		SetupBaseTest(test.cache, test.writable)

		t.Run(fmt.Sprintf("%+v", test), func(t *testing.T) {
			gid := goid.Get()
			ctx := &context{}
			ctx.writable = test.writable
			contextMap.Store(gid, ctx)
			defer contextMap.Delete(gid)

			for i := range 1000 {
				data := NewTestBaseModel()
				data.ID = i + 1
				data.IntVal = data.ID
				data.IsValid(true)
				setSessionCache(gid, data)
				if test.cache {
					setGlobalCache(data)
				}
			}

			cond := Cond("int_val >= {0} && int_val <= {1}", 1, 500)
			Clear(model, cond)

			var scount int
			var icond, scond *Condition

			if scache := getSessionCache(gid, model); scache != nil {
				scache.Range(func(key, value any) bool {
					sobj := value.(*sessionObject)
					data := sobj.ptr.(*TestBaseModel)
					if data.IntVal >= 1 && data.IntVal <= 500 && !sobj.ptr.IsValid() {
						scount++
					}
					return true
				})

				var sobj *sessionObject
				if tmp, _ := scache.Load(model.DataUnique()); tmp != nil {
					sobj = tmp.(*sessionObject)
				}

				if test.writable {
					icond = cond
					if sobj != nil {
						scond = sobj.clear
					}
				}
			}
			assert.Equal(t, scond, icond, "会话存储的清除条件实例 %v 应当和输入的 %v 实例相等。", scond, icond)
			ecount := 0
			if test.writable {
				ecount = 500
			}
			assert.Equal(t, ecount, scount, "会话清除标记的数据应当为 %v 个。", ecount)

			var gcount int
			if gcache := getGlobalCache(model); gcache != nil {
				gcache.Range(func(key, value any) bool {
					data := value.(*TestBaseModel)
					if data.IntVal >= 1 && data.IntVal <= 500 && !data.IsValid() {
						gcount++
					}
					return true
				})
			}
			ecount = 0
			if test.writable && test.cache {
				ecount = 500
			}
			assert.Equal(t, ecount, gcount, "全局清除标记的数据应当为 %v 个。", ecount)
		})
	}
}
