// Copyright (c) 2025 EFramework Innovation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package XOrm

import (
	"fmt"
	"sync"
	"testing"

	"github.com/petermattis/goid"
	"github.com/stretchr/testify/assert"
)

// TestContextDelete 测试删除操作。
func TestContextDelete(t *testing.T) {
	tests := []struct {
		cache    bool
		writable bool
	}{
		{true, true},
		{true, false},
		{false, true},
	}

	defer ResetContext()
	defer ResetBaseTest()

	model := NewTestBaseModel()

	for _, test := range tests {
		ResetContext()
		ResetBaseTest()
		SetupBaseTest(test.cache, test.writable)

		t.Run(fmt.Sprintf("%+v", test), func(t *testing.T) {
			var wg sync.WaitGroup
			for i := range 100 {
				wg.Add(1)

				go func(chunk int) {
					defer wg.Done()

					gid := goid.Get()
					ctx := &context{}
					ctx.writable = test.writable
					contextMap.Store(gid, ctx)
					defer contextMap.Delete(gid)

					data := NewTestBaseModel()
					data.ID = chunk + 1
					data.IntVal = chunk
					data.FloatVal = float64(chunk) + 0.5
					data.StringVal = fmt.Sprintf("test_string_%d", chunk)
					data.BoolVal = true
					data.IsValid(true)

					setSessionCache(gid, data)
					if test.cache {
						setGlobalCache(data.Clone())
					}

					Delete(data)

					var sobj *sessionObject
					if scache := getSessionCache(gid, model); scache != nil {
						if tmp, _ := scache.Load(data.DataUnique()); tmp != nil {
							sobj = tmp.(*sessionObject)
						}
					}

					var gobj IModel
					if gcache := getGlobalCache(model); gcache != nil {
						if tmp, _ := getGlobalCache(model).Load(data.DataUnique()); tmp != nil {
							gobj = tmp.(IModel)
						}
					}

					assert.Equal(t, test.writable, sobj != nil && !sobj.ptr.IsValid(), "数据应当被会话监控，且被标记为无效。")
					assert.Equal(t, test.cache && test.writable, gobj != nil && !gobj.IsValid(), "数据应当被全局标记为无效。")
				}(i)
			}
			wg.Wait()
		})
	}
}
