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

// TestContextWrite 测试写入操作。
func TestContextWrite(t *testing.T) {
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

					Write(data)

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

					assert.Equal(t, test.writable, sobj != nil && data == sobj.ptr, "有效的数据应当被会话监控，且实例指针相等。")
					assert.Equal(t, test.cache && test.writable, gobj != nil && gobj.Equals(data), "有效的数据应当被全局缓存，且实例数据相等。")
				}(i)
			}
			wg.Wait()
		})
	}
}
