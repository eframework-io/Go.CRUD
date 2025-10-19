// Copyright (c) 2025 EFramework Innovation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package XOrm

import (
	"sync"
	"testing"

	"github.com/petermattis/goid"
	"github.com/stretchr/testify/assert"
)

// TestContextList 测试列举操作。
func TestContextList(t *testing.T) {
	defer ResetContext()
	defer ResetBaseTest()

	model := NewTestBaseModel()
	ResetBaseTest()
	SetupBaseTest()

	tests := []struct {
		name       string
		concurrent int
		arrange    func(chunk int)
	}{
		{
			name:       "Session", // 会话内存
			concurrent: 10,        // 会话使用多线程测试
			arrange: func(chunk int) {
				gid := goid.Get()
				isSessionListed(gid, model, true)
				for i := range 1000 {
					data := NewTestBaseModel()
					data.ID = chunk*1000 + i + 1
					data.IntVal = data.ID
					data.IsValid(true)
					sobj := setSessionCache(gid, data)
					if i%2 == 1 {
						sobj.ptr.IsValid(false)
					}
				}
			},
		},
		{
			name:       "Global", // 全局内存
			concurrent: 1,        // 全局使用单线程测试
			arrange: func(chunk int) {
				isGlobalListed(model, true)
				for i := range 1000 {
					data := NewTestBaseModel()
					data.ID = chunk*1000 + i + 1
					data.IntVal = data.ID
					data.IsValid(i%2 == 0)
					setGlobalCache(data)

				}
			},
		},
		{
			name:       "Database", // 持久化层
			concurrent: 1,          // 持久化层使用单线程测试
			arrange: func(chunk int) {
				for i := range 500 {
					data := NewTestBaseModel()
					data.ID = chunk*500 + i + 1
					data.IntVal = data.ID
					data.IsValid(true)
					data.Write()
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ResetContext()

			var wg sync.WaitGroup
			for i := range test.concurrent {
				wg.Add(1)

				go func(chunk int) {
					defer wg.Done()

					gid := goid.Get()
					ctx := &context{}
					contextMap.Store(gid, ctx)
					defer contextMap.Delete(gid)

					test.arrange(chunk)
					var datas []*TestBaseModel

					{
						// 条件列举数据
						ndatas := List(model, Cond("int_val >= {0} && int_val <= {1}", chunk*1000+1, chunk*1000+1000))
						assert.Equal(t, 500, len(ndatas), "条件列举的数据应当为 500 个。")
						datas = append(datas, ndatas...)

						ndatas = List(model, Cond("int_val < {0}", 0))
						assert.Equal(t, 0, len(ndatas), "条件列举不存在的数据应当为 0 个。")
					}

					{
						// 全量列举数据
						ndatas := List(model)
						assert.Equal(t, 500, len(ndatas), "全量列举的数据应当为 500 个。")
						datas = append(datas, ndatas...)

						assert.Equal(t, true, isGlobalListed(model), "全量列举后会话的列举状态标识应当为 true。")
						assert.Equal(t, true, isGlobalListed(model), "全量列举后全局的列举状态标识应当为 true。")
					}

					for _, data := range datas {
						var sobj *sessionObject
						if tmp, _ := getSessionCache(gid, model).Load(data.DataUnique()); tmp != nil {
							sobj = tmp.(*sessionObject)
						}
						assert.Equal(t, data.IsValid(), sobj != nil && data == sobj.ptr, "有效的数据应当被会话监控，且实例指针相等。")
					}
				}(i)
			}
			wg.Wait()
		})
	}
}
