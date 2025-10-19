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

// TestContextRead 测试读取操作。
func TestContextRead(t *testing.T) {
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
			concurrent: 10,       // 全局使用多线程测试
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
				data := NewTestBaseModel()
				data.ID = chunk*1000 + 1
				data.IsValid(true)
				data.Write()
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ResetContext()

			var wg sync.WaitGroup
			for i := range 10 {
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
						// 精确读取存在的数据
						exactValid := NewTestBaseModel()
						exactValid.ID = chunk*1000 + 1
						datas = append(datas, Read(exactValid))
					}

					{
						// 精确读取不存在的数据
						exactInvalid := NewTestBaseModel()
						exactInvalid.ID = chunk*1000 + 2
						datas = append(datas, Read(exactInvalid))
					}

					{
						// 模糊读取存在的数据
						fuzzyValid := NewTestBaseModel()
						fuzzyValid.ID = chunk*1000 + 3
						datas = append(datas, Read(fuzzyValid, Cond("int_val == {0}", fuzzyValid.ID)))
					}

					{
						// 模糊读取不存在的数据
						fuzzyInvalid := NewTestBaseModel()
						fuzzyInvalid.ID = chunk*1000 + 4
						datas = append(datas, Read(fuzzyInvalid, Cond("int_val == {0}", fuzzyInvalid.ID)))
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
