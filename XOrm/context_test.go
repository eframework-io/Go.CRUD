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

// TestContext 测试上下文操作。
func TestContext(t *testing.T) {
	defer ResetContext()
	defer ResetBaseTest()

	model := NewTestBaseModel()
	tests := []struct {
		cache      bool
		writable   bool
		concurrent int
	}{
		{true, true, 1},    // 写入操作应当单线程
		{false, true, 1},   // 写入操作应当单线程
		{true, false, 10},  // 读取操作可以多线程
		{false, false, 10}, // 读取操作可以多线程
	}

	for _, test := range tests {
		ResetContext()
		ResetBaseTest()
		SetupBaseTest(test.cache, test.writable)

		t.Run(fmt.Sprintf("%+v", test), func(t *testing.T) {
			var wg sync.WaitGroup
			for i := range test.concurrent {
				wg.Add(1)

				go func(chunk int) {
					defer wg.Done()

					// 开始会话
					Watch(test.writable)

					gid := goid.Get()
					ccond := Cond("id >= {0} && id <= {1}", chunk*1000+1, chunk*1000+1000)

					// 验证写入后的数据正确性
					{
						for i := range 500 {
							data := NewTestBaseModel()
							data.ID = chunk*1000 + i + 1
							data.IntVal = data.ID
							data.IsValid(true)
							data.Write() // 持久化
						}
						for i := range 500 {
							data := NewTestBaseModel()
							data.ID = chunk*1000 + i + 1 + 500
							data.IntVal = data.ID
							Write(data) // 缓存
						}
						datas := List(model, ccond)
						if test.writable {
							assert.Equal(t, 1000, len(datas), "写入操作后列举出来的数据数量应当为 1000。")
						} else {
							assert.Equal(t, 500, len(datas), "写入操作后列举出来的数据数量应当为 500。")
						}
						for _, sdata := range datas {
							data := NewTestBaseModel()
							data.ID = sdata.ID
							data = Read(data)
							assert.Equal(t, sdata, data, "列举出来的数据指针应当与读取数据的一致。")
						}
					}

					// 验证更新后的数据正确性
					{
						for i := range 1000 {
							data := NewTestBaseModel()
							data.ID = chunk*1000 + i + 1
							data = Read(data)
							data.IntVal = data.ID * 2
						}
						datas := List(model, ccond)
						for _, sdata := range datas {
							assert.Equal(t, sdata.IntVal, sdata.ID*2, "更新操作后数据的 IntVal 应当为 ID 的两倍。")
						}
					}

					// 验证删除后的数据正确性
					{
						for i := range 1000 {
							if i%2 == 1 {
								data := NewTestBaseModel()
								data.ID = chunk*1000 + i + 1
								Delete(data)
							}
						}
						datas := List(model, ccond)
						assert.Equal(t, 500, len(datas), "删除操作后列举出来的数据数量应当为 500。")
						for _, sdata := range datas {
							data := NewTestBaseModel()
							data.ID = sdata.ID
							data = Read(data)
							assert.Equal(t, sdata, data, "删除操作后列举出来的数据指针应当与读取数据的一致。")
						}
					}

					// 验证清除后的数据正确性
					{
						Clear(model, Cond("id >= {0} && id <= {1}", chunk*1000+1, chunk*1000+500))
						datas := List(model, ccond)
						if test.writable {
							assert.Equal(t, 250, len(datas), "清除操作后列举出来的数据数量应当为 250。")
						} else {
							assert.Equal(t, 500, len(datas), "清除操作后列举出来的数据数量应当为 500。")
						}
						for _, sdata := range datas {
							data := NewTestBaseModel()
							data.ID = sdata.ID
							data = Read(data)
							assert.Equal(t, sdata, data, "清除操作后列举出来的数据指针应当与读取数据的一致。")
						}
					}

					// 结束会话
					Defer()

					// 等待事务
					Flush(gid)

					// 验证会话结束后数据的正确性
					if test.writable {
						assert.Equal(t, 250, model.Count(ccond), "会话结束后数据数量应当为 250。")
						datas := List(model, ccond)
						for _, sdata := range datas {
							if sdata.ID%2 == 1 {
								assert.Equal(t, sdata.IntVal, sdata.ID*2, "会话结束后数据的 IntVal 应当为 ID 的两倍。")
							}
						}
					} else {
						assert.Equal(t, 500, model.Count(ccond), "会话结束后数据数量应当为 500。")
					}
				}(i)
			}
			wg.Wait()
		})
	}
}
