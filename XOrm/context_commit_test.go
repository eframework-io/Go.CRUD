// Copyright (c) 2025 EFramework Innovation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package XOrm

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/eframework-io/Go.Utility/XPrefs"
	"github.com/petermattis/goid"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
)

// TestContextCommit 测试提交操作。
func TestContextCommit(t *testing.T) {
	t.Run("Setup", func(t *testing.T) {
		defer initContextCommit(XPrefs.Asset())

		initContextCommit(XPrefs.New())
		assert.Equal(t, runtime.NumCPU(), contextCommitQueueCount, fmt.Sprintf("默认的提交队列数量应当为 %v。", runtime.NumCPU()))
		assert.Equal(t, 100000, contextCommitQueueCapacity, "默认的提交批次数量应当为 100000。")

		initContextCommit(XPrefs.New().Set(preferencesContextCommitQueueCount, 20).Set(preferencesContextCommitQueueCapacity, 1000))
		assert.Equal(t, 20, contextCommitQueueCount, "设置的提交队列数量应当为 20。")
		assert.Equal(t, 1000, contextCommitQueueCapacity, "设置的提交批次数量应当为 1000。")

		assert.Equal(t, 20, len(contextCommitQueues), "提交队列数量应当为 20。")
		assert.Equal(t, 1000, cap(contextCommitQueues[0]), "提交队列容量应当为 1000。")
		assert.Equal(t, 20, len(contextCommitSetupSig), "初始化信号数量应当为 20。")
		assert.Equal(t, 20, len(contextCommitFlushWait), "提交信号数量应当为 20。")

		Close()
		assert.Equal(t, int32(1), atomic.LoadInt32(&contextCommitCloseSig), "关闭状态标识应该为 1。")
	})

	t.Run("Pool", func(t *testing.T) {
		wg := sync.WaitGroup{}
		for i := range 100 {
			wg.Add(1)

			go func(i int) {
				defer wg.Done()

				bobj := contextCommitBatchPool.Get().(*commitBatch)
				bobj.reset()
				assert.Nil(t, bobj.tag, "commitBatch 对象的 tag 字段应为 nil。")
				assert.Equal(t, 0, bobj.stime, "commitBatch 对象的 time 字段应为 0。")
				assert.Nil(t, bobj.objects, "commitBatch 对象的 objs 字段应为 nil。")
				assert.Nil(t, bobj.prehandler, "commitBatch 对象的 prehandler 字段应为 nil。")
				assert.Nil(t, bobj.posthandler, "commitBatch 对象的 posthandler 字段应为 nil。")
				contextCommitBatchPool.Put(bobj)
			}(i)
		}
		wg.Wait()
	})

	t.Run("Commit", func(t *testing.T) {
		defer ResetBaseTest()
		defer initContextCommit(XPrefs.Asset())

		ResetBaseTest()
		SetupBaseTest(false, true)
		initContextCommit(XPrefs.Asset())

		model := NewTestBaseModel()

		wg := sync.WaitGroup{}
		for i := range 10 { // 10 个 goroutine
			wg.Add(1)

			go func(i int, t *testing.T) {
				defer wg.Done()

				// 测试创建操作
				t.Run("Create", func(t *testing.T) {
					for j := range 10 { // 10 个批次
						batch := contextCommitBatchPool.Get().(*commitBatch)
						for k := range 10 { // 10 个对象
							data := NewTestBaseModel()
							data.ID = i*100 + j*10 + k + 1
							data.StringVal = "test_create"

							sobj := sessionObjectPool.Get().(*sessionObject)
							sobj.ptr = data
							sobj.create = true

							batch.objects = append(batch.objects, sobj)
						}
						batch.submit()
					}
					Flush() // 等待提交完成

					datas := make([]*TestBaseModel, 0)
					model.List(&datas, Cond("id >= {0} && id <= {1}", i*100+1, i*100+100))

					assert.Equal(t, 100, len(datas), "创建操作后的对象数量应该为 100。")
					for _, result := range datas {
						assert.Equal(t, "test_create", result.StringVal, "创建操作后的对象的 StringVal 字段应该为 test_create。")
					}
				})

				// 测试更新操作
				t.Run("Update", func(t *testing.T) {
					datas := make([]*TestBaseModel, 0)
					model.List(&datas, Cond("id >= {0} && id <= {1}", i*100+1, i*100+100))

					batch := contextCommitBatchPool.Get().(*commitBatch)
					for _, data := range datas {
						data.StringVal = "test_update"
						sobj := sessionObjectPool.Get().(*sessionObject)
						sobj.ptr = data.Clone()
						batch.objects = append(batch.objects, sobj)
					}
					batch.submit()
					Flush() // 等待提交完成

					datas = make([]*TestBaseModel, 0)
					model.List(&datas, Cond("id >= {0} && id <= {1}", i*100+1, i*100+100)) // 重新读取数据
					for _, result := range datas {
						assert.Equal(t, "test_update", result.StringVal, "更新操作后的对象的 StringVal 字段应该为 test_update。")
					}
				})

				// 测试删除操作
				t.Run("Delete", func(t *testing.T) {
					datas := make([]*TestBaseModel, 0)
					model.List(&datas, Cond("id >= {0} && id <= {1}", i*100+1, i*100+100))

					batch := contextCommitBatchPool.Get().(*commitBatch)
					for j := range len(datas) / 2 {
						data := datas[j]
						sobj := sessionObjectPool.Get().(*sessionObject)
						sobj.ptr = data.Clone()
						sobj.delete = true
						batch.objects = append(batch.objects, sobj)
					}
					batch.submit()
					Flush() // 等待提交完成

					datas = make([]*TestBaseModel, 0)
					model.List(&datas, Cond("id >= {0} && id <= {1}", i*100+1, i*100+100)) // 重新读取数据
					assert.Equal(t, 50, len(datas), "删除操作后的对象数量应该为 50。")
				})

				// 测试清除操作
				t.Run("Clear", func(t *testing.T) {
					batch := contextCommitBatchPool.Get().(*commitBatch)
					sobj := sessionObjectPool.Get().(*sessionObject)
					sobj.ptr = model
					sobj.clear = Cond("id >= {0} && id <= {1}", i*100+1, i*100+100)
					batch.objects = append(batch.objects, sobj)
					batch.submit()
					Flush() // 等待提交完成

					datas := make([]*TestBaseModel, 0)
					model.List(&datas, Cond("id >= {0} && id <= {1}", i*100+1, i*100+100)) // 重新读取数据
					assert.Equal(t, 0, len(datas), "清除作后的对象数量应该为 0。")
				})

				// 测试前后置处理器回调
				t.Run("Handler", func(t *testing.T) {
					batch := contextCommitBatchPool.Get().(*commitBatch)
					sobj := sessionObjectPool.Get().(*sessionObject)
					sobj.ptr = model
					batch.objects = append(batch.objects, sobj)
					var prebatch *commitBatch
					var preobj *sessionObject
					var postbatch *commitBatch
					var postobj *sessionObject
					batch.prehandler = func(batch *commitBatch, sobj *sessionObject) {
						prebatch = batch
						preobj = sobj
					}
					batch.posthandler = func(batch *commitBatch, sobj *sessionObject) {
						postbatch = batch
						postobj = sobj
					}
					batch.submit()
					Flush() // 等待提交完成

					assert.Equal(t, batch, prebatch, "前置处理器回调的批次对象应该与提交时的对象相同。")
					assert.Equal(t, sobj, preobj, "前置处理器回调的提交对象应该与提交时的对象相同。")

					assert.Equal(t, batch, postbatch, "后置处理器回调的批次对象应该与提交时的对象相同。")
					assert.Equal(t, sobj, postobj, "后置处理器回调的提交对象应该与提交时的对象相同。")
				})
			}(i, t)
		}
		wg.Wait()
	})

	t.Run("Metrics", func(t *testing.T) {
		defer ResetBaseTest()
		defer initContextCommit(XPrefs.Asset())

		ResetBaseTest()
		SetupBaseTest(false, true)
		initContextCommit(XPrefs.Asset())

		gid := goid.Get()
		queueID := max(int(gid)%contextCommitQueueCount, 0)

		batch := contextCommitBatchPool.Get().(*commitBatch)
		for i := range 100 { // 100 个对象
			data := NewTestBaseModel()
			data.ID = i + 1

			sobj := sessionObjectPool.Get().(*sessionObject)
			sobj.ptr = data
			sobj.create = true

			batch.objects = append(batch.objects, sobj)
		}
		batch.submit()

		assert.Equal(t, 100, int(testutil.ToFloat64(contextCommitGauges[queueID])), "指定队列 %v 等待提交的对象数量应当为 100。", queueID)
		assert.Equal(t, 100, int(testutil.ToFloat64(contextCommitGauge)), "所有队列等待提交的对象数量应当为 100。")

		Flush() // 等待提交完成

		assert.Equal(t, 0, int(testutil.ToFloat64(contextCommitGauges[queueID])), "指定队列 %v 等待提交的对象数量应当为 0。", queueID)
		assert.Equal(t, 0, int(testutil.ToFloat64(contextCommitGauge)), "所有队列等待提交的对象数量应当为 0。")

		assert.Equal(t, 100, int(testutil.ToFloat64(contextCommitCounters[queueID])), "指定队列 %v 已经提交的对象总数应当为 100。", queueID)
		assert.Equal(t, 100, int(testutil.ToFloat64(contextCommitCounter)), "所有队列已经提交的对象总数应当为 100。")
	})

	t.Run("Flush", func(t *testing.T) {
		defer ResetBaseTest()
		defer initContextCommit(XPrefs.Asset())

		ResetBaseTest()
		SetupBaseTest(false, true)
		initContextCommit(XPrefs.Asset())

		model := NewTestBaseModel()

		wg := sync.WaitGroup{}
		for i := range 100 {
			wg.Add(1)

			go func(i int) {
				defer wg.Done()

				batch := contextCommitBatchPool.Get().(*commitBatch)
				data := NewTestBaseModel()
				data.ID = i + 1
				data.StringVal = "test_flush"

				sobj := sessionObjectPool.Get().(*sessionObject)
				sobj.ptr = data
				sobj.create = true

				batch.objects = append(batch.objects, sobj)
				batch.submit()

				if i%2 == 0 {
					Flush()
					assert.Equal(t, true, data.Read(), "指定线程 Flush 后应当可以立即读取到数据。")
				}
			}(i)
		}
		wg.Wait()

		Flush(-1)
		assert.Equal(t, 100, model.Count(), "所有线程 Flush 后应当可以读取到所有数据。")
	})
}
