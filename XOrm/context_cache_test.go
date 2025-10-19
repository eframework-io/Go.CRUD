// Copyright (c) 2025 EFramework Innovation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package XOrm

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/eframework-io/Go.Utility/XObject"
	"github.com/petermattis/goid"
	"github.com/stretchr/testify/assert"
)

// TestContextCache 测试缓存操作。
func TestContextCache(t *testing.T) {
	defer ResetContext()

	models := []IModel{
		XObject.New[TestCacheModel1](),
		XObject.New[TestCacheModel2](),
	}
	ResetBaseTest()
	for _, model := range models {
		Register(model, true, false)
	}

	t.Run("Pool", func(t *testing.T) {
		wg := sync.WaitGroup{}
		for i := range 100 {
			wg.Add(1)

			go func(i int) {
				defer wg.Done()

				sobj := sessionObjectPool.Get().(*sessionObject)
				assert.NotNil(t, sobj, "sessionObject 对象不应为 nil。")
				sobj.reset()
				assert.Nil(t, sobj.raw, "sessionObject 对象的 raw 字段应为 nil。")
				assert.Nil(t, sobj.ptr, "sessionObject 对象的 ptr 字段应为 nil。")
				assert.Equal(t, 0, sobj.write, "sessionObject 对象的 write 字段应为 0。")
				assert.Equal(t, false, sobj.create, "sessionObject 对象的 create 字段应为 false。")
				assert.Equal(t, false, sobj.delete, "sessionObject 对象的 delete 字段应为 false。")
				assert.Nil(t, sobj.clear, "sessionObject 对象的 clear 字段应为 nil。")

				sobj.isWritable(false)
				assert.Equal(t, 1, sobj.write, "sessionObject 对象的 writable 字段应为 1。")

				sobj.isWritable(true)
				assert.Equal(t, 2, sobj.write, "sessionObject 对象的 writable 字段应为 2。")

				sobj.isWritable(false)
				assert.Equal(t, 2, sobj.write, "已被标记为读写的 sessionObject 对象的 writable 字段仍应为 2。")

				sobj.create = true
				sobj.delete = true
				sobj.clear = Cond()

				sessionObjectPool.Put(sobj)
			}(i)
		}
		wg.Wait()
	})

	t.Run("Global", func(t *testing.T) {
		defer ResetContext()
		ResetContext()

		wg := sync.WaitGroup{}
		for i := range 100 {
			wg.Add(1)

			go func(i int) {
				defer wg.Done()

				for _, model := range models {
					data := model.Clone().(TestCacheModel)
					data.IDProp(i)
					setGlobalCache(data)
					cache := getGlobalCache(model)
					assert.NotNil(t, cache, "全局缓存应当不为 nil。")

					tmp, ok := cache.Load(data.DataUnique())
					assert.Equal(t, true, ok, "全局缓存存储的对象应当存在。")
					gobj := tmp.(IModel)
					assert.Equal(t, data, gobj, "全局缓存存储的对象应当和创建的实例相等。")

					assert.Equal(t, false, isGlobalListed(model, false), "全局列举标记应当为 false。")
					assert.Equal(t, true, isGlobalListed(model, true), "全局列举标记应当为 true。")
				}
			}(i)
		}
		wg.Wait()
	})

	t.Run("Session", func(t *testing.T) {
		wg := sync.WaitGroup{}
		for i := range 100 {
			wg.Add(1)

			go func(i int) {
				defer wg.Done()

				gid := goid.Get()
				for _, model := range models {
					data := model.Clone().(TestCacheModel)
					data.IDProp(1)
					setSessionCache(gid, data)
					cache := getSessionCache(gid, model)
					assert.NotNil(t, cache, "会话缓存应当不为 nil。")

					tmp, ok := cache.Load(data.DataUnique())
					assert.Equal(t, true, ok, "会话缓存存储的对象应当存在。")
					sobj := tmp.(*sessionObject)
					assert.Equal(t, data, sobj.ptr, "会话缓存存储的对象应当和创建的实例相等。")

					assert.Equal(t, false, isSessionListed(gid, model, false), "会话列举标记应当为 false。")
					assert.Equal(t, true, isSessionListed(gid, model, true), "会话列举标记应当为 true。")
				}
			}(i)
		}
		wg.Wait()
	})

	t.Run("Lock", func(t *testing.T) {
		wg := sync.WaitGroup{}
		for i := range 100 {
			wg.Add(1)

			go func(i int) {
				for _, model := range models {
					globalLock(model)
				}
				wg.Done()

				time.Sleep(time.Second)

				for _, model := range models {
					globalUnlock(model)
				}
			}(i)
		}
		wg.Wait()

		for _, model := range models {
			_, loaded := globalLockMap.Load(model.ModelUnique())
			assert.Equal(t, true, loaded, "全局锁应当被设置。")
		}

		for i := range 100 {
			wg.Add(1)

			go func(i int) {
				defer wg.Done()

				for _, model := range models {
					globalWait(fmt.Sprintf("goroutine_%v", i), model)
				}
			}(i)
		}
		wg.Wait()

		for _, model := range models {
			_, loaded := globalLockMap.Load(model.ModelUnique())
			assert.Equal(t, false, loaded, "全局锁应当被取消。")
		}
	})

	t.Run("Dump", func(t *testing.T) {
		ResetContext()

		wg := sync.WaitGroup{}
		for i, model := range models {
			data := model.Clone().(TestCacheModel)
			data.IDProp(i)
			setGlobalCache(data)
			isGlobalListed(model, true)

			wg0 := sync.WaitGroup{}
			wg0.Add(1)
			wg.Add(1)
			go func(i int, mode IModel) {
				if i == 0 {
					defer wg0.Done()
				}
				defer wg.Done()

				globalLock(model)
			}(i, model)

			if i == 0 {
				Dump(model)
				assert.Nil(t, getGlobalCache(model), "Dump 指定模型后全局缓存应当为 nil。")
				assert.Equal(t, false, isGlobalListed(models[0]), "Dump 指定模型后全局列举状态应当为 false。")
				wg0.Wait()
			}
		}

		Dump()
		assert.Nil(t, getGlobalCache(models[1]), "Dump 所有模型后全局缓存应当为 nil。")
		assert.Equal(t, false, isGlobalListed(models[1]), "Dump 所有模型后全局列举状态应当为 false。")
		wg.Wait()
	})

	t.Run("Print", func(t *testing.T) {
		defer ResetContext()
		ResetContext()

		for i, model := range models {
			data := model.Clone().(TestCacheModel)
			data.IDProp(i)
			setGlobalCache(data)
			isGlobalListed(model, true)
			globalLock(model)
			globalIncrementMap.Store(fmt.Sprintf("%v_%v", model.ModelUnique(), "id"), 1000)
		}

		var ctt = Print()
		assert.NotNil(t, ctt, "打印缓存信息应当不为 nil。")
		assert.NotEmpty(t, ctt, "打印缓存信息应当不为空。")

		// 验证输出内容包含所有必要的部分
		assert.Contains(t, ctt, "[Data]", "输出应包含 Data 章节标记")
		assert.Contains(t, ctt, "[List]", "输出应包含 List 章节标记")
		assert.Contains(t, ctt, "[Increment]", "输出应包含 Increment 自增值章节标记")
		assert.Contains(t, ctt, "[Lock]", "输出应包含 Lock 章节标记")

		// 验证对象部分内容
		assert.Contains(t, ctt, "testcache_model1_0 = {\"ID\":0}", "输出应包含第一个模型对象")
		assert.Contains(t, ctt, "testcache_model2_1 = {\"ID\":1}", "输出应包含第二个模型对象")

		// 验证列举部分内容
		assert.Contains(t, ctt, "testcache_model1 = true", "输出应包含第一个模型的列举状态")
		assert.Contains(t, ctt, "testcache_model2 = true", "输出应包含第二个模型的列举状态")

		// 验证最大值部分内容
		assert.Contains(t, ctt, "testcache_model1_id = 1000", "输出应包含第一个模型的最大值信息")
		assert.Contains(t, ctt, "testcache_model2_id = 1000", "输出应包含第二个模型的最大值信息")

		// 验证锁部分内容
		assert.Contains(t, ctt, "testcache_model1", "输出应包含第一个模型的锁信息")
		assert.Contains(t, ctt, "testcache_model2", "输出应包含第二个模型的锁信息")
	})
}

// ResetContext 重置会话上下文。
func ResetContext() {
	Dump()
}

type TestCacheModel interface {
	IModel
	IDProp(val ...int) int
}

type TestCacheModel1 struct {
	Model[TestCacheModel1] `orm:"-" json:"-"`
	ID                     int `orm:"column(id);pk"`
}

func (tm1 *TestCacheModel1) AliasName() string {
	return "testcache"
}

func (tm1 *TestCacheModel1) TableName() string {
	return "model1"
}

func (tm1 *TestCacheModel1) IDProp(val ...int) int {
	if len(val) > 0 {
		tm1.ID = val[0]
	}
	return tm1.ID
}

type TestCacheModel2 struct {
	Model[TestCacheModel2] `orm:"-" json:"-"`
	ID                     int `orm:"column(id);pk"`
}

func (tm2 *TestCacheModel2) AliasName() string {
	return "testcache"
}

func (tm2 *TestCacheModel2) TableName() string {
	return "model2"
}

func (tm2 *TestCacheModel2) IDProp(val ...int) int {
	if len(val) > 0 {
		tm2.ID = val[0]
	}
	return tm2.ID
}
