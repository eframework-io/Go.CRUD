// Copyright (c) 2025 EFramework Innovation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package XOrm

import (
	"fmt"
	"strings"
	"sync"

	"github.com/eframework-io/Go.Utility/XCollect"
	"github.com/eframework-io/Go.Utility/XLog"
	"github.com/eframework-io/Go.Utility/XString"
	"github.com/eframework-io/Go.Utility/XTime"
)

var (
	// globalCacheMap 存储全局缓存，键为模型标识，值为对象映射 map[string]*globalObject。
	globalCacheMap sync.Map

	// globalListMap 存储全局列举标记，键为模型标识，值为模型列举状态。
	globalListMap sync.Map

	// globalLockMap 存储全局锁标记，键为模型标识，值为 WaitGroup。
	globalLockMap sync.Map

	// globalIncrementMap 存储全局自增值，键为模型标识 + 字段名称，值为当前的最大值。
	globalIncrementMap sync.Map

	// globalIncreMutex 用于确定全局自增值的原子性。
	globalIncreMutex sync.Mutex

	// sessionCacheMap 存储会话缓存，键为 goroutine ID，值为模型对象映射。
	sessionCacheMap sync.Map

	// sessionListMap 存储会话列举标记，键为 goroutine ID，值为模型列举状态。
	sessionListMap sync.Map

	// sessionObjectPool 用于存储 sessionObject 对象的对象池。
	sessionObjectPool sync.Pool = sync.Pool{New: func() any { return new(sessionObject) }}

	// cacheDumpWait 用于等待 Dump 函数的执行。
	cacheDumpWait sync.WaitGroup

	// cacheDumpMutex 用于确定 Dump 函数的原子性。
	cacheDumpMutex sync.Mutex
)

// sessionObject 定义了会话缓存中的对象结构。
type sessionObject struct {
	raw    IModel     // 原始实例
	ptr    IModel     // 工作实例
	write  int        // 是否为读写状态（0：未标记，1：只读，2：读写）
	create bool       // 是否为新建状态
	delete bool       // 是否标记为删除
	clear  *Condition // 是否标记为清除
}

// reset 重置对象状态。
func (sobj *sessionObject) reset() {
	sobj.raw = nil
	sobj.ptr = nil
	sobj.write = 0
	sobj.create = false
	sobj.delete = false
	sobj.clear = nil
}

// isWritable 设置或获取对象的读写状态。
// status 为可选的读写标志，true 表示读写，false 表示只读。
// 返回当前的读写状态（0：未标记，1：只读，2：读写）。
// 若对象已被标记为读写(2)，则无法降级为只读(1)。
// 未标记状态(0)可以设置为任意状态。
// 只读状态(1)可以升级为读写状态(2)。
func (so *sessionObject) isWritable(status ...bool) int {
	if len(status) >= 1 {
		// 若该数据已经被标记为读写，则无法修改为只读
		switch so.write {
		case 0: // 未标记
			if status[0] {
				so.write = 2
			} else {
				so.write = 1
			}
		case 1: // 只读
			if status[0] {
				so.write = 2
			}
		}
	}
	return so.write
}

// getGlobalCache 获取指定模型的全局内存映射。
// model 为模型实例。
// 返回对象映射，如果不存在则返回 nil。
func getGlobalCache(model IModel) *XCollect.Map {
	value, _ := globalCacheMap.Load(model.ModelUnique())
	if value != nil {
		return value.(*XCollect.Map)
	}
	return nil
}

// getSessionCache 获取当前会话中指定模型的内存映射。
// gid 为 goroutine ID。
// model 为模型实例。
// 返回对象映射，如果不存在则返回 nil。
func getSessionCache(gid int64, model IModel) *XCollect.Map {
	value, _ := sessionCacheMap.Load(gid)
	if value != nil {
		value2, _ := value.(*sync.Map).Load(model.ModelUnique())
		if value2 != nil {
			return value2.(*XCollect.Map)
		}
	}
	return nil
}

// setGlobalCache 将模型实例保存到全局缓存中。
// model 为要缓存的模型实例。
// 返回全局对象实例。
// 此操作会覆盖已存在的对象，除非该对象已被标记为删除。
// 覆盖操作会记录错误日志。
func setGlobalCache(model IModel) {
	name := model.DataUnique()
	omap, _ := globalCacheMap.LoadOrStore(model.ModelUnique(), XCollect.NewMap())
	value, _ := omap.(*XCollect.Map).LoadOrStore(name, model)
	gobj := value.(IModel)
	if gobj != model {
		if !gobj.IsValid() {
			// 若是被标记为删除的数据，则不算覆盖
		} else {
			XLog.Error("XOrm.Cache.setGlobalCache: data of %v has been overwritten.", name)
		}
	}
}

// setSessionCache 将模型实例保存到会话缓存中。
// gid 为 goroutine ID。
// model 为要缓存的模型实例。
// 返回会话对象实例。
// 此操作会覆盖已存在的对象，除非该对象已被标记为删除。
// 覆盖操作会记录错误日志。
// 会保存原始模型的克隆副本用于比较。
func setSessionCache(gid int64, model IModel) *sessionObject {
	name := model.DataUnique()
	tmap, _ := sessionCacheMap.LoadOrStore(gid, &sync.Map{}) // 对应线程
	omap, _ := tmap.(*sync.Map).LoadOrStore(model.ModelUnique(), XCollect.NewMap())
	value, loaded := omap.(*XCollect.Map).LoadOrStore(name, sessionObjectPool.Get())
	sobj := value.(*sessionObject)
	if !loaded {
		sobj.ptr = model
		sobj.raw = model.Clone()
	} else if sobj.ptr != model {
		if !sobj.ptr.IsValid() {
			// 若是被标记为删除的数据，则不算覆盖
		} else {
			XLog.Error("XOrm.Cache.setSessionCache: data of %v has been overwritten.", name)
		}
		sobj.ptr = model
		sobj.raw = model.Clone()
	}
	return sobj
}

// isGlobalListed 判断指定模型是否已被全局列举。
// model 为要检查的模型实例。
// status 为可选的列举状态，true 表示已列举，false 表示未列举。
// 返回当前的列举状态，如果未提供 status 参数，则返回当前的状态。
func isGlobalListed(model IModel, status ...bool) bool {
	if len(status) >= 1 {
		globalListMap.Store(model.ModelUnique(), status[0])
		return status[0]
	} else {
		if value, _ := globalListMap.Load(model.ModelUnique()); value != nil {
			return value.(bool)
		}
		return false
	}
}

// isSessionListed 判断当前会话中指定模型是否已被列举。
// gid 为 goroutine ID。
// model 为要检查的模型实例。
// status 为可选的列举状态，true 表示已列举，false 表示未列举。
// 返回当前的列举状态，如果未提供 status 参数，则返回当前的状态。
func isSessionListed(gid int64, model IModel, status ...bool) bool {
	value, _ := sessionListMap.LoadOrStore(gid, &sync.Map{})
	if value != nil {
		if len(status) > 0 {
			value.(*sync.Map).Store(model.ModelUnique(), status[0])
			return status[0]
		} else {
			if tmp, _ := value.(*sync.Map).Load(model.ModelUnique()); tmp != nil {
				return tmp.(bool)
			}
		}
	}
	return false
}

// globalLock 对指定的数据模型加锁。
// model 参数为要加锁的数据模型，必须实现 IModel 接口。
// 函数会获取或创建模型的 WaitGroup，并增加一个等待计数。每次调用都会增加一个等待计数，
// 必须通过 globalUnlock 解锁。锁的粒度是模型级别的，同一模型的所有实例共享同一把锁。
func globalLock(model IModel) {
	if tmp, loaded := globalLockMap.LoadOrStore(model.ModelUnique(), &sync.WaitGroup{}); !loaded {
		tmp.(*sync.WaitGroup).Add(1)
	}
}

// globalWait 等待指定数据模型的锁释放。source 参数为调用来源的标识，用于日志；
// model 参数为要等待的数据模型，必须实现 IModel 接口。
// 函数获取模型的 WaitGroup，如果存在，则开始等待并记录等待开始时间。等待完成后，函数会删除锁记录并记录等待耗时。
// 函数会记录详细的等待日志，等待完成后会自动清理锁。
func globalWait(source string, model IModel) {
	if tmp, _ := globalLockMap.Load(model.ModelUnique()); tmp != nil {
		wg := tmp.(*sync.WaitGroup)
		t := XTime.GetMicrosecond()
		XLog.Notice("XOrm.Cache.globalWait: [%v] %v wait for unlock.", source, model.ModelUnique())
		wg.Wait()
		globalLockMap.Delete(model.ModelUnique())
		XLog.Notice("XOrm.Cache.globalWait: [%v] %v unlock cost %.2fms.", source, model.ModelUnique(), float64(XTime.GetMicrosecond()-t)/1e3)
	}
}

// globalUnlock 解除指定数据模型的锁定。model 参数为要解锁的数据模型，必须实现 IModel 接口。
// 函数会获取模型的 WaitGroup，如果存在，则减少一个等待计数。必须与 globalLock 配对使用，
// 如果没有对应的锁，调用会被忽略。解锁后等待的操作会被唤醒。
func globalUnlock(model IModel) {
	if tmp, loaded := globalLockMap.LoadAndDelete(model.ModelUnique()); loaded {
		tmp.(*sync.WaitGroup).Done()
	}
}

// Dump 清除数据模型的缓存数据。
// models 为要清除的数据模型，若未指定则清除所有模型的缓存。
func Dump(models ...IModel) {
	defer cacheDumpMutex.Unlock()
	cacheDumpMutex.Lock()

	defer cacheDumpWait.Done()
	cacheDumpWait.Add(1)

	if len(models) == 0 {
		globalCacheMap.Clear()
		globalListMap.Clear()
		globalIncrementMap.Clear()
		globalLockMap.Range(func(key, value any) bool {
			value.(*sync.WaitGroup).Done()
			return true
		})
		globalLockMap.Clear()
		XLog.Notice("XOrm.Dump: all models' cache has been dumpped.")
	} else {
		for _, model := range models {
			if model == nil {
				continue
			}

			key := model.ModelUnique()
			globalCacheMap.Delete(key)
			globalListMap.Delete(key)

			var deleteIncres []string
			globalIncrementMap.Range(func(k, v any) bool {
				gkey := k.(string)
				if XString.StartsWith(gkey, key) {
					deleteIncres = append(deleteIncres, gkey)
				}
				return true
			})
			if len(deleteIncres) > 0 {
				for _, k := range deleteIncres {
					globalIncrementMap.Delete(k)
				}
			}

			if tmp, loaded := globalLockMap.LoadAndDelete(key); loaded {
				tmp.(*sync.WaitGroup).Done()
			}
			XLog.Notice("XOrm.Dump: cache of model: %v has been dumpped.", key)
		}
	}
}

// Print 生成缓存的文本信息。
// 返回包含以下信息的字符串：
//
//	[Data]: 缓存数据
//	[List]: 列举状态
//	[Increment]: 自增记录
//	[Lock]: 数据锁状态
func Print() string {
	cacheDumpWait.Wait()

	var ctt strings.Builder
	ctt.WriteString("[Data]\n")
	globalCacheMap.Range(func(k1, v1 any) bool {
		v1.(*XCollect.Map).Range(func(k2, v2 any) bool {
			gobj := v2.(IModel)
			ctt.WriteString("\t")
			ctt.WriteString(k2.(string))
			ctt.WriteString(" = ")
			ctt.WriteString(gobj.Json())
			ctt.WriteString("\n")
			return true
		})
		return true
	})

	ctt.WriteString("[List]\n")
	globalListMap.Range(func(k, v any) bool {
		ctt.WriteString(fmt.Sprintf("\t%v = %v\n", k, v))
		return true
	})

	ctt.WriteString("[Increment]\n")
	globalIncrementMap.Range(func(k, v any) bool {
		ctt.WriteString(fmt.Sprintf("\t%v = %v\n", k, v))
		return true
	})

	ctt.WriteString("[Lock]\n")
	globalLockMap.Range(func(k, v any) bool {
		ctt.WriteString(fmt.Sprintf("\t%v\n", k))
		return true
	})

	return ctt.String()
}
