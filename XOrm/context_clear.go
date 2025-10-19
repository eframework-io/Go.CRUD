// Copyright (c) 2025 EFramework Innovation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package XOrm

import (
	"sync"

	"github.com/eframework-io/Go.Utility/XLog"
	"github.com/eframework-io/Go.Utility/XTime"
	"github.com/petermattis/goid"
)

// Clear 根据条件批量标记数据模型为清除状态。
// model 为要清除的数据模型，必须实现 IModel 接口。
// cond 为可选的查询条件列表，用于匹配要清除的数据。
//
// 清除操作首先验证模型是否已注册，然后创建标记映射用于跟踪已处理的对象。
// 在会话内存清除阶段，遍历会话内存中的对象，对匹配条件的对象设置删除标记，并记录到标记映射中。
// 如果启用了缓存，在全局内存清除阶段，遍历全局内存中的对象，对匹配条件的对象设置删除标记。
// 对于未在会话内存中处理过的对象，会克隆到会话内存并设置相应的删除标记。
//
// 需要注意的是，清除操作是软删除，不会立即从内存中移除数据，被标记清除的数据在读取时会被忽略。
// 该函数是线程不安全的，操作相同的数据模型时，需要控制并发或使用适当的同步方式（如：Mutex）以确保操作的正确性。
func Clear[T IModel](model T, cond ...*Condition) {
	cacheDumpWait.Wait()

	gid := goid.Get()
	ctx := getContext(gid)
	if ctx == nil {
		XLog.Critical("XOrm.Clear: context was not found: %v", XLog.Caller(1, false))
		return
	}
	meta := getModelMeta(model)
	if meta == nil {
		XLog.Critical("XOrm.Clear: model of %v was not registered: %v", model.ModelUnique(), XLog.Caller(1, false))
		return
	}
	if !ctx.writable {
		XLog.Error("XOrm.Clear: context was not writable.")
		return
	}
	if !meta.writable {
		XLog.Error("XOrm.Clear: model of %v was not writable.", model.ModelUnique())
		return
	}

	time := XTime.GetMicrosecond()
	defer func() {
		ctx.clearCost += XTime.GetMicrosecond() - time
		ctx.clearCount++
	}()

	model.IsValid(false)

	var marked sync.Map
	scache := getSessionCache(gid, model)
	if scache != nil { // 标记相关的会话内存为无效，避免再次读取
		scache.RangeConcurrent(func(index int, key, value any) bool {
			if value.(*sessionObject).ptr.Matchs(cond...) {
				value.(*sessionObject).ptr.IsValid(false)
				marked.Store(key, 1)
			}
			return true
		})
	}

	if meta.cache { // 标记相关的全局内存为无效，避免再次读取
		gcache := getGlobalCache(model)
		if gcache != nil {
			gcache.RangeConcurrent(func(index int, key, value any) bool {
				gobj := value.(IModel)
				if gobj.Matchs(cond...) {
					gobj.IsValid(false)
					if _, loaded := marked.Load(key); !loaded {
						nobj := gobj.Clone()
						ret := setSessionCache(gid, nobj) // 标记相关的会话内存为无效，避免再次读取
						ret.ptr.IsValid(false)
					}
				}
				return true
			})
		}
	}

	sobj := setSessionCache(gid, model)
	if len(cond) > 0 {
		sobj.clear = cond[0]
	} else {
		sobj.clear = Cond()
	}
	sobj.create = false
	sobj.delete = false
}
