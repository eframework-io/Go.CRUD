// Copyright (c) 2025 EFramework Innovation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package XOrm

import (
	"github.com/eframework-io/Go.Utility/XLog"
	"github.com/eframework-io/Go.Utility/XTime"
	"github.com/petermattis/goid"
)

// Delete 标记数据模型为删除状态。
// model 为要删除的数据模型，必须实现 IModel 接口。
//
// 删除操作首先验证模型是否已注册。如果启用了缓存，会在全局内存中查找对应的对象，
// 如果存在，则设置其删除标记为 true。
// 然后在会话内存中创建或获取会话对象，并设置删除标记为 true。
//
// 需要注意的是，删除操作是软删除，不会立即从内存中移除数据，被标记删除的数据在读取时会被忽略。
// 该函数是线程不安全的，操作相同的数据模型时，需要控制并发或使用适当的同步方式（如：Mutex）以确保操作的正确性。
func Delete[T IModel](model T) {
	cacheDumpWait.Wait()

	gid := goid.Get()
	ctx := getContext(gid)
	if ctx == nil {
		XLog.Critical("XOrm.Delete: context was not found: %v", XLog.Caller(1, false))
		return
	}
	meta := getModelMeta(model)
	if meta == nil {
		XLog.Critical("XOrm.Delete: model of %v was not registered: %v", model.ModelUnique(), XLog.Caller(1, false))
		return
	}
	if !ctx.writable {
		XLog.Error("XOrm.Delete: context was not writable.")
		return
	}
	if !meta.writable {
		XLog.Error("XOrm.Delete: model of %v was not writable.", model.ModelUnique())
		return
	}

	time := XTime.GetMicrosecond()
	defer func() {
		ctx.deleteCost += XTime.GetMicrosecond() - time
		ctx.deleteCount++
	}()

	model.IsValid(false)

	if meta.cache {
		gcache := getGlobalCache(model)
		if gcache != nil {
			gobj, exist := gcache.Load(model.DataUnique())
			if exist {
				gobj.(IModel).IsValid(false)
			}
		}
	}

	sobj := setSessionCache(gid, model)
	sobj.delete = true
	sobj.create = false
	sobj.clear = nil
}
