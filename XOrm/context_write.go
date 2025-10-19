// Copyright (c) 2025 EFramework Innovation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package XOrm

import (
	"github.com/eframework-io/Go.Utility/XLog"
	"github.com/eframework-io/Go.Utility/XTime"
	"github.com/petermattis/goid"
)

// Write 将数据模型写入到内存缓存中。model 参数为要写入的数据模型，必须实现 IModel 接口。
//
// 函数首先验证模型是否已注册，然后设置模型为有效状态。
// 如果启用了缓存，会克隆模型并写入全局内存，同时清除删除标记。
// 接着写入会话内存，设置创建标记，并清除删除标记和清理标记。
//
// 需要注意的是，写入操作不会立即持久化到远端数据。
// 该函数是线程不安全的，操作相同的数据模型时，需要控制并发或使用适当的同步方式（如：Mutex）以确保操作的正确性。
func Write[T IModel](model T) {
	cacheDumpWait.Wait()

	gid := goid.Get()
	ctx := getContext(gid)
	if ctx == nil {
		XLog.Critical("XOrm.Write: context was not found: %v", XLog.Caller(1, false))
		return
	}
	meta := getModelMeta(model)
	if meta == nil {
		XLog.Critical("XOrm.Write: model of %v was not registered: %v", model.ModelUnique(), XLog.Caller(1, false))
		return
	}
	if !ctx.writable {
		XLog.Error("XOrm.Write: context was not writable.")
		return
	}
	if !meta.writable {
		XLog.Error("XOrm.Write: model of %v was not writable.", model.ModelUnique())
		return
	}

	time := XTime.GetMicrosecond()
	defer func() {
		ctx.writeCost += XTime.GetMicrosecond() - time
		ctx.writeCount++
	}()

	model.IsValid(true)

	if meta.cache {
		setGlobalCache(model.Clone())
	}

	sobj := setSessionCache(gid, model)
	sobj.create = true
	sobj.delete = false
	sobj.clear = nil
}
