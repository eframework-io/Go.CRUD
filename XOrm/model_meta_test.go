// Copyright (c) 2025 EFramework Innovation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package XOrm

import (
	"testing"

	"github.com/beego/beego/v2/client/orm"
	"github.com/eframework-io/Go.Utility/XObject"
	"github.com/stretchr/testify/assert"
)

type TestModelMeta1 struct {
	Model[TestModelMeta1]
	Id   int    `orm:"column(id);pk"`
	Name string `orm:"column(name)"`
}

func (m *TestModelMeta1) AliasName() string {
	return "myalias1"
}

func (m *TestModelMeta1) TableName() string {
	return "mytable1"
}

type TestModelMeta2 struct {
	Model[TestModelMeta2]
	Id   int    `orm:"column(id);pk"`
	Name string `orm:"column(name)"`
}

func (m *TestModelMeta2) AliasName() string {
	return "myalias2"
}

func (m *TestModelMeta2) TableName() string {
	return "mytable2"
}

func TestModelMeta(t *testing.T) {
	defer orm.ResetModelCache()
	orm.ResetModelCache()

	model1 := XObject.New[TestModelMeta1]()
	model2 := XObject.New[TestModelMeta2]()

	// 检查模型是否已注册
	Register(model1, true, true)
	meta1 := getModelMeta(model1)
	assert.NotNil(t, meta1, "已注册的模型描述信息不应为空。")
	assert.Equal(t, "mytable1", meta1.table, "注册模型的名称应当和输入的一致。")
	assert.Equal(t, true, meta1.cache, "注册模型的缓存标识应当和输入的一致。")
	assert.Equal(t, true, meta1.writable, "注册模型的可写标识应当和输入的一致。")

	// 测试注册多个模型
	Register(model2, true, false)
	meta2 := getModelMeta(model2)
	assert.Equal(t, true, meta2.cache, "注册模型的缓存标识应当和输入的一致。")
	assert.Equal(t, false, meta2.writable, "注册模型的可写标识应当和输入的一致。")

	// 测试重复注册同一个模型
	defer func() {
		if r := recover(); r == nil {
			t.Error("重复注册相同模型时应当 panic。")
		}
	}()
	Register(model1, true, true)

	// 测试空模型
	defer func() {
		if r := recover(); r == nil {
			t.Error("注册空模型时应当 panic。")
		}
	}()
	Register(nil, true, true)
}
