// Copyright (c) 2025 EFramework Innovation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package XOrm

import (
	"reflect"
	"sync"
	"unsafe"

	"github.com/beego/beego/v2/client/orm"
	"github.com/eframework-io/Go.Utility/XLog"
)

//go:linkname defaultModelCache github.com/beego/beego/v2/client/orm.defaultModelCache
var defaultModelCache *any

// beegoModelCache 是 beego/orm 的模型缓存实例。
var beegoModelCache *beegoModelMap = (*beegoModelMap)(unsafe.Pointer(reflect.ValueOf(defaultModelCache).UnsafePointer()))

// beegoModelMap 定义了 beego/orm 的内部模型映射结构。
type beegoModelMap struct {
	orders          []string                   // 模型注册顺序
	cache           map[string]*beegoModelInfo // 按表名索引的模型信息
	cacheByFullName map[string]*beegoModelInfo // 按完整名称索引的模型信息
	bootstrapOnce   *sync.Once                 // 初始化同步锁
}

// beegoModelInfo 定义了 beego/orm 的内部模型信息结构。
type beegoModelInfo struct {
	manual    bool           // 是否手动注册
	isThrough bool           // 是否为中间表
	pkg       string         // 包名
	name      string         // 模型名称
	fullName  string         // 完整名称
	table     string         // 表名
	model     any            // 模型实例
	fields    *beegoFieldMap // 字段映射
	addrField reflect.Value  // 模型地址
	uniques   []string       // 唯一索引
}

// beegoFieldMap 定义了 beego/orm 的内部字段映射结构。
type beegoFieldMap struct {
	pk            *beegoFieldInfo            // 主键字段
	columns       map[string]*beegoFieldInfo // 按列名索引的字段
	fields        map[string]*beegoFieldInfo // 按字段名索引的字段
	fieldsLow     map[string]*beegoFieldInfo // 按小写字段名索引的字段
	fieldsByType  map[int][]*beegoFieldInfo  // 按类型索引的字段
	fieldsRel     []*beegoFieldInfo          // 关联字段
	fieldsReverse []*beegoFieldInfo          // 反向关联字段
	fieldsDB      []*beegoFieldInfo          // 数据库字段
	rels          []*beegoFieldInfo          // 所有关联字段
	orders        []string                   // 字段顺序
	dbCols        []string                   // 数据库列名
}

// beegoFieldInfo 定义了 beego/orm 的内部字段信息结构。
type beegoFieldInfo struct {
	dbCol               bool                // 是否为数据库列（外键和一对一关系）
	inModel             bool                // 是否在模型中
	auto                bool                // 是否自增
	pk                  bool                // 是否为主键
	null                bool                // 是否可为空
	index               bool                // 是否有索引
	unique              bool                // 是否唯一
	colDefault          bool                // 是否有默认值标签
	toText              bool                // 是否转换为文本
	autoNow             bool                // 是否自动更新时间
	autoNowAdd          bool                // 是否自动添加时间
	rel                 bool                // 是否为关联字段（外键、一对一、多对多）
	reverse             bool                // 是否为反向关联
	isFielder           bool                // 是否实现 Fielder 接口
	mi                  *beegoModelInfo     // 所属模型信息
	fieldIndex          []int               // 字段索引
	fieldType           int                 // 字段类型
	name                string              // 字段名
	fullName            string              // 完整名称
	column              string              // 列名
	addrValue           reflect.Value       // 字段地址
	sf                  reflect.StructField // 结构体字段
	initial             string              // 默认值
	size                int                 // 字段大小
	reverseField        string              // 反向关联字段名
	reverseFieldInfo    *beegoFieldInfo     // 反向关联字段信息
	reverseFieldInfoTwo *beegoFieldInfo     // 双向关联字段信息
	reverseFieldInfoM2M *beegoFieldInfo     // 多对多关联字段信息
	relTable            string              // 关联表名
	relThrough          string              // 中间表名
	relThroughModelInfo *beegoModelInfo     // 中间表模型信息
	relModelInfo        *beegoModelInfo     // 关联模型信息
	digits              int                 // 数字位数
	decimals            int                 // 小数位数
	onDelete            string              // 删除时操作
	description         string              // 字段描述
	timePrecision       *int                // 时间精度
	dbType              string              // 数据库类型
}

// modelMeta 定义了模型的扩展信息。
type modelMeta struct {
	*beegoModelInfo      // 继承 beego/orm 的模型信息
	cache           bool // 是否缓存
	writable        bool // 是否可写
}

// modelMetaCache 存储所有已注册模型的信息。
var modelMetaCache map[string]*modelMeta

// modelMetaMutex 用于保护模型缓存的互斥锁。
var modelMetaMutex sync.Mutex

// getModelMeta 获取指定模型的信息。
// model 为模型实例。
// 返回模型的描述信息，如果模型未注册则返回 nil。
func getModelMeta(model IModel) *modelMeta {
	if model != nil {
		return modelMetaCache[model.TableName()]
	}
	return nil
}

// Meta 注册一个模型。
// model 为模型实例。
// cache 指定是否缓存。
// writable 指定是否可写。
// 如果模型为 nil 或已注册，将触发 panic。
func Meta(model IModel, cache bool, writable bool) {
	if model == nil {
		XLog.Panic("XOrm.Meta: nil model instance.")
		return
	}

	modelMetaMutex.Lock()
	defer modelMetaMutex.Unlock()

	if len(beegoModelCache.cache) == 0 {
		modelMetaCache = make(map[string]*modelMeta)
	}

	id := model.TableName()
	orm.RegisterModel(model)
	meta := &modelMeta{beegoModelInfo: beegoModelCache.cache[id], cache: cache, writable: writable}
	modelMetaCache[id] = meta
}
