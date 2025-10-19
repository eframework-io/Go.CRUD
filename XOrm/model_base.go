// Copyright (c) 2025 EFramework Innovation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package XOrm

import (
	"fmt"
	"reflect"
	"strings"
	"unsafe"

	"github.com/beego/beego/v2/client/orm"
	"github.com/eframework-io/Go.Utility/XLog"
	"github.com/eframework-io/Go.Utility/XObject"
	"github.com/eframework-io/Go.Utility/XString"
)

// IModel 定义了数据模型的基础接口。
// 实现此接口的类型可以参与数据库的 CRUD 操作和缓存管理。
type IModel interface {
	// Ctor 执行模型的构造初始化。
	// obj 为模型实例，必须是实现了 IModel 接口的结构体指针。
	// 此方法会在模型创建时自动调用，用于初始化模型的基本状态。
	Ctor(obj any)

	// OnEncode 在对象编码前调用。
	// 子类可以重写此方法以实现自定义的编码逻辑。
	// 通常用于在数据写入前对字段进行预处理。
	OnEncode()

	// OnDecode 在对象解码后调用。
	// 子类可以重写此方法以实现自定义的解码逻辑。
	// 通常用于在数据读取后对字段进行后处理。
	OnDecode()

	// OnQuery 在执行查询时调用。
	// 子类可以重写此方法以实现自定义的查询逻辑。
	// 通常用于在执行查询前追加一个全局的条件，如数据分区等。
	// action 是查询的类型，包括：Count、Max、Min、Read、List、Delete、Clear。
	// cond 是查询的条件，传入的值可能为空。
	// 返回执行查询的最终条件。
	OnQuery(action string, cond *orm.Condition) *orm.Condition

	// AliasName 返回数据库别名。
	// 返回值用于标识不同的数据库连接。
	// 此方法必须由子类实现。
	AliasName() string

	// TableName 返回数据表名称。
	// 返回值用于标识数据库中的具体表。
	// 此方法必须由子类实现。
	TableName() string

	// ModelUnique 返回模型的唯一标识。
	// 返回值格式为 "数据库别名_表名"。
	// 用于在缓存和其他场景中唯一标识一个模型类型。
	ModelUnique() string

	// DataUnique 返回数据记录的唯一标识。
	// 返回值格式为 "模型标识_主键值"。
	// 用于在缓存和其他场景中唯一标识一条记录。
	DataUnique() string

	// DataValue 获取指定字段的值。
	// field 为字段名称。
	// 返回字段值，若字段不存在则返回 nil。
	DataValue(field string) any

	// Count 统计符合条件的记录数量。
	// cond 为可选的查询条件。
	// 返回记录数量，如果发生错误则返回 -1。
	Count(cond ...*Condition) int

	// Max 获取指定列的最大值。
	// column 为可选的列名，若不指定则使用主键列。
	// 返回最大值，如果发生错误则返回 -1。
	Max(column ...string) int

	// Min 获取指定列的最小值。
	// column 为可选的列名，若不指定则使用主键列。
	// 返回最小值，如果发生错误则返回 -1。
	Min(column ...string) int

	// Write 写入或更新当前记录。
	// 在写入前会调用 OnEncode 进行编码处理。
	// 返回受影响的行数，如果发生错误则返回 -1。
	Write() int

	// Read 读取符合条件的记录。
	// cond 为可选的查询条件，若不指定则使用主键作为查询条件。
	// 读取成功后会调用 OnDecode 进行解码处理。
	// 返回是否成功读取到记录。
	Read(cond ...*Condition) bool

	// List 查询符合条件的记录列表。
	// rets 必须是指向切片的指针，用于存储查询结果。
	// cond 为可选的查询条件，可以指定偏移量和限制数量。
	// 返回查询到的记录数量，如果发生错误则返回 -1。
	List(rets any, cond ...*Condition) int

	// Delete 删除当前记录。
	// 使用主键作为删除条件。
	// 返回受影响的行数，如果发生错误则返回 -1。
	Delete() int

	// Clear 清理符合条件的记录。
	// cond 为可选的查询条件，若不指定则清理所有记录。
	// 返回受影响的行数，如果发生错误则返回 -1。
	// 注意：MySQL Connector 最大的参数是 65535，清理大量数据时可能会触发错误：Prepared statement contains too many placeholders，解决方法为分批执行清理。
	Clear(cond ...*Condition) int

	// IsValid 检查或设置对象的有效性。
	// value 为可选的设置值，如果提供则设置对象的有效性状态。
	// 返回对象当前的有效性状态。
	IsValid(value ...bool) bool

	// Clone 创建对象的深度拷贝。
	// 拷贝后会调用 OnDecode 进行解码处理。
	// 返回新的对象实例，如果拷贝失败则返回 nil。
	Clone() IModel

	// Json 将对象转换为 JSON 字符串。
	// 返回 JSON 格式的字符串表示。
	Json() string

	// Equals 比较两个对象是否相等。
	// model 为待比较的对象。
	// 返回两个对象的所有数据库字段是否完全相等。
	Equals(model IModel) bool

	// Matchs 检查对象是否匹配指定条件。
	// cond 为可选的匹配条件。
	// 返回对象是否满足所有条件。
	Matchs(cond ...*Condition) bool
}

// Model 实现了 IModel 接口的基础模型。
// T 为具体的模型类型，必须是结构体类型。
// 所有的具体模型类型都应该嵌入此类型。
type Model[T any] struct {
	this        IModel `orm:"-" json:"-"` // 模型实例
	modelUnique string `orm:"-" json:"-"` // 模型标识
	dataUnique  string `orm:"-" json:"-"` // 数据标识
	isValid     bool   `orm:"-" json:"-"` // 有效标志
}

// Ctor 初始化模型实例。
// obj 必须实现 IModel 接口。
// 此方法会在模型创建时自动调用。
func (md *Model[T]) Ctor(obj any) {
	md.this = obj.(IModel)
	md.modelUnique = ""
	md.dataUnique = ""
	md.isValid = false
}

// OnEncode 在对象编码前调用。
// 子类可以重写此方法以实现自定义的编码逻辑。
// 通常用于在数据写入前对字段进行预处理。
func (md *Model[T]) OnEncode() {}

// OnDecode 在对象解码后调用。
// 子类可以重写此方法以实现自定义的解码逻辑。
// 通常用于在数据读取后对字段进行后处理。
func (md *Model[T]) OnDecode() {}

// OnQuery 在执行查询时调用。
// 子类可以重写此方法以实现自定义的查询逻辑。
// 通常用于在执行查询前追加一个全局的条件，如数据分区等。
// action 是查询的类型，包括：Count、Max、Min、Read、List、Delete、Clear。
// cond 是查询的条件，传入的值可能为空。
// 返回执行查询的最终条件。
func (md *Model[T]) OnQuery(action string, cond *orm.Condition) *orm.Condition { return cond }

// AliasName 返回数据库别名。
// 此方法需要被子类重写，默认会触发 panic。
func (md *Model[T]) AliasName() string { XLog.Panic("Alias name is nil."); return "" }

// TableName 返回数据表名称。
// 此方法需要被子类重写，默认会触发 panic。
func (md *Model[T]) TableName() string { XLog.Panic("Table name is nil."); return "" }

// ModelUnique 返回模型的唯一标识。
// 返回值格式为 "数据库别名_表名"。
func (md *Model[T]) ModelUnique() string {
	if XString.IsEmpty(md.modelUnique) {
		md.modelUnique = fmt.Sprintf("%v_%v", md.this.AliasName(), md.this.TableName())
	}
	return md.modelUnique
}

// DataUnique 返回数据记录的唯一标识。
// 返回值格式为 "模型标识_主键值"。
// 如果模型信息或主键未找到，将返回空字符串。
func (md *Model[T]) DataUnique() string {
	if XString.IsEmpty(md.dataUnique) {
		meta := getModelMeta(md.this)
		if meta == nil {
			XLog.Error("XOrm.Model.DataUnique(%v): model info is nil.", md.this.ModelUnique())
			return ""
		}
		if meta.fields.pk == nil {
			XLog.Error("XOrm.Model.DataUnique(%v): primary key was not found.", md.this.ModelUnique())
			return ""
		}
		fvalue := md.this.DataValue(meta.fields.pk.name)
		md.dataUnique = fmt.Sprintf("%v_%v", md.this.ModelUnique(), fvalue)
	}
	return md.dataUnique
}

// DataValue 获取指定字段的值。
// field 为字段名称。
// 返回字段值，若字段不存在则返回 nil。
func (md *Model[T]) DataValue(field string) any {
	vtp := reflect.ValueOf(md.this).Elem()
	fld := vtp.FieldByName(field)
	if fld.IsValid() {
		return fld.Interface()
	}
	return nil
}

// Count 统计符合条件的记录数量。
// cond 为可选的查询条件。
// 返回记录数量，如果发生错误则返回 -1。
func (md *Model[T]) Count(cond ...*Condition) int {
	if ormer := orm.NewOrmUsingDB(md.this.AliasName()); ormer == nil {
		XLog.Error("XOrm.Model.Count(%v): failed to create orm instance of %v.", md.this.TableName(), md.this.AliasName())
		return -1
	} else {
		query := ormer.QueryTable(md.this)
		if len(cond) > 0 && cond[0] != nil {
			ncond := md.this.OnQuery("Count", cond[0].Base)
			query = query.SetCond(ncond)
		} else {
			ncond := md.this.OnQuery("Count", nil)
			if ncond != nil {
				query = query.SetCond(ncond)
			}
		}
		count, err := query.Count()
		if err != nil {
			XLog.Warn("XOrm.Model.Count(%v): %v", md.this.TableName(), err)
			return -1
		} else {
			return int(count)
		}
	}
}

// Max 获取指定列的最大值。
// column 为可选的列名，若不指定则使用主键列。
// 返回最大值，如果发生错误则返回 -1。
func (md *Model[T]) Max(column ...string) int {
	if ormer := orm.NewOrmUsingDB(md.this.AliasName()); ormer == nil {
		XLog.Error("XOrm.Model.Max(%v): failed to create orm instance of %v.", md.this.TableName(), md.this.AliasName())
		return -1
	} else {
		name := ""
		if len(column) > 0 {
			name = column[0]
		}
		if name == "" {
			meta := getModelMeta(md.this)
			if meta != nil && meta.fields.pk != nil {
				name = meta.fields.pk.column
			}
		}
		if name == "" {
			XLog.Error("XOrm.Model.Max(%v): column was empty.", md.this.TableName())
			return -1
		}
		query := ormer.QueryTable(md.this)
		cond := md.this.OnQuery("Max", nil)
		if cond != nil {
			query = query.SetCond(cond)
		}
		var result orm.ParamsList
		count, err := query.Aggregate(fmt.Sprintf("MAX(`%v`)", name)).ValuesFlat(&result, name)
		if err != nil {
			XLog.Error("XOrm.Model.Max(%v): %v", md.this.TableName(), err)
			return -1
		}
		if count <= 0 || len(result) <= 0 {
			XLog.Error("XOrm.Model.Max(%v): empty result count.", md.this.TableName(), err)
			return -1
		}
		if result[0] == nil { // 空表返回的计数为 nil，这里返回 0。
			return 0
		}
		val, ok := toInt64(result[0])
		if !ok {
			XLog.Error("XOrm.Model.Max(%v): parse result failed: %v.", md.this.TableName(), result[0])
			return -1
		}
		return int(val)
	}
}

// Min 获取指定列的最小值。
// column 为可选的列名，若不指定则使用主键列。
// 返回最小值，如果发生错误则返回 -1。
func (md *Model[T]) Min(column ...string) int {
	if ormer := orm.NewOrmUsingDB(md.this.AliasName()); ormer == nil {
		XLog.Error("XOrm.Model.Min(%v): failed to create orm instance of %v.", md.this.TableName(), md.this.AliasName())
		return -1
	} else {
		name := ""
		if len(column) > 0 {
			name = column[0]
		}
		if name == "" {
			meta := getModelMeta(md.this)
			if meta != nil && meta.fields.pk != nil {
				name = meta.fields.pk.column
			}
		}
		if name == "" {
			XLog.Error("XOrm.Model.Min(%v): column was empty.", md.this.TableName())
			return -1
		}
		query := ormer.QueryTable(md.this)
		cond := md.this.OnQuery("Min", nil)
		if cond != nil {
			query = query.SetCond(cond)
		}
		var result orm.ParamsList
		count, err := query.Aggregate(fmt.Sprintf("MIN(`%v`)", name)).ValuesFlat(&result, name)
		if err != nil {
			XLog.Error("XOrm.Model.Min(%v): %v", md.this.TableName(), err)
			return -1
		}
		if count <= 0 || len(result) <= 0 {
			XLog.Error("XOrm.Model.Min(%v): empty result count.", md.this.TableName(), err)
			return -1
		}
		if result[0] == nil { // 空表返回的计数为 nil，这里返回 0。
			return 0
		}
		val, ok := toInt64(result[0])
		if !ok {
			XLog.Error("XOrm.Model.Min(%v): parse result failed: %v.", md.this.TableName(), result[0])
			return -1
		}
		return int(val)
	}
}

// Write 写入或更新当前记录。
// 在写入前会调用 OnEncode 进行编码处理。
// 返回受影响的行数，如果发生错误则返回 -1。
func (md *Model[T]) Write() int {
	md.this.IsValid(true)
	if ormer := orm.NewOrmUsingDB(md.this.AliasName()); ormer == nil {
		XLog.Error("XOrm.Model.Write(%v): failed to create orm instance of %v.", md.this.TableName(), md.this.AliasName())
		return -1
	} else {
		md.this.OnEncode()
		count, err := ormer.InsertOrUpdate(md.this)
		if err != nil {
			XLog.Error("XOrm.Model.Write(%v): %v", md.this.TableName(), err)
			return -1
		}
		return int(count)
	}
}

// Read 读取符合条件的记录。
// cond 为可选的查询条件，若不指定则使用主键作为查询条件。
// 读取成功后会调用 OnDecode 进行解码处理。
// 返回是否成功读取到记录。
func (md *Model[T]) Read(cond ...*Condition) bool {
	if ormer := orm.NewOrmUsingDB(md.this.AliasName()); ormer == nil {
		XLog.Error("XOrm.Model.Read(%v): failed to create orm instance of %v.", md.this.TableName(), md.this.AliasName())
		return false
	} else {
		meta := getModelMeta(md.this)
		if meta == nil {
			XLog.Error("XOrm.Model.Read(%v): model info is nil", md.this.TableName())
			return false
		}
		if meta.fields.pk == nil {
			XLog.Error("XOrm.Model.Read(%v): primary key was not found", md.this.TableName())
			return false
		}
		query := ormer.QueryTable(md.this)
		if len(cond) > 0 && cond[0] != nil {
			ncond := md.this.OnQuery("Read", cond[0].Base)
			query = query.SetCond(ncond)
		} else {
			ncond := orm.NewCondition().And(meta.fields.pk.column, md.this.DataValue(meta.fields.pk.name)) // 附加主键值
			ncond = md.this.OnQuery("Read", ncond)
			query = query.SetCond(ncond)
		}
		that := md.this // query.One() 会修改对象，所以需要暂存指针
		e := query.One(that)
		md.this = that // 恢复指针
		if e != nil {
			XLog.Warn("XOrm.Model.Read(%v): %v", md.this.TableName(), e)
			return false
		} else {
			md.this.IsValid(true)
			md.this.OnDecode()
			return true
		}
	}
}

// List 查询符合条件的记录列表。
// rets 必须是指向切片的指针，用于存储查询结果。
// cond 为可选的查询条件，可以指定偏移量和限制数量。
// 返回查询到的记录数量，如果发生错误则返回 -1。
func (md *Model[T]) List(rets any, cond ...*Condition) int {
	if ormer := orm.NewOrmUsingDB(md.this.AliasName()); ormer == nil {
		XLog.Error("XOrm.Model.List(%v): failed to create orm instance of %v.", md.this.TableName(), md.this.AliasName())
		return -1
	} else {
		val := reflect.ValueOf(rets)
		if val.Kind() != reflect.Ptr || val.Elem().Kind() != reflect.Slice {
			XLog.Error("XOrm.Model.List(%v): rets must be a pointer to a slice.", md.this.TableName())
			return -1
		}

		query := ormer.QueryTable(md.this)
		if len(cond) > 0 && cond[0] != nil {
			cond0 := cond[0]
			ncond := md.this.OnQuery("List", cond0.Base)
			query = query.SetCond(ncond)
			if cond0.Offset > 0 {
				query = query.Offset(cond0.Offset)
			}
			if cond0.Limit > 0 {
				query = query.Limit(cond0.Limit)
			}
		} else {
			ncond := md.this.OnQuery("List", nil)
			if ncond != nil {
				query = query.SetCond(ncond)
			}
		}

		tcount, terr := query.All(val.Elem().Addr().Interface())
		if terr != nil {
			XLog.Warn("XOrm.Model.List(%v): %v", md.this.TableName(), terr)
			return -1
		}

		if tcount > 0 {
			for i := 0; i < val.Elem().Len(); i++ {
				elem := val.Elem().Index(i)
				ev := elem.Interface()
				if model, ok := ev.(IModel); ok {
					model.Ctor(ev)
					model.OnDecode()
					model.IsValid(true)
				}
			}
		}

		return int(tcount)
	}
}

// Delete 删除当前记录。
// 使用主键作为删除条件。
// 返回受影响的行数，如果发生错误则返回 -1。
func (md *Model[T]) Delete() int {
	if ormer := orm.NewOrmUsingDB(md.this.AliasName()); ormer == nil {
		XLog.Error("XOrm.Model.Delete(%v): failed to create orm instance of %v.", md.this.TableName(), md.this.AliasName())
		return -1
	} else {
		meta := getModelMeta(md.this)
		if meta == nil {
			XLog.Error("XOrm.Model.Delete(%v): model info is nil", md.this.TableName())
			return -1
		}
		if meta.fields.pk == nil {
			XLog.Error("XOrm.Model.Delete(%v): primary key was not found", md.this.TableName())
			return -1
		}
		cond := orm.NewCondition().And(meta.fields.pk.column, md.this.DataValue(meta.fields.pk.name)) // 附加主键值
		cond = md.this.OnQuery("Delete", cond)
		query := ormer.QueryTable(md.this).SetCond(cond)
		count, err := query.Delete()
		if err != nil {
			XLog.Error("XOrm.Model.Delete(%v): %v", md.this.TableName(), err)
			return -1
		}
		return int(count)
	}
}

// Clear 清理符合条件的记录。
// cond 为可选的查询条件，若不指定则清理所有记录。
// 返回受影响的行数，如果发生错误则返回 -1。
// 注意：MySQL Connector 最大的参数是 65535，清理大量数据时可能会触发错误：Prepared statement contains too many placeholders，解决方法为分批执行清理。
func (md *Model[T]) Clear(cond ...*Condition) int {
	if ormer := orm.NewOrmUsingDB(md.this.AliasName()); ormer == nil {
		XLog.Error("XOrm.Model.Clear(%v): failed to create orm instance of %v.", md.this.TableName(), md.this.AliasName())
		return -1
	} else {
		query := ormer.QueryTable(md.this.TableName())
		var ncond *Condition
		if len(cond) > 0 && cond[0] != nil && len(getCondParams(cond[0].Base)) > 0 {
			ncond = cond[0]
		} else {
			// beego orm 的 Delete 方法不支持条件，所以需要使用主键字段 >= 0 作为条件，这样可以匹配所有记录
			meta := getModelMeta(md.this)
			if meta == nil {
				XLog.Error("XOrm.Model.Clear(%v): model info is nil.", md.this.TableName())
				return -1
			}
			if meta.fields.pk == nil {
				XLog.Error("XOrm.Model.Clear(%v): primary key was not found.", md.this.TableName())
				return -1
			}
			ncond = Cond(fmt.Sprintf("%v >= {0}", meta.fields.pk.column), 0)
		}

		query = query.SetCond(md.this.OnQuery("Clear", ncond.Base))
		if ncond.Offset > 0 {
			query = query.Offset(ncond.Offset)
		}
		if ncond.Limit > 0 {
			query = query.Limit(ncond.Limit)
		}

		count, err := query.Delete()
		if err != nil {
			XLog.Error("XOrm.Model.Clear(%v): %v", md.this.TableName(), err)
			return -1
		}
		return int(count)
	}
}

// IsValid 检查或设置对象的有效性。
// value 为可选的设置值。
// 返回对象是否有效。
func (md *Model[T]) IsValid(value ...bool) bool {
	if len(value) > 0 {
		md.isValid = value[0]
	}
	return md.isValid
}

// Clone 创建对象的深度拷贝。
// 拷贝后会调用 OnDecode 进行解码处理。
// 返回新的对象实例，如果拷贝失败则返回 nil。
func (md *Model[T]) Clone() IModel {
	dst := new(T)
	psrc := (*T)(unsafe.Pointer(reflect.ValueOf(md.this).Pointer()))
	pdst := (*T)(unsafe.Pointer(reflect.ValueOf(dst).Pointer()))
	if psrc == nil || pdst == nil {
		XLog.Error("XOrm.Model.Clone(%v): invalid pointer.", md.this.TableName())
		return md.this
	}
	*pdst = *psrc

	if model, ok := any(dst).(IModel); ok {
		model.Ctor(dst)
		model.OnDecode()
		model.IsValid(true)
		return model
	} else {
		return nil
	}
}

// Json 将对象转换为 JSON 字符串。
// 返回 JSON 格式的字符串表示。
func (md *Model[T]) Json() string {
	result, _ := XObject.ToJson(md.this)
	return result
}

// Equals 比较两个对象是否相等。
// model 为待比较的对象。
// 返回两个对象的所有数据库字段是否完全相等。
func (md *Model[T]) Equals(model IModel) bool {
	if md.this == model {
		return true
	}
	if md.this == nil || model == nil {
		return md.this == model
	}

	meta := getModelMeta(md.this)
	if meta == nil {
		return false
	}

	thisAddr := reflect.ValueOf(md.this).Elem()
	compAddr := reflect.ValueOf(model).Elem()

	for _, field := range meta.fields.fieldsDB {
		thisVal := thisAddr.FieldByName(field.name).Interface()
		compFld := compAddr.FieldByName(field.name)
		if !compFld.IsValid() {
			return false
		}
		compVal := compFld.Interface()
		if thisVal != compVal {
			return false
		}
	}

	return true
}

// Matchs 检查对象是否匹配指定条件。
// cond 为可选的匹配条件。
// 返回对象是否满足所有条件。
func (md *Model[T]) Matchs(cond ...*Condition) bool {
	if len(cond) == 0 || cond[0] == nil {
		return true
	}

	meta := getModelMeta(md.this)
	if meta == nil {
		return false
	}

	return doMatch(md.this, meta, cond[0], getCondParams(cond[0].Base), 0)
}

// doMatch 内部匹配方法。
func doMatch(model IModel, meta *modelMeta, ctx *Condition, conds []beegoCondValue, depth int) bool {
	if conds == nil {
		return false
	}

	for i := range conds {
		cond := conds[i]
		hasNext := false
		var nextCond beegoCondValue
		if i < len(conds)-1 {
			nextCond = conds[i+1]
			hasNext = true
		}

		if cond.isCond {
			if doMatch(model, meta, ctx, getCondParams(cond.cond), depth+1) == !cond.isNot {
				if !hasNext || nextCond.isOr {
					return true
				}
			} else if !hasNext || !nextCond.isOr {
				return false
			}
		} else {
			if doComp(model, meta, ctx, cond, depth) == !cond.isNot {
				if !hasNext || nextCond.isOr {
					return true
				}
			} else if !hasNext || !nextCond.isOr {
				return false
			}
		}
	}
	return true
}

// doComp 根据指定操作符对字段进行比较。
//
//	整型支持: Int, Int32, Int64
//	浮点支持: Float32, Float64
func doComp(model IModel, meta *modelMeta, ctx *Condition, cond beegoCondValue, depth int) bool {
	if !(len(cond.exprs) > 0 && len(cond.args) > 0) {
		return false
	}

	field, operator := parseCondition(cond)
	cvalue := getFieldValue(model, meta, field)
	if cvalue == nil {
		return false
	}

	ctype := reflect.TypeOf(cvalue)
	switch operator {
	case "isnull":
		return isNullValue(cvalue, ctype)
	case "in":
		return handleInOperator(ctx, cvalue, cond.args, field, depth)
	case "exact", "ne":
		return handleExactOperator(cvalue, ctype, operator, cond.args[0])
	case "gt", "gte", "lt", "lte":
		return handleComparisonOperator(cvalue, ctype, operator, cond.args[0])
	case "contains", "startswith", "endswith":
		return handleStringOperator(cvalue, ctype, operator, cond.args[0])
	default:
		XLog.Error("XOrm.Model.doComp: operator: %v wasn't supported for table: %v", operator, model.TableName())
		return false
	}
}

// parseCondition 解析条件表达式。
func parseCondition(cond beegoCondValue) (field, operator string) {
	field = cond.exprs[0]
	operator = "exact"
	if len(cond.exprs) == 2 {
		operator = cond.exprs[1]
	}
	return
}

// getFieldValue 获取字段值。
func getFieldValue(model IModel, meta *modelMeta, field string) any {
	fmeta := meta.fields.columns[field]
	if fmeta == nil {
		// 兼容业务层可能传入的名称大小写，统一处理为小写，保持和原生查询一致
		fmeta = meta.fields.columns[strings.ToLower(field)]
	}
	if fmeta != nil {
		return model.DataValue(fmeta.name)
	}
	return nil
}

// isNullValue 判断是否为空值。
func isNullValue(value any, typ reflect.Type) bool {
	if typ.Kind() == reflect.String {
		return value == ""
	}
	return value == nil
}

// operatorKey 是用于缓存操作上下文的键。
type operatorKey struct {
	operator string // 操作类型
	field    string // 字段名称
	depth    int    // 遍历深度
}

// handleInOperator 处理 IN 操作符。
func handleInOperator(ctx *Condition, cvalue any, args []any, field string, depth int) bool {
	if len(args) == 0 {
		return false
	}

	switch arg0 := args[0].(type) {
	case int32:
		nvalue, ok := toInt64(cvalue)
		if !ok {
			return false
		}
		return int64(arg0) == nvalue
	case []int32:
		if len(arg0) == 0 {
			return false
		}
		nvalue, ok := toInt64(cvalue)
		if !ok {
			return false
		}

		var nargs map[int64]struct{}
		okey := operatorKey{"in", field, depth}
		if val, ok := ctx.context.Load(okey); ok {
			nargs = val.(map[int64]struct{})
		} else {
			nargs = make(map[int64]struct{}, len(arg0))
			for _, val := range arg0 {
				nargs[int64(val)] = struct{}{}
			}
			ctx.context.LoadOrStore(okey, nargs)
		}

		_, exists := nargs[nvalue]
		return exists
	case int:
		nvalue, ok := toInt64(cvalue)
		if !ok {
			return false
		}
		return int64(arg0) == nvalue
	case []int:
		if len(arg0) == 0 {
			return false
		}
		nvalue, ok := toInt64(cvalue)
		if !ok {
			return false
		}

		var nargs map[int64]struct{}
		okey := operatorKey{"in", field, depth}
		if val, ok := ctx.context.Load(okey); ok {
			nargs = val.(map[int64]struct{})
		} else {
			nargs = make(map[int64]struct{}, len(arg0))
			for _, val := range arg0 {
				nargs[int64(val)] = struct{}{}
			}
			ctx.context.LoadOrStore(okey, nargs)
		}

		_, exists := nargs[nvalue]
		return exists
	case int64:
		nvalue, ok := toInt64(cvalue)
		if !ok {
			return false
		}
		return arg0 == nvalue
	case []int64:
		if len(arg0) == 0 {
			return false
		}
		nvalue, ok := toInt64(cvalue)
		if !ok {
			return false
		}

		var nargs map[int64]struct{}
		okey := operatorKey{"in", field, depth}
		if val, ok := ctx.context.Load(okey); ok {
			nargs = val.(map[int64]struct{})
		} else {
			nargs = make(map[int64]struct{}, len(arg0))
			for _, val := range arg0 {
				nargs[val] = struct{}{}
			}
			ctx.context.LoadOrStore(okey, nargs)
		}

		_, exists := nargs[nvalue]
		return exists
	case float32:
		nvalue, ok := toFloat64(cvalue)
		if !ok {
			return false
		}
		return float64(arg0) == nvalue
	case []float32:
		if len(arg0) == 0 {
			return false
		}
		nvalue, ok := toFloat64(cvalue)
		if !ok {
			return false
		}

		var nargs map[float64]struct{}
		okey := operatorKey{"in", field, depth}
		if val, ok := ctx.context.Load(okey); ok {
			nargs = val.(map[float64]struct{})
		} else {
			nargs = make(map[float64]struct{}, len(arg0))
			for _, val := range arg0 {
				nargs[float64(val)] = struct{}{}
			}
			ctx.context.LoadOrStore(okey, nargs)
		}

		_, exists := nargs[nvalue]
		return exists
	case float64:
		nvalue, ok := toFloat64(cvalue)
		if !ok {
			return false
		}
		return arg0 == nvalue
	case []float64:
		if len(arg0) == 0 {
			return false
		}
		nvalue, ok := toFloat64(cvalue)
		if !ok {
			return false
		}

		var nargs map[float64]struct{}
		okey := operatorKey{"in", field, depth}
		if val, ok := ctx.context.Load(okey); ok {
			nargs = val.(map[float64]struct{})
		} else {
			nargs = make(map[float64]struct{}, len(arg0))
			for _, val := range arg0 {
				nargs[val] = struct{}{}
			}
			ctx.context.LoadOrStore(okey, nargs)
		}

		_, exists := nargs[nvalue]
		return exists
	case string:
		nvalue, ok := cvalue.(string)
		if !ok {
			return false
		}
		return arg0 == nvalue
	case []string:
		if len(arg0) == 0 {
			return false
		}
		nvalue, ok := cvalue.(string)
		if !ok {
			return false
		}

		var nargs map[string]struct{}
		okey := operatorKey{"in", field, depth}
		if val, ok := ctx.context.Load(okey); ok {
			nargs = val.(map[string]struct{})
		} else {
			nargs = make(map[string]struct{}, len(arg0))
			for _, val := range arg0 {
				nargs[val] = struct{}{}
			}
			ctx.context.LoadOrStore(okey, nargs)
		}

		_, exists := nargs[nvalue]
		return exists
	}
	return false
}

// handleExactOperator 处理精确匹配操作符。
func handleExactOperator(cvalue any, ctype reflect.Type, operator string, arg any) bool {
	switch {
	case isNumericType(ctype):
		return handleNumericExactOperator(cvalue, ctype, operator, arg)
	case ctype.Kind() == reflect.String || ctype.Kind() == reflect.Bool:
		return operator == "exact" && arg == cvalue || operator == "ne" && arg != cvalue
	default:
		return false
	}
}

// handleNumericExactOperator 处理数值类型的精确匹配。
func handleNumericExactOperator(cvalue any, ctype reflect.Type, operator string, arg any) bool {
	if isIntegerType(ctype) {
		return handleIntegerExactOperator(cvalue, operator, arg)
	}
	return handleFloatExactOperator(cvalue, operator, arg)
}

// handleIntegerExactOperator 处理整数类型的精确匹配。
func handleIntegerExactOperator(cvalue any, operator string, arg any) bool {
	cval, ok1 := toInt64(cvalue)
	val, ok2 := toInt64(arg)
	if !ok1 || !ok2 {
		return false
	}

	if operator == "exact" {
		return cval == val
	}
	return cval != val
}

// handleFloatExactOperator 处理浮点类型的精确匹配。
func handleFloatExactOperator(cvalue any, operator string, arg any) bool {
	cval, ok1 := toFloat64(cvalue)
	val, ok2 := toFloat64(arg)
	if !ok1 || !ok2 {
		return false
	}

	if operator == "exact" {
		return cval == val
	}
	return cval != val
}

// handleComparisonOperator 处理比较操作符。
func handleComparisonOperator(cvalue any, ctype reflect.Type, operator string, arg any) bool {
	if !isNumericType(ctype) {
		return false
	}

	if isIntegerType(ctype) {
		return handleIntegerComparisonOperator(cvalue, operator, arg)
	}
	return handleFloatComparisonOperator(cvalue, operator, arg)
}

// handleIntegerComparisonOperator 处理整数类型的比较操作。
func handleIntegerComparisonOperator(cvalue any, operator string, arg any) bool {
	cval, ok1 := toInt64(cvalue)
	val, ok2 := toInt64(arg)
	if !ok1 || !ok2 {
		return false
	}

	switch operator {
	case "gt":
		return cval > val
	case "gte":
		return cval >= val
	case "lt":
		return cval < val
	case "lte":
		return cval <= val
	default:
		return false
	}
}

// handleFloatComparisonOperator 处理浮点类型的比较操作。
func handleFloatComparisonOperator(cvalue any, operator string, arg any) bool {
	cval, ok1 := toInt64(cvalue)
	val, ok2 := toInt64(arg)
	if !ok1 || !ok2 {
		return false
	}

	switch operator {
	case "gt":
		return cval > val
	case "gte":
		return cval >= val
	case "lt":
		return cval < val
	case "lte":
		return cval <= val
	default:
		return false
	}
}

// handleStringOperator 处理字符串操作符。
func handleStringOperator(cvalue any, ctype reflect.Type, operator string, arg any) bool {
	if ctype.Kind() != reflect.String {
		return false
	}

	str := cvalue.(string)
	pattern := arg.(string)

	switch operator {
	case "contains":
		return strings.Contains(str, pattern)
	case "startswith":
		return strings.HasPrefix(str, pattern)
	case "endswith":
		return strings.HasSuffix(str, pattern)
	default:
		return false
	}
}

// isNumericType 是类型判断辅助函数。
func isNumericType(t reflect.Type) bool {
	return isIntegerType(t) || isFloatType(t)
}

// isIntegerType 判断是否整数类型。
func isIntegerType(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Int, reflect.Int32, reflect.Int64:
		return true
	default:
		return false
	}
}

// isFloatType 判断是否浮点类型。
func isFloatType(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Float32, reflect.Float64:
		return true
	default:
		return false
	}
}

// toInt64 是 Int64 类型转换辅助函数。
func toInt64(v any) (int64, bool) {
	switch val := v.(type) {
	case int:
		return int64(val), true
	case int32:
		return int64(val), true
	case int64:
		return val, true
	default:
		rv := reflect.ValueOf(v)
		if isIntegerType(rv.Type()) {
			return rv.Int(), true
		}
		return 0, false
	}
}

// toFloat64 是 Float64 类型转换辅助函数。
func toFloat64(v any) (float64, bool) {
	if v == nil {
		return 0, false
	}

	switch val := v.(type) {
	case float32:
		return float64(val), true
	case float64:
		return val, true
	default:
		rv := reflect.ValueOf(v)
		if isFloatType(rv.Type()) {
			return rv.Float(), true
		}
		return 0, false
	}
}
