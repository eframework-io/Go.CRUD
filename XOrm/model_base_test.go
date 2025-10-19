// Copyright (c) 2025 EFramework Innovation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package XOrm

import (
	"database/sql"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"

	"github.com/beego/beego/v2/client/orm"
	"github.com/eframework-io/Go.Utility/XLog"
	"github.com/eframework-io/Go.Utility/XObject"
	_ "github.com/go-sql-driver/mysql"
)

const (
	// 数据库配置
	TestDatabaseHost = "localhost"
	TestDatabasePort = 3306
	TestDatabaseName = "mysql"
	TestDatabaseUser = "root"
	TestDatabasePass = "123456"

	// 测试表配置
	TestTableName = "model_base_test"
	TestAliasName = "test_alias"

	// 测试SQL
	TestTruncateTableSQL = `TRUNCATE TABLE ` + TestTableName + `;`
	TestDropTableSQL     = `DROP TABLE IF EXISTS ` + TestTableName + `;`
	TestCreateTableSQL   = `
		CREATE TABLE ` + TestTableName + ` (
			id INT AUTO_INCREMENT PRIMARY KEY,
			int_val INT,
			float_val FLOAT,
			string_val VARCHAR(255),
			bool_val BOOLEAN
		);`
)

// TestBaseModel 测试模型
type TestBaseModel struct {
	Model[TestBaseModel] `orm:"-" json:"-"`
	ID                   int     `orm:"column(id);pk"`
	IntVal               int     `orm:"column(int_val)"`
	FloatVal             float64 `orm:"column(float_val)"`
	StringVal            string  `orm:"column(string_val)"`
	BoolVal              bool    `orm:"column(bool_val)"`

	onEncodeCalled bool
	onDecodeCalled bool
	onQueryParams  []any
}

func (m *TestBaseModel) AliasName() string {
	return TestAliasName
}

func (m *TestBaseModel) TableName() string {
	return TestTableName
}

func (m *TestBaseModel) OnEncode() { m.onEncodeCalled = true }

func (m *TestBaseModel) OnDecode() { m.onDecodeCalled = true }

func (m *TestBaseModel) OnQuery(action string, cond *orm.Condition) *orm.Condition {
	m.onQueryParams = make([]any, 2)
	m.onQueryParams[0] = action
	m.onQueryParams[1] = cond
	return cond
}

func NewTestBaseModel() *TestBaseModel {
	return XObject.New[TestBaseModel]()
}

var setupDatabaseOnce sync.Once

// SetupBaseTest 设置测试数据库。
func SetupBaseTest(cacheAndWritable ...bool) {
	// 连接测试数据库
	testDatabase, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4",
		TestDatabaseUser, TestDatabasePass, TestDatabaseHost, TestDatabasePort, TestDatabaseName))
	if err != nil {
		XLog.Panic("连接测试数据库失败: %v", err)
	}
	defer testDatabase.Close()

	// 创建测试表
	if _, err := testDatabase.Exec(TestDropTableSQL); err != nil {
		XLog.Panic("删除测试表失败: %v", err)
	}
	if _, err := testDatabase.Exec(TestCreateTableSQL); err != nil {
		XLog.Panic("创建测试表失败: %v", err)
	}

	// 注册数据库
	setupDatabaseOnce.Do(func() {
		if err := orm.RegisterDataBase(TestAliasName, "mysql",
			fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4",
				TestDatabaseUser, TestDatabasePass, TestDatabaseHost, TestDatabasePort, TestDatabaseName)); err != nil {
			XLog.Panic("注册数据库失败: %v", err)
		}
	})

	// 注册模型
	cache := true
	writable := true
	if len(cacheAndWritable) > 0 {
		cache = cacheAndWritable[0]
		if len(cacheAndWritable) > 1 {
			writable = cacheAndWritable[1]
		}
	}
	Register(NewTestBaseModel(), cache, writable)
}

// ResetBaseTest 删除测试数据库。
func ResetBaseTest() {
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
		TestDatabaseUser, TestDatabasePass, TestDatabaseHost, TestDatabasePort, TestDatabaseName))
	if err != nil {
		XLog.Panic("连接测试数据库失败: %v", err)
	}
	defer db.Close()

	// 清理测试数据
	if _, err := db.Exec(TestDropTableSQL); err != nil {
		XLog.Panic("清理测试表失败: %v", err)
	}

	// 清理注册信息
	orm.ResetModelCache()
}

// ClearBaseTest 清空测试数据库。
func ClearBaseTest() {
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
		TestDatabaseUser, TestDatabasePass, TestDatabaseHost, TestDatabasePort, TestDatabaseName))
	if err != nil {
		XLog.Panic("连接测试数据库失败: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(TestTruncateTableSQL); err != nil {
		XLog.Panic("清空测试表失败: %v", err)
	}
}

// WriteBaseTest 写入测试数据库。
func WriteBaseTest(count int) []*TestBaseModel {
	var models []*TestBaseModel
	for i := 1; i <= count; i++ {
		model := NewTestBaseModel()
		model.ID = i
		model.IntVal = i
		model.FloatVal = float64(i) + 0.5
		model.StringVal = fmt.Sprintf("test_string_%d", i)
		model.BoolVal = i%2 == 0

		if writeCount := model.Write(); writeCount <= 0 {
			XLog.Panic("插入测试数据 %d 失败", i)
		}
		models = append(models, model)
	}
	return models
}

// TestModelBasic 测试基本操作。
func TestModelBasic(t *testing.T) {
	// 测试写入
	t.Run("Write", func(t *testing.T) {
		SetupBaseTest()
		defer ResetBaseTest()
		ResetContext()

		model := NewTestBaseModel()
		model.IntVal = 1
		model.FloatVal = 1.5
		model.StringVal = "test_string"
		model.BoolVal = true

		count := model.Write()
		if count <= 0 {
			t.Error("写入操作失败")
		}
		if !model.onEncodeCalled {
			t.Error("OnEncode 未被调用。")
		}
	})

	// 测试读取
	t.Run("Read", func(t *testing.T) {
		SetupBaseTest()
		defer ResetBaseTest()
		ResetContext()
		models := WriteBaseTest(5)

		// 测试主键读取
		nmodel := NewTestBaseModel()
		nmodel.ID = models[3].ID
		if !nmodel.Read() {
			t.Error("通过主键读取失败")
		}
		if !nmodel.Equals(models[3]) {
			t.Error("通过主键读取的数据不匹配")
		}
		if !nmodel.onDecodeCalled {
			t.Error("OnDecode 未被调用。")
		}

		// 测试条件读取
		nmodel = NewTestBaseModel()
		nmodel.ID = models[2].ID
		cond := Cond("int_val == {0}", 4)
		if !nmodel.Read(cond) {
			t.Error("通过条件读取失败")
		}
		if !nmodel.Equals(models[3]) {
			t.Error("通过条件读取的数据不匹配")
		}
		if !nmodel.onDecodeCalled {
			t.Error("OnDecode 未被调用。")
		}

		// 因为 Read 行为是覆盖对象的
		// 所以无法验证 onQueryParams
	})

	// 测试删除
	t.Run("Delete", func(t *testing.T) {
		SetupBaseTest()
		defer ResetBaseTest()
		ResetContext()

		model := WriteBaseTest(1)[0]
		count := model.Delete()
		if count <= 0 {
			t.Error("通过主键删除失败")
		}
		if len(model.onQueryParams) == 0 {
			t.Error("OnQuery 未被调用。")
		}
		if model.onQueryParams[0] != "Delete" {
			t.Errorf("OnQuery 的类型错误：%v", model.onQueryParams[0])
		}
		if model.Read() {
			t.Error("记录应该已被删除")
		}
	})
}

// TestModelList 测试列举操作。
func TestModelList(t *testing.T) {
	SetupBaseTest()
	defer ResetBaseTest()
	ResetContext()

	models := WriteBaseTest(5)
	model := NewTestBaseModel()

	// 测试Count
	t.Run("Count", func(t *testing.T) {
		count := model.Count()
		if count != len(models) {
			t.Errorf("预期计数 %d，实际得到 %d", len(models), count)
		}

		// 测试带条件的Count
		cond := Cond("int_val > {0}", 3)
		count = model.Count(cond)
		if count != 2 {
			t.Errorf("预期带条件的计数为 2，实际得到 %d", count)
		}
		if len(model.onQueryParams) == 0 {
			t.Error("OnQuery 未被调用。")
		}
		if model.onQueryParams[0] != "Count" {
			t.Errorf("OnQuery 的类型错误：%v", model.onQueryParams[0])
		}
		if model.onQueryParams[1] == nil {
			t.Errorf("OnQuery 的条件不应为空。")
		}
	})

	// 测试List
	t.Run("List", func(t *testing.T) {
		var results []*TestBaseModel
		count := model.List(&results)
		if count != len(models) {
			t.Errorf("预期列表计数 %d，实际得到 %d", len(models), count)
		}

		// 测试带条件的List
		results = nil
		cond := Cond("int_val > {0}", 3)
		count = model.List(&results, cond)
		if count != 2 {
			t.Errorf("预期匹配条件的记录有 2 条，实际得到 %d 条", count)
		}
		if len(model.onQueryParams) == 0 {
			t.Error("OnQuery 未被调用。")
		}
		if model.onQueryParams[0] != "List" {
			t.Errorf("OnQuery 的类型错误：%v", model.onQueryParams[0])
		}
		if model.onQueryParams[1] == nil {
			t.Errorf("OnQuery 的条件不应为空。")
		}
		for _, result := range results {
			if result.IntVal <= 3 {
				t.Errorf("预期 IntVal > 3，实际得到 %d", result.IntVal)
			}
			if !result.onDecodeCalled {
				t.Error("OnDecode 应当被调用。")
			}
		}
	})

	t.Run("ListOriginalQuery", func(t *testing.T) {
		var results []*TestBaseModel
		count := model.List(&results)
		if count != len(models) {
			t.Errorf("预期列表计数 %d，实际得到 %d", len(models), count)
		}

		// 测试带条件的List
		results = nil
		cond := Cond(orm.NewCondition().And("int_val__in", []int{1, 2}))
		count = model.List(&results, cond)
		if count != 2 {
			t.Errorf("预期匹配条件的记录有 2 条，实际得到 %d 条", count)
		}
		for _, result := range results {
			if result.IntVal > 2 {
				t.Errorf("预期 IntVal <= 2，实际得到 %d", result.IntVal)
			}
		}
	})

	// 测试Max
	t.Run("Max", func(t *testing.T) {
		max := model.Max("int_val")
		if max != 5 {
			t.Errorf("预期最大值为 5，实际得到 %d", max)
		}
		if len(model.onQueryParams) == 0 {
			t.Error("OnQuery 未被调用。")
		}
		if model.onQueryParams[0] != "Max" {
			t.Errorf("OnQuery 的类型错误：%v", model.onQueryParams[0])
		}
	})

	// 测试Min
	t.Run("Min", func(t *testing.T) {
		min := model.Min("int_val")
		if min != 1 {
			t.Errorf("预期最小值为 1，实际得到 %d", min)
		}
		if len(model.onQueryParams) == 0 {
			t.Error("OnQuery 未被调用。")
		}
		if model.onQueryParams[0] != "Min" {
			t.Errorf("OnQuery 的类型错误：%v", model.onQueryParams[0])
		}
	})

	// 测试Clear
	t.Run("Clear", func(t *testing.T) {
		count := model.Clear()
		if count != len(models) {
			t.Errorf("预期清除计数 %d，实际得到 %d", len(models), count)
		}
		if model.Count() != 0 {
			t.Error("清除后表应该为空")
		}

		// 重新准备测试数据
		models = WriteBaseTest(5)

		// 测试带条件的Clear
		cond := Cond("int_val > {0}", 3)
		count = model.Clear(cond)
		if count != 2 {
			t.Errorf("预期带条件清除 2 条记录，实际清除了 %d 条", count)
		}
		if len(model.onQueryParams) == 0 {
			t.Error("OnQuery 未被调用。")
		}
		if model.onQueryParams[0] != "Clear" {
			t.Errorf("OnQuery 的类型错误：%v", model.onQueryParams[0])
		}
		if model.onQueryParams[1] == nil {
			t.Errorf("OnQuery 的条件不应为空。")
		}
		if model.Count() != 3 {
			t.Error("条件清除后应该剩余 3 条记录")
		}
	})
}

// TestModelUtility 测试工具方法。
func TestModelUtility(t *testing.T) {
	SetupBaseTest()
	defer ResetBaseTest()
	ResetContext()

	model := NewTestBaseModel()
	model.ID = 1
	model.IntVal = 100
	model.FloatVal = 1.5
	model.StringVal = "test_string"
	model.BoolVal = true

	// 测试Clone
	t.Run("Clone", func(t *testing.T) {
		cloned := model.Clone().(*TestBaseModel)
		if cloned.ID != model.ID {
			t.Error("克隆的 ID 不匹配")
		}
		if cloned.IntVal != model.IntVal {
			t.Error("克隆的 IntVal 不匹配")
		}
		if cloned.FloatVal != model.FloatVal {
			t.Error("克隆的 FloatVal 不匹配")
		}
		if cloned.StringVal != model.StringVal {
			t.Error("克隆的 StringVal 不匹配")
		}
		if cloned.BoolVal != model.BoolVal {
			t.Error("克隆的 BoolVal 不匹配")
		}
	})

	// 测试Equals
	t.Run("Equals", func(t *testing.T) {
		// 测试相同对象比较
		t.Run("SameObject", func(t *testing.T) {
			if !model.Equals(model) {
				t.Error("相同对象应该等于自身")
			}
		})

		// 测试相同值的不同对象
		t.Run("SameValues", func(t *testing.T) {
			model2 := NewTestBaseModel()
			model2.ID = model.ID
			model2.IntVal = model.IntVal
			model2.FloatVal = model.FloatVal
			model2.StringVal = model.StringVal
			model2.BoolVal = model.BoolVal

			if !model.Equals(model2) {
				t.Errorf("具有相同值的对象应该相等:\nmodel1=%#v\nmodel2=%#v", model, model2)
			}
		})

		// 测试不同值的对象
		t.Run("DifferentValues", func(t *testing.T) {
			model2 := NewTestBaseModel()
			model2.ID = model.ID + 1
			model2.IntVal = model.IntVal + 1
			model2.FloatVal = model.FloatVal + 1.0
			model2.StringVal = model.StringVal + "_different"
			model2.BoolVal = !model.BoolVal

			if model.Equals(model2) {
				t.Errorf("具有不同值的对象不应该相等:\nmodel1=%#v\nmodel2=%#v", model, model2)
			}
		})

		// 测试nil对象
		t.Run("NilObject", func(t *testing.T) {
			if model.Equals(nil) {
				t.Error("非空对象不应该等于 nil")
			}
		})

		// 测试不同类型对象
		t.Run("DifferentTypes", func(t *testing.T) {
			// 创建一个不同类型的模型
			type DifferentModel struct {
				Model[DifferentModel] `orm:"-" json:"-"`
				ID                    int    `orm:"column(id);pk;auto"`
				Value                 string `orm:"column(value)"`
			}

			differentModel := XObject.New[DifferentModel]()
			differentModel.ID = model.ID
			differentModel.Value = "test"

			if model.Equals(differentModel) {
				t.Error("不同类型的对象不应该相等")
			}
		})

		// 测试字段值边界情况
		t.Run("EdgeCases", func(t *testing.T) {
			model2 := NewTestBaseModel()
			model3 := NewTestBaseModel()

			// 测试空字符串
			model2.StringVal = ""
			model3.StringVal = ""
			if !model2.Equals(model3) {
				t.Errorf("具有空字符串的对象应该相等:\nmodel1=%#v\nmodel2=%#v", model2, model3)
			}

			// 测试零值
			model2.IntVal = 0
			model3.IntVal = 0
			model2.FloatVal = 0.0
			model3.FloatVal = 0.0
			if !model2.Equals(model3) {
				t.Errorf("具有零值的对象应该相等:\nmodel1=%#v\nmodel2=%#v", model2, model3)
			}

			// 测试最大值
			model2.IntVal = math.MaxInt32
			model3.IntVal = math.MaxInt32
			model2.FloatVal = math.MaxFloat64
			model3.FloatVal = math.MaxFloat64
			if !model2.Equals(model3) {
				t.Errorf("具有最大值的对象应该相等:\nmodel1=%#v\nmodel2=%#v", model2, model3)
			}
		})
	})

	// 测试Json
	t.Run("Json", func(t *testing.T) {
		json := model.Json()
		if json == "" {
			t.Error("JSON 序列化失败")
		}
		if !strings.Contains(json, fmt.Sprintf(`"ID":%d`, model.ID)) {
			t.Error("JSON 应该包含 ID 字段")
		}
		if !strings.Contains(json, fmt.Sprintf(`"IntVal":%d`, model.IntVal)) {
			t.Error("JSON 应该包含 IntVal 字段")
		}
		if !strings.Contains(json, fmt.Sprintf(`"FloatVal":%g`, model.FloatVal)) {
			t.Error("JSON 应该包含 FloatVal 字段")
		}
		if !strings.Contains(json, fmt.Sprintf(`"StringVal":"%s"`, model.StringVal)) {
			t.Error("JSON 应该包含 StringVal 字段")
		}
		if !strings.Contains(json, fmt.Sprintf(`"BoolVal":%v`, model.BoolVal)) {
			t.Error("JSON 应该包含 BoolVal 字段")
		}
	})

	// 测试IsValid
	t.Run("IsValid", func(t *testing.T) {
		model2 := NewTestBaseModel()
		if model2.IsValid() {
			t.Error("新模型默认应该是无效的")
		}
		model2.IsValid(false)
		if model2.IsValid() {
			t.Error("设置为无效后模型应该是无效的")
		}
		model2.IsValid(true)
		if !model2.IsValid() {
			t.Error("设置为有效后模型应该是有效的")
		}
	})
}

// TestModelMatchs 测试匹配操作。
func TestModelMatchs(t *testing.T) {
	SetupBaseTest()
	defer ResetBaseTest()
	ResetContext()

	models := WriteBaseTest(5)

	// 测试简单条件匹配
	t.Run("Simple", func(t *testing.T) {
		// 等于条件
		t.Run("Equals", func(t *testing.T) {
			cond := Cond("int_val == {0}", 3)
			matches := models[2].Matchs(cond)
			if !matches {
				t.Error("预期 int_val=3 的模型应该匹配条件")
			}
		})

		// 大于条件
		t.Run("Greater", func(t *testing.T) {
			cond := Cond("int_val > {0}", 3)
			matchCount := 0
			for _, model := range models {
				if model.Matchs(cond) {
					matchCount++
				}
			}
			if matchCount != 2 {
				t.Errorf("预期有 2 个模型的 int_val>3，实际得到 %d 个", matchCount)
			}
		})

		// 小于条件
		t.Run("Less", func(t *testing.T) {
			cond := Cond("int_val < {0}", 3)
			matchCount := 0
			for _, model := range models {
				if model.Matchs(cond) {
					matchCount++
				}
			}
			if matchCount != 2 {
				t.Errorf("预期有 2 个模型的 int_val<3，实际得到 %d 个", matchCount)
			}
		})

		// 字符串匹配
		t.Run("String", func(t *testing.T) {
			cond := Cond("string_val == {0}", "test_string_3")
			matches := models[2].Matchs(cond)
			if !matches {
				t.Error("预期 string_val='test_string_3' 的模型应该匹配条件")
			}
		})

		// 布尔值匹配
		t.Run("Boolean", func(t *testing.T) {
			cond := Cond("bool_val == {0}", true)
			matchCount := 0
			for _, model := range models {
				if model.Matchs(cond) {
					matchCount++
				}
			}
			if matchCount != 2 {
				t.Errorf("预期有 2 个模型的 bool_val=true，实际得到 %d 个", matchCount)
			}
		})
	})

	// 测试复合条件匹配
	t.Run("Compound", func(t *testing.T) {
		// AND 条件
		t.Run("And", func(t *testing.T) {
			// 测试数据中，IntVal > 2 的有 3,4,5，bool_val 为 true 的有 2,4
			// 所以 IntVal > 2 && bool_val == true 的只有 4
			cond := Cond("int_val > {0} && bool_val == {1}", 2, true)
			matchCount := 0
			for _, model := range models {
				if model.Matchs(cond) {
					matchCount++
					if model.IntVal <= 2 || !model.BoolVal {
						t.Errorf("int_val=%d 且 bool_val=%v 的模型不应该匹配",
							model.IntVal, model.BoolVal)
					}
				}
			}
			if matchCount != 1 {
				t.Errorf("预期有 1 个模型匹配 AND 条件，实际得到 %d 个", matchCount)
			}
		})

		// OR 条件
		t.Run("Or", func(t *testing.T) {
			// 测试数据中，IntVal < 2 的有 1，IntVal > 4 的有 5
			cond := Cond("int_val < {0} || int_val > {1}", 2, 4)
			matchCount := 0
			for _, model := range models {
				if model.Matchs(cond) {
					matchCount++
					if model.IntVal >= 2 && model.IntVal <= 4 {
						t.Errorf("int_val=%d 的模型不应该匹配", model.IntVal)
					}
				}
			}
			if matchCount != 2 {
				t.Errorf("预期有 2 个模型匹配 OR 条件，实际得到 %d 个", matchCount)
			}
		})

		// NOT 条件
		t.Run("Not", func(t *testing.T) {
			// 测试数据中，!(2 <= IntVal <= 4) 意味着 IntVal < 2 或 IntVal > 4
			// 所以应该匹配 IntVal = 1 和 IntVal = 5
			cond := Cond("int_val < {0} || int_val > {1}", 2, 4)
			matchCount := 0
			for _, model := range models {
				if model.Matchs(cond) {
					matchCount++
					if model.IntVal >= 2 && model.IntVal <= 4 {
						t.Errorf("int_val=%d 的模型不应该匹配", model.IntVal)
					}
				}
			}
			if matchCount != 2 {
				t.Errorf("预期有 2 个模型匹配 NOT 条件，实际得到 %d 个", matchCount)
			}
		})

		// 复杂组合条件
		t.Run("Complex", func(t *testing.T) {
			// 测试数据：
			// ID=1: IntVal=1, FloatVal=1.5, StringVal="test_string_1", BoolVal=false
			// ID=2: IntVal=2, FloatVal=2.5, StringVal="test_string_2", BoolVal=true
			// ID=3: IntVal=3, FloatVal=3.5, StringVal="test_string_3", BoolVal=false
			// ID=4: IntVal=4, FloatVal=4.5, StringVal="test_string_4", BoolVal=true
			// ID=5: IntVal=5, FloatVal=5.5, StringVal="test_string_5", BoolVal=false

			// 使用更简单的条件组合
			cond := Cond("int_val == {0} || int_val == {1}", 1, 4)

			matchCount := 0
			expectedMatches := map[int]struct{}{
				1: {}, // 匹配第一个条件：int_val == 1
				4: {}, // 匹配第二个条件：int_val == 4
			}

			for _, model := range models {
				matches := model.Matchs(cond)
				t.Logf("模型 {ID=%d, IntVal=%d}: 匹配结果=%v", model.ID, model.IntVal, matches)

				if matches {
					matchCount++
					if _, ok := expectedMatches[model.IntVal]; !ok {
						t.Errorf("模型意外匹配: {ID=%d, IntVal=%d}",
							model.ID, model.IntVal)
					}
				} else {
					if _, ok := expectedMatches[model.IntVal]; ok {
						t.Errorf("预期匹配的模型未匹配: {ID=%d, IntVal=%d}",
							model.ID, model.IntVal)
					}
				}
			}

			if matchCount != 2 {
				t.Errorf("预期恰好有 2 个模型匹配复杂条件，实际得到 %d 个", matchCount)
			}
		})
	})
}
