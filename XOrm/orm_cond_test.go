// Copyright (c) 2025 EFramework Innovation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package XOrm

import (
	"fmt"
	"sync"
	"testing"

	"github.com/beego/beego/v2/client/orm"
	"github.com/stretchr/testify/assert"
)

func TestOrmCond(t *testing.T) {
	t.Run("New", func(t *testing.T) {
		defer exprParserCache.Clear()
		exprParserCache.Clear()

		tests := []struct {
			name string
			args []any
		}{
			{
				name: "Empty",
				args: []any{},
			},
			{
				name: "Existing",
				args: []any{orm.NewCondition()},
			},
			{
				name: "Expression",
				args: []any{"name == {0}", "test"},
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				wg := sync.WaitGroup{}
				for range 100 {
					wg.Add(1)
					go func() {
						defer wg.Done()
						cond := Cond(test.args...)
						assert.NotNil(t, cond, "创建的表达式实例应当不为空。")
						assert.NotNil(t, cond.Base, "创建的表达式实例 Base 应当不为空。")
					}()
				}
				wg.Wait()
			})
		}
	})

	t.Run("Parse", func(t *testing.T) {
		defer exprParserCache.Clear()
		exprParserCache.Clear()

		tests := []struct {
			expr  string
			args  []any
			panic bool
		}{
			// 基本操作符测试 - 正常情况
			{"age > {0}", []any{18}, false},
			{"age>{0}", []any{18}, false},
			{"age >= {0}", []any{18}, false},
			{"age>={0}", []any{18}, false},
			{"age < {0}", []any{30}, false},
			{"age<{0}", []any{30}, false},
			{"age <= {0}", []any{30}, false},
			{"age<={0}", []any{30}, false},
			{"name == {0}", []any{"test"}, false},
			{"name=={0}", []any{"test"}, false},
			{"name != {0}", []any{"test"}, false},
			{"name!={0}", []any{"test"}, false},
			{"name contains {0}", []any{"test"}, false},
			{"name startswith {0}", []any{"test"}, false},
			{"name endswith {0}", []any{"test"}, false},
			{"active isnull {0}", []any{true}, false},

			// 复合条件测试 - 正常情况
			{"(age > {0} && name contains {1}) || (status == {2})", []any{18, "test", "active"}, false},
			{"!(age < {0}) && !(name == {1})", []any{20, "test"}, false},
			{"((age >= {0} && age <= {1}) || (score > {2})) && active == {3}", []any{18, 30, 90, true}, false},

			// 分页参数测试 - 正常情况
			{"limit = {0}", []any{1}, false},
			{"limit={0}", []any{1}, false},
			{"offset = {0}", []any{1}, false},
			{"offset={0}", []any{1}, false},
			{"name == {0} && age > {1} && limit = {2} && offset = {3}", []any{"test", 10, 20, 30}, false},
			{"age > {0} && limit = {1}", []any{18, 10}, false},
			{"age > {0} && offset = {1}", []any{18, 5}, false},
			{"age > {0} && limit = {1} && offset = {2}", []any{18, 10, 5}, false},

			// 语法错误测试
			{"((a > {0})", []any{1}, true},                                  // 括号不匹配
			{"a > {abc}", []any{1}, true},                                   // 参数索引格式错误
			{"a  b", []any{}, true},                                         // 无效的表达式
			{"a > {0} limit {1}", []any{1, 1}, true},                        // limit 没有使用赋值符号和逻辑连接符
			{"a > {0} offset {1}", []any{1, 1}, true},                       // offset 没有使用赋值符号和逻辑连接符
			{"a > {0} && limit {1}", []any{1, 1}, true},                     // limit 没有使用赋值符号
			{expr: "a > {0} && offset {1}", args: []any{1, 1}, panic: true}, // offset 没有使用赋值符号

			// 参数错误测试
			{"a > {0} && b > {2}", []any{1}, true},                 // 参数索引超出范围
			{"a > {-1}", []any{1}, true},                           // 负数参数索引
			{"a > {0} && limit = {1}", []any{1, "invalid"}, true},  // limit 参数类型错误
			{"a > {0} && offset = {1}", []any{1, "invalid"}, true}, // offset 参数类型错误
			{"a > {0} && limit == {1}", []any{1, 2}, true},         // limit 参数赋值错误
			{"a > {0} && offset == {1}", []any{1, 2}, true},        // offset 参数赋值错误

			// 复杂组合测试
			{"(a > {0} || b < {1}) && (c == {2} || d != {3}) && limit = {4} && offset = {5}", []any{1, 2, "test", "sample", 10, 20}, false},
			{"!(a > {0}) && b contains {1} && limit = {2}", []any{10, "test", 5}, false},
		}

		for _, test := range tests {
			t.Run(fmt.Sprintf("%+v", test), func(t *testing.T) {
				wg := sync.WaitGroup{}
				for range 100 {
					wg.Add(1)
					go func() {
						defer wg.Done()
						defer func() {
							r := recover()
							if test.panic {
								assert.Equal(t, r != nil, true, "错误的表达式应当 panic。")
							} else {
								assert.Equal(t, r == nil, true, "正常的表达式不应当 panic。")
							}
						}()

						cond := Cond(append([]any{test.expr}, test.args...)...)

						var parser *exprParser
						if tmp, _ := exprParserCache.Load(test.expr); tmp != nil {
							parser = tmp.(*exprParser)
						}
						assert.NotNil(t, parser, "解析后的表达式应该被缓存。")

						var tcount int
						var visit func(cond *orm.Condition)
						visit = func(cond *orm.Condition) {
							if cond == nil {
								return
							}
							params := getCondParams(cond)
							for _, param := range params {
								if param.args != nil {
									assert.Equal(t, test.args[tcount], param.args[0], "解析后的参数应当和输入的相等。")
									tcount++ // 参数是按顺序设置的，且为单表达式
								}
								if param.isCond && param.cond != nil {
									visit(param.cond)
								}
							}
						}
						visit(cond.Base)

						if parser.limit != -1 {
							assert.Equal(t, test.args[parser.limit], cond.Limit, "解析后的分页限定参数应当和输入的相等。")
						}

						if parser.offset != -1 {
							assert.Equal(t, test.args[parser.offset], cond.Offset, "解析后的分页偏移参数应当和输入的相等。")
						}
					}()
				}
				wg.Wait()
			})
		}
	})

	t.Run("Cache", func(t *testing.T) {
		defer exprParserCache.Clear()
		exprParserCache.Clear()

		expr := "name == {0} && age > {1}"
		exprCondition(expr, []any{"test", 18})
		var parser *exprParser
		if tmp, _ := exprParserCache.Load(expr); tmp != nil {
			parser = tmp.(*exprParser)
		}
		assert.NotNil(t, parser, "解析后的表达式应该被缓存。")

		var wg sync.WaitGroup
		for range 100 {
			wg.Add(1)
			go func() {
				defer wg.Done()

				exprCondition(expr, []any{"test", 18})
				var nparser *exprParser
				if tmp, _ := exprParserCache.Load(expr); tmp != nil {
					nparser = tmp.(*exprParser)
				}
				assert.Equal(t, parser, nparser, "解析同一个表达式时应当返回相同的实例。")
			}()
		}
		wg.Wait()
	})
}
