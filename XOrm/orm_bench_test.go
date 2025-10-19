// Copyright (c) 2025 EFramework Innovation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package XOrm

import (
	"fmt"
	"testing"

	"github.com/beego/beego/v2/client/orm"
)

// BenchmarkOrm 比较 XOrm 缓存查询和回源查询的性能。
// 多核测试可以使用命令 go test -run=^$ -bench=^BenchmarkOrm$ -benchmem -cpu=1,2,4,8,16,32
func BenchmarkOrm(b *testing.B) {
	defer ResetContext()
	defer ResetBaseTest()

	for _, count := range []int{100000} {
		ResetContext()
		ResetBaseTest()
		SetupBaseTest()
		WriteBaseTest(count)

		model := NewTestBaseModel()
		Watch()
		List(model) // 读取至缓存
		Defer()

		conds := []struct {
			Name string
			Cond *Condition
		}{
			{"int_val > 0 && string_val == 'hello world'", Cond("int_val > {0} && string_val == {1}", 0, "hello world")},
			{"int_val__in int", Cond(orm.NewCondition().And("int_val__in", make([]int, count/2)))},
		}
		for _, cond := range conds {
			b.Run(fmt.Sprintf("XOrm.List/%d/%s", count, cond.Name), func(b *testing.B) {
				Watch()
				List(model, cond.Cond)
				Defer()
			})

			b.Run(fmt.Sprintf("XOrm.Model.List/%d/%s", count, cond.Name), func(b *testing.B) {
				var rets []*TestBaseModel
				model.List(&rets, cond.Cond)
			})
		}
	}
}
