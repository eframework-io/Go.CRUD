// Copyright (c) 2025 EFramework Innovation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package XOrm

import (
	"fmt"
	"sync"
	"testing"

	"github.com/petermattis/goid"
	"github.com/stretchr/testify/assert"
)

// TestContextIncre 测试自增操作。
func TestContextIncre(t *testing.T) {
	tests := []struct {
		cache    bool
		writable bool
	}{
		{true, true},
		{true, false},
		{false, true},
		{false, false},
	}

	defer ResetContext()
	defer ResetBaseTest()

	model := NewTestBaseModel()

	for _, test := range tests {
		ResetContext()
		ResetBaseTest()
		SetupBaseTest(test.cache, test.writable)
		WriteBaseTest(1000)

		t.Run(fmt.Sprintf("%+v", test), func(t *testing.T) {
			var wg sync.WaitGroup
			for i := range 100 {
				wg.Add(1)

				go func(j int) {
					defer wg.Done()

					gid := goid.Get()
					ctx := &context{}
					ctx.writable = test.writable
					contextMap.Store(gid, ctx)
					defer contextMap.Delete(gid)

					Increment(model)
					Increment(model, "int_val", 2)
				}(i)
			}
			wg.Wait()

			if !test.writable {
				if _, ok := globalIncrementMap.Load(fmt.Sprintf("%v_%v", model.ModelUnique(), "id")); ok {
					t.Errorf("只读模式下 id 自增值应当为空")
				}
				if _, ok := globalIncrementMap.Load(fmt.Sprintf("%v_%v", model.ModelUnique(), "int_val")); ok {
					t.Errorf("只读模式下 int_val 自增值应当为空")
				}
			} else {
				idIncre, _ := globalIncrementMap.Load(fmt.Sprintf("%v_%v", model.ModelUnique(), "id"))
				intValIncre, _ := globalIncrementMap.Load(fmt.Sprintf("%v_%v", model.ModelUnique(), "int_val"))
				assert.Equal(t, 1100, int(*(idIncre).(*int64)), "id 的自增值应当为 1100。")
				assert.Equal(t, 1200, int(*(intValIncre).(*int64)), "int_val 的自增值应当为 1200。")
			}
		})
	}
}
