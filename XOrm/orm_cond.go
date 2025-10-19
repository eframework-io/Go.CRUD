// Copyright (c) 2025 EFramework Innovation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package XOrm

import (
	"strconv"
	"strings"
	"sync"
	"unsafe"

	"github.com/beego/beego/v2/client/orm"
	"github.com/eframework-io/Go.Utility/XLog"
	"github.com/eframework-io/Go.Utility/XObject"
)

// Condition 表示一个查询条件，包含基础条件和分页信息。
type Condition struct {
	Base    *orm.Condition // 基础条件
	Limit   int            // 分页限定
	Offset  int            // 分页偏移
	context sync.Map       // 上下文信息
}

// Ctor 初始化条件。
func (c *Condition) Ctor(obj any) {
	c.Base = orm.NewCondition()
}

// Cond 创建新的条件。
//
// 用法:
// 1. Cond() - 创建空条件
// 2. Cond(existingCond *orm.Cond) - 从现有条件创建
// 3. Cond("a > {0} && b == {1}", 1, 2) - 从表达式和参数创建
func Cond(condOrExprAndParams ...any) *Condition {
	c := XObject.New[Condition]()
	if len(condOrExprAndParams) == 0 {
		return c
	}

	if cond, ok := condOrExprAndParams[0].(*orm.Condition); ok {
		c.Base = cond
		return c
	}

	if expr, ok := condOrExprAndParams[0].(string); ok {
		if expr == "" {
			return c
		}
		params := condOrExprAndParams[1:]
		cond, limit, offset := exprCondition(expr, params)
		c.Base = cond
		c.Limit = limit
		c.Offset = offset
		return c
	}

	XLog.Panic("XOrm.Cond: invalid params type: %T", condOrExprAndParams[0])
	return nil
}

// condOpMap 是条件操作符映射表。
var condOpMap = map[string]string{
	">":          "__gt",
	">=":         "__gte",
	"<":          "__lt",
	"<=":         "__lte",
	"==":         "__exact",
	"!=":         "__ne",
	"contains":   "__contains",
	"startswith": "__startswith",
	"endswith":   "__endswith",
	"isnull":     "__isnull",
}

const (
	exprTokenTypeField    = iota // exprTokenTypeField 是字段名类型。
	exprTokenTypeOperator        // exprTokenTypeOperator 是内置操作符。
	exprTokenTypeParam           // exprTokenTypeParam 是参数占位符。
	exprTokenTypeAssign          // exprTokenTypeAssign 是赋值操作符。
	exprTokenTypeLogic           // exprTokenTypeLogic 是逻辑操作符。
	exprTokenTypeNot             // exprTokenTypeNot 是非操作符。
	exprTokenTypeLBracket        // exprTokenTypeLBracket 是左括号类型。
	exprTokenTypeRBracket        // exprTokenTypeRBracket 是右括号类型。
	exprTokenTypeLimit           // exprTokenTypeLimit 是限定关键字。
	exprTokenTypeOffset          // exprTokenTypeOffset 是偏移关键字。
)

// exprToken 是表达式词法单元。
type exprToken struct {
	typ   int    // 类型
	value string // 值
	pos   int    // 位置
}

// exprParser 是表达式解析器。
type exprParser struct {
	expr   string         // 表达式
	tokens []exprToken    // 词法单元
	pos    int            // 当前位置
	cond   *orm.Condition // 条件实例
	limit  int            // 分页限定
	offset int            // 分页偏移
}

// exprParserCache 是表达式解析器缓存。
var exprParserCache = sync.Map{}

// parse 解析表达式并构建条件。
func (parser *exprParser) parse(expr string, tokens []exprToken) {
	parser.expr = expr
	parser.tokens = tokens
	parser.pos = 0
	parser.limit = -1
	parser.offset = -1
	parser.cond = parser.condition()
}

// current 获取当前词法单元。
func (parser *exprParser) current() *exprToken {
	if parser.pos >= len(parser.tokens) {
		return nil
	}
	return &parser.tokens[parser.pos]
}

// next 前进到下一个词法单元。
func (parser *exprParser) next() { parser.pos++ }

// condition 将解析表达式并返回条件实例。
func (parser *exprParser) condition() *orm.Condition {
	cond := orm.NewCondition()

	// 处理括号表达式或简单表达式
	if parser.current() != nil && parser.current().typ == exprTokenTypeLBracket {
		parser.next() // 跳过左括号
		cond = parser.condition()

		if parser.current() != nil && parser.current().typ == exprTokenTypeRBracket {
			parser.next() // 跳过右括号
		} else {
			XLog.Panic("XOrm.Cond: missing right bracket, expression: %v", parser.expr)
		}
	} else if parser.current() != nil && parser.current().typ == exprTokenTypeNot {
		parser.next() // 跳过!

		if parser.current() != nil && parser.current().typ == exprTokenTypeLBracket {
			parser.next() // 跳过左括号
			rightCond := parser.condition()

			if parser.current() != nil && parser.current().typ == exprTokenTypeRBracket {
				parser.next() // 跳过右括号
				cond = cond.AndNotCond(rightCond)
			} else {
				XLog.Panic("XOrm.Cond: missing right bracket after not operator, expression: %v", parser.expr)
			}
		} else {
			// 处理简单的否定表达式
			if parser.paging() {
				// 如果是分页参数，直接返回
				return cond
			}

			// 解析字段名
			if parser.current() != nil && parser.current().typ == exprTokenTypeField {
				field := parser.current().value
				parser.next()

				// 解析操作符
				if parser.current() != nil && parser.current().typ == exprTokenTypeOperator {
					operator := parser.current().value
					suffix, ok := condOpMap[operator]
					if !ok {
						XLog.Panic("XOrm.Cond: unknown operator: %s, expression: %v", operator, parser.expr)
					}
					parser.next()

					// 解析参数
					if parser.current() != nil && parser.current().typ == exprTokenTypeParam {
						paramIdx := parser.param(parser.current().value)
						parser.next()

						// 构建条件
						fieldWithOp := field + suffix
						rightCond := orm.NewCondition().And(fieldWithOp, paramIdx)
						cond = cond.AndNotCond(rightCond)
					} else {
						XLog.Panic("XOrm.Cond: parameter required after operator, expression: %v", parser.expr)
					}
				} else {
					XLog.Panic("XOrm.Cond: operator required after field name, expression: %v", parser.expr)
				}
			} else {
				XLog.Panic("XOrm.Cond: field name required after not operator, expression: %v", parser.expr)
			}
		}
	} else {
		// 处理简单表达式
		if parser.paging() {
			// 如果是分页参数，直接返回
			return cond
		}

		// 解析字段名
		if parser.current() != nil && parser.current().typ == exprTokenTypeField {
			field := parser.current().value
			parser.next()

			// 解析操作符
			if parser.current() != nil && parser.current().typ == exprTokenTypeOperator {
				operator := parser.current().value
				suffix, ok := condOpMap[operator]
				if !ok {
					XLog.Panic("XOrm.Cond: unknown operator: %s, expression: %v", operator, parser.expr)
				}
				parser.next()

				// 解析参数
				if parser.current() != nil && parser.current().typ == exprTokenTypeParam {
					paramIdx := parser.param(parser.current().value)
					parser.next()

					// 构建条件
					fieldWithOp := field + suffix
					cond = cond.And(fieldWithOp, paramIdx)
				} else {
					XLog.Panic("XOrm.Cond: parameter required after operator, expression: %v", parser.expr)
				}
			} else {
				XLog.Panic("XOrm.Cond: operator required after field name, expression: %v", parser.expr)
			}
		} else {
			// 如果没有匹配任何模式，可能是语法错误
			if parser.current() != nil {
				XLog.Panic("XOrm.Cond: unexpected token: %s.", parser.current().value)
			} else {
				XLog.Panic("XOrm.Cond: unexpected end of expression, expression: %v", parser.expr)
			}
		}
	}

	// 处理后续的逻辑操作符
	for parser.current() != nil && parser.current().typ == exprTokenTypeLogic {
		logic := parser.current().value
		parser.next()

		// 处理 limit 和 offset 的语法
		if parser.paging() {
			continue
		}

		var rightCond *orm.Condition

		// 处理取反操作
		not := false
		if parser.current() != nil && parser.current().typ == exprTokenTypeNot {
			not = true
			parser.next()
		}

		// 解析右侧表达式
		if parser.current() != nil && parser.current().typ == exprTokenTypeLBracket {
			parser.next() // 跳过左括号
			rightCond = parser.condition()

			if parser.current() != nil && parser.current().typ == exprTokenTypeRBracket {
				parser.next() // 跳过右括号
			} else {
				XLog.Panic("XOrm.Cond: missing right bracket, expression: %v", parser.expr)
			}
		} else {
			// 解析字段名
			if parser.current() != nil && parser.current().typ == exprTokenTypeField {
				field := parser.current().value
				parser.next()

				// 解析操作符
				if parser.current() != nil && parser.current().typ == exprTokenTypeOperator {
					operator := parser.current().value
					suffix, ok := condOpMap[operator]
					if !ok {
						XLog.Panic("XOrm.Cond: unknown operator: %s, expression: %v", operator, parser.expr)
					}
					parser.next()

					// 解析参数
					if parser.current() != nil && parser.current().typ == exprTokenTypeParam {
						paramIdx := parser.param(parser.current().value)
						parser.next()

						// 构建条件
						fieldWithOp := field + suffix
						rightCond = orm.NewCondition()
						if not {
							rightCond = rightCond.AndNot(fieldWithOp, paramIdx)
						} else {
							rightCond = rightCond.And(fieldWithOp, paramIdx)
						}
					} else {
						XLog.Panic("XOrm.Cond: parameter required after operator, expression: %v", parser.expr)
					}
				} else {
					XLog.Panic("XOrm.Cond: operator required after field name, expression: %v", parser.expr)
				}
			} else {
				// 如果没有匹配任何模式，可能是语法错误
				if parser.current() != nil {
					XLog.Panic("XOrm.Cond: unexpected token, expression: %v", parser.expr)
				} else {
					XLog.Panic("XOrm.Cond: unexpected end of expression, expression: %v", parser.expr)
				}
			}

			not = false // 已经处理了否定
		}

		// 应用逻辑操作
		switch logic {
		case "&&":
			if not {
				cond = cond.AndNotCond(rightCond)
			} else {
				cond = cond.AndCond(rightCond)
			}
		case "||":
			if not {
				cond = cond.OrNotCond(rightCond)
			} else {
				cond = cond.OrCond(rightCond)
			}
		}
	}

	// 如果还有 token 未处理并且不是 exprTokenTypeRBracket 则判定为语法错误
	if parser.current() != nil && parser.current().typ != exprTokenTypeRBracket {
		XLog.Panic("XOrm.Cond: unexpected token after expression, expression: %v", parser.expr)
	}

	return cond
}

// paging 处理分页参数。
func (parser *exprParser) paging() bool {
	// 处理 limit
	if parser.current() != nil && parser.current().typ == exprTokenTypeLimit {
		parser.next()

		// 检查是否有等号，必须有等号
		if parser.current() != nil && parser.current().typ == exprTokenTypeAssign {
			parser.next()
		} else {
			XLog.Panic("XOrm.Cond: assignment operator (=) required after limit, expression: %v", parser.expr)
		}

		if parser.current() != nil && parser.current().typ == exprTokenTypeParam {
			paramIdx := parser.param(parser.current().value)
			parser.limit = paramIdx
			parser.next()
			return true
		} else {
			XLog.Panic("XOrm.Cond: parameter required after limit, expression: %v", parser.expr)
		}
	}

	// 处理 offset
	if parser.current() != nil && parser.current().typ == exprTokenTypeOffset {
		parser.next()

		// 检查是否有等号，必须有等号
		if parser.current() != nil && parser.current().typ == exprTokenTypeAssign {
			parser.next()
		} else {
			XLog.Panic("XOrm.Cond: assignment operator (=) required after offset, expression: %v", parser.expr)
		}

		if parser.current() != nil && parser.current().typ == exprTokenTypeParam {
			paramIdx := parser.param(parser.current().value)
			parser.offset = paramIdx
			parser.next()
			return true
		} else {
			XLog.Panic("XOrm.Cond: parameter required after offset, expression: %v", parser.expr)
		}
	}

	return false
}

// param 根据传入的索引字符串解析参数的索引。
func (parser *exprParser) param(index string) int {
	str := index[1 : len(index)-1] // 去掉花括号
	idx, err := strconv.Atoi(str)
	if err != nil {
		XLog.Panic("XOrm.Cond: invalid parameter index: %s, expression: %v", str, parser.expr)
	}
	if idx < 0 {
		XLog.Panic("XOrm.Cond: parameter index cannot be negative: %d, expression: %v", idx, parser.expr)
	}
	return idx
}

// exprCondition 解析表达式并返回条件实例、分页限定和分页偏移。
func exprCondition(expr string, params []any) (cond *orm.Condition, limit, offset int) {
	var parser *exprParser
	if tmp, _ := exprParserCache.Load(expr); tmp != nil {
		parser = tmp.(*exprParser)
	} else {
		tokens := exprTokenize(expr)
		parser = &exprParser{}
		parser.parse(expr, tokens)
		exprParserCache.Store(expr, parser)
	}

	cond = cloneCondition(parser.cond, expr, params)

	if parser.limit != -1 {
		if parser.limit >= len(params) || parser.limit < 0 {
			XLog.Panic("XOrm.Cond: parameter limit index is out of range: %d, expression: %v", parser.limit, parser.expr)
		} else {
			limit = params[parser.limit].(int)
		}
	}

	if parser.offset != -1 {
		if parser.offset >= len(params) || parser.offset < 0 {
			XLog.Panic("XOrm.Cond: parameter offset index is out of range: %d, expression: %v", parser.offset, parser.expr)
		} else {
			offset = params[parser.offset].(int)
		}
	}

	return cond, limit, offset
}

// exprTokenize 将表达式分解为词法单元。
func exprTokenize(expr string) (tokens []exprToken) {
	tokens = make([]exprToken, 0)

	// 预处理表达式，确保标识符前后有空格符。
	var result strings.Builder
	result.Grow(len(expr) * 2) // 预分配足够的空间
	for i := 0; i < len(expr); i++ {
		switch expr[i] {
		case '(':
			result.WriteString(" ( ")
		case ')':
			result.WriteString(" ) ")
		case '&':
			if i+1 < len(expr) && expr[i+1] == '&' {
				result.WriteString(" && ")
				i++
			} else {
				result.WriteByte(expr[i])
			}
		case '|':
			if i+1 < len(expr) && expr[i+1] == '|' {
				result.WriteString(" || ")
				i++
			} else {
				result.WriteByte(expr[i])
			}
		case '!':
			if i+1 < len(expr) && expr[i+1] == '=' {
				result.WriteString(" != ")
				i++
			} else {
				result.WriteString(" ! ")
			}
		case '>':
			if i+1 < len(expr) && expr[i+1] == '=' {
				result.WriteString(" >= ")
				i++
			} else {
				result.WriteString(" > ")
			}
		case '<':
			if i+1 < len(expr) && expr[i+1] == '=' {
				result.WriteString(" <= ")
				i++
			} else {
				result.WriteString(" < ")
			}
		case '=':
			if i+1 < len(expr) && expr[i+1] == '=' {
				result.WriteString(" == ")
				i++
			} else {
				result.WriteString(" = ")
			}
		default:
			result.WriteByte(expr[i])
		}
	}

	// 对表达式进行分词处理。
	expr = result.String()
	words := strings.Fields(expr)
	pos := 0

	for _, word := range words {
		switch word {
		case "(":
			tokens = append(tokens, exprToken{typ: exprTokenTypeLBracket, value: word, pos: pos})
		case ")":
			tokens = append(tokens, exprToken{typ: exprTokenTypeRBracket, value: word, pos: pos})
		case "&&":
			tokens = append(tokens, exprToken{typ: exprTokenTypeLogic, value: word, pos: pos})
		case "||":
			tokens = append(tokens, exprToken{typ: exprTokenTypeLogic, value: word, pos: pos})
		case "!":
			tokens = append(tokens, exprToken{typ: exprTokenTypeNot, value: word, pos: pos})
		case "limit":
			tokens = append(tokens, exprToken{typ: exprTokenTypeLimit, value: word, pos: pos})
		case "offset":
			tokens = append(tokens, exprToken{typ: exprTokenTypeOffset, value: word, pos: pos})
		case "=":
			tokens = append(tokens, exprToken{typ: exprTokenTypeAssign, value: word, pos: pos})
		default:
			if strings.HasPrefix(word, "{") && strings.HasSuffix(word, "}") {
				tokens = append(tokens, exprToken{typ: exprTokenTypeParam, value: word, pos: pos})
			} else if _, ok := condOpMap[word]; ok {
				tokens = append(tokens, exprToken{typ: exprTokenTypeOperator, value: word, pos: pos})
			} else {
				tokens = append(tokens, exprToken{typ: exprTokenTypeField, value: word, pos: pos})
			}
		}
		pos += len(word) + 1
	}

	return tokens
}

// cloneCondition 使用传入的参数对 orm.Condition 进行深拷贝。
// expr 是原始表达式。
// params 是新的参数列表。
// 返回一个新的 orm.Condition 实例。
func cloneCondition(cond *orm.Condition, expr string, params []any) *orm.Condition {
	if cond == nil {
		return nil
	}
	ncond := orm.NewCondition()
	rparams := getCondParams(cond)
	nparams := make([]beegoCondValue, len(rparams))
	for i, param := range rparams {
		// 复制基本字段
		nparams[i] = beegoCondValue{
			exprs:  make([]string, len(param.exprs)),
			isOr:   param.isOr,
			isNot:  param.isNot,
			isCond: param.isCond,
			isRaw:  param.isRaw,
			sql:    param.sql,
		}

		// 复制表达式
		copy(nparams[i].exprs, param.exprs)

		// 应用新参数
		if param.args != nil {
			pindex := param.args[0].(int)
			if pindex >= len(params) || pindex < 0 {
				XLog.Panic("XOrm.Cond: parameter %v index is out of range: %d, expression: %v", param.exprs[0], pindex, expr)
			}
			nparams[i].args = append(nparams[i].args, params[pindex])
		}

		// 递归克隆嵌套的条件
		if param.isCond && param.cond != nil {
			nparams[i].cond = cloneCondition(param.cond, expr, params)
		}
	}

	// 设置新条件的参数
	(*beegoCondition)(unsafe.Pointer(ncond)).params = nparams

	return ncond
}

// beegoCondition 结构体映射了 beego 中的条件结构。
type beegoCondition struct {
	params []beegoCondValue
}

// beegoCondValue 结构体映射了 beego 中的条件值结构。
type beegoCondValue struct {
	exprs  []string
	args   []any
	cond   *orm.Condition
	isOr   bool
	isNot  bool
	isCond bool
	isRaw  bool
	sql    string
}

// getCondParams 获取条件参数。
func getCondParams(cond *orm.Condition) []beegoCondValue {
	ncond := (*beegoCondition)(unsafe.Pointer(cond))
	return ncond.params
}
