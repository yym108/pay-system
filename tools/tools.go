// Package tools 保留必须出现在 go.mod 里、但还没有业务代码引用的依赖。
// sonic 由 BASE-01 / D8 锁定：不显式引用时，后续 hertz 会拉回不兼容 Go 1.27 的旧版本。
package tools

import _ "github.com/bytedance/sonic"
