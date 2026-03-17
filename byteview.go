package KakaCache

// ByteView 只读的字节视图，用于缓存数据
// 实现了 store 包下的 Value 接口，所以未来存储K-V的时候，使用 ByteView 作为缓存值
type ByteView struct {
	b []byte
}

// Len 返回字节视图字节数
func (b ByteView) Len() int {
	return len(b.b)
}

// ByteSLice 返回字节视图的字节切片
func (b ByteView) ByteSLice() []byte {
	return cloneBytes(b.b)
}

// String 获取字节视图的字符串
func (b ByteView) String() string {
	return string(b.b)
}

// cloneBytes 深复制字节视图的字节切片
func cloneBytes(b []byte) []byte {
	c := make([]byte, len(b))
	copy(c, b)
	return c
}
