package beeq

func Alloc(l int) []byte {
	return make([]byte, l)
}

func ReAlloc(buf []byte, l int) []byte {
	b := make([]byte, l)
	copy(b, buf)
	return b
}
