package ptr

func V[T any](v T) *T {
	return &v
}
