package sql2keyval

func Bool2error(ok bool, egen func() error) error {
	if ok {
		return nil
	}
	return egen()
}

func Compose[T, U, V any](f func(T) U, g func(U) V) func(T) V { return compose(f, g) }
