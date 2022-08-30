package sql2keyval

type Iter[T any] func() Option[T]

func IterFromArray[T any](a []T) Iter[T] {
	l := len(a)
	i := 0
	return func() Option[T] {
		if i < l {
			o := OptionNew(a[i])
			i += 1
			return o
		}
		return OptionEmptyNew[T]()
	}
}

func IterEmptyNew[T any]() Iter[T] {
	return func() Option[T]{
		return OptionEmptyNew[T]()
	}
}

func IterFromOpt[T any](o Option[T]) Iter[T]{
	i := 0
	return func() Option[T]{
		if 0 == i {
			i += 1
			return o
		}
		return OptionEmptyNew[T]()
	}
}
