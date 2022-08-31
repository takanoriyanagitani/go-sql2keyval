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
	return func() Option[T] {
		return OptionEmptyNew[T]()
	}
}

func IterFromOpt[T any](o Option[T]) Iter[T] {
	i := 0
	return func() Option[T] {
		if 0 == i {
			i += 1
			return o
		}
		return OptionEmptyNew[T]()
	}
}

func IterFromChan[T any](c <-chan T) Iter[T] {
	return func() Option[T] {
		t, ok := <-c
		if ok {
			return OptionNew(t)
		}
		return OptionEmptyNew[T]()
	}
}

func IterFromChanNB[T any](c <-chan T) Iter[T] {
	return func() Option[T] {
		select {
		case t := <-c:
			return OptionNew(t)
		default:
			return OptionEmptyNew[T]()
		}
	}
}

func IterMap[T, U any](i Iter[T], f func(T) U) Iter[U] {
	return func() Option[U] {
		oi := i()
		return OptionMap(oi, f)
	}
}

func (i Iter[T]) ToArray() (a []T) {
	for o := i(); o.HasValue(); o = i() {
		a = append(a, o.Value())
	}
	return a
}

func IterInts(lbi int, ube int) Iter[int] {
	i := lbi
	return func() Option[int] {
		if i < ube {
			o := OptionNew(i)
			i += 1
			return o
		}
		return OptionEmptyNew[int]()
	}
}
