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

func IterFlat2Chan[T any](i Iter[Iter[T]], c chan<- T, lmt int) {
	j := 0
	for oi := i(); j < lmt && oi.HasValue(); oi = i() {
		var si Iter[T] = oi.Value()
		for o := si(); j < lmt && o.HasValue(); o = si() {
			var t T = o.Value()
			c <- t
			j += 1
		}
	}
}

func (i Iter[T]) IntoInspect(f func(T)) Iter[T] {
	return func() Option[T] {
		var o Option[T] = i()
		o.ForEach(f)
		return o
	}
}

func (i Iter[T]) Count() uint64 {
	var u uint64 = 0
	for o := i(); o.HasValue(); o = i() {
		u += 1
	}
	return u
}

func (iter Iter[T]) Take(imax int) Iter[T] {
	i := 0
	return func() Option[T] {
		return iter().Filter(func(_ T) bool {
			ok := i < imax
			i += 1
			return ok
		})
	}
}

func IterReduce[T, U any](i Iter[T], init U, reducer func(state U, item T) U) U {
	state := init
	for o := i(); o.HasValue(); o = i() {
		state = reducer(state, o.Value())
	}
	return state
}
