package sql2keyval

type Iter[T any] func() Option[T]

func IterFromArray[T any](a []T) Iter[T] {
	l := len(a)
	i := 0
	return func() Option[T] {
		if i < l {
			o := optionNew(a[i])
			i += 1
			return o
		}
		return optionEmptyNew[T]()
	}
}
