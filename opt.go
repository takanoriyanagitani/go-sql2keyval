package sql2keyval

type Option[T any] interface {
	Value() T
	Empty() bool
	HasValue() bool
	ForEach(f func(T))
}

type optionEmpty[T any] struct{}

func (o optionEmpty[T]) Value() (t T)       { return }
func (o optionEmpty[T]) Empty() bool        { return true }
func (o optionEmpty[T]) HasValue() bool     { return false }
func (o optionEmpty[T]) ForEach(_ func(T))  {}
func OptionEmptyNew[T any]() optionEmpty[T] { return optionEmpty[T]{} }

type optionValue[T any] struct{ val T }

func (o optionValue[T]) Value() T           { return o.val }
func (o optionValue[T]) Empty() bool        { return false }
func (o optionValue[T]) HasValue() bool     { return true }
func (o optionValue[T]) ForEach(f func(T))  { f(o.Value()) }
func OptionNew[T any](val T) optionValue[T] { return optionValue[T]{val} }

func OptionMap[T, U any](o Option[T], f func(T) U) Option[U] {
	if o.HasValue() {
		t := o.Value()
		u := f(t)
		return OptionNew(u)
	}
	return OptionEmptyNew[U]()
}
