package sql2keyval

type Option[T any] interface {
	Value() T
	Empty() bool
	HasValue() bool
	ForEach(f func(T))
	Filter(f func(T) bool) Option[T]
}

type optionEmpty[T any] struct{}

func (o optionEmpty[T]) Value() (t T)                    { return }
func (o optionEmpty[T]) Empty() bool                     { return true }
func (o optionEmpty[T]) HasValue() bool                  { return false }
func (o optionEmpty[T]) ForEach(_ func(T))               {}
func (o optionEmpty[T]) Filter(_ func(T) bool) Option[T] { return o }
func OptionEmptyNew[T any]() optionEmpty[T]              { return optionEmpty[T]{} }

type optionValue[T any] struct{ val T }

func OptionNew[T any](val T) optionValue[T] { return optionValue[T]{val} }

func (o optionValue[T]) Value() T          { return o.val }
func (o optionValue[T]) Empty() bool       { return false }
func (o optionValue[T]) HasValue() bool    { return true }
func (o optionValue[T]) ForEach(f func(T)) { f(o.Value()) }
func (o optionValue[T]) Filter(f func(T) bool) Option[T] {
	if f(o.Value()) {
		return o
	}
	return OptionEmptyNew[T]()
}

func OptionMap[T, U any](o Option[T], f func(T) U) Option[U] {
	if o.HasValue() {
		t := o.Value()
		u := f(t)
		return OptionNew(u)
	}
	return OptionEmptyNew[U]()
}
