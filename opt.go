package sql2keyval

type Option[T any] interface {
	Value() T
	Empty() bool
	HasValue() bool
}

type optionEmpty[T any] struct{}

func (o optionEmpty[T]) Value() (t T)       { return }
func (o optionEmpty[T]) Empty() bool        { return true }
func (o optionEmpty[T]) HasValue() bool     { return optionHasValue[T](o) }
func optionEmptyNew[T any]() optionEmpty[T] { return optionEmpty[T]{} }

type optionValue[T any] struct{ val T }

func (o optionValue[T]) Value() T           { return o.val }
func (o optionValue[T]) Empty() bool        { return false }
func (o optionValue[T]) HasValue() bool     { return optionHasValue[T](o) }
func optionNew[T any](val T) optionValue[T] { return optionValue[T]{val} }

func optionHasValue[T any](o Option[T]) bool { return !o.Empty() }
