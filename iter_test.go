package sql2keyval

import (
	"testing"
)

func TestAll(t *testing.T) {

	t.Parallel()

	t.Run("IterFromArray", func(t *testing.T) {
		t.Parallel()

		t.Run("Empty", func(t *testing.T) {
			t.Parallel()
			i := IterFromArray[int](nil)
			o := i()
			if o.HasValue() {
				t.Errorf("Must be empty")
			}
			if 0 != o.Value(){
				t.Errorf("Unexpected value: %v", o.Value())
			}
		})

		t.Run("Multi", func(t *testing.T) {
			t.Parallel()
			i := IterFromArray[int]([]int{6, 3, 4})

			o1 := i()
			if ! o1.HasValue(){
				t.Errorf("Must have a value")
			}

			i1 := o1.Value()
			i2 := i().Value()
			i3 := i().Value()
			o := i()
			if o.HasValue() {
				t.Errorf("Must be empty")
			}
			j := i1*100 + i2*10 + i3*1
			if 634 != j {
				t.Errorf("Unexpected value: %v", j)
			}
		})
	})

	t.Run("IterEmptyNew", func(t *testing.T) {
		t.Parallel()
		ei := IterEmptyNew[float64]()
		f := ei()
		if f.HasValue() {
			t.Errorf("Must be empty")
		}
	})

	t.Run("IterFromOpt", func(t *testing.T) {
		t.Parallel()

		t.Run("1st", func(t *testing.T) {
			oi := IterFromOpt[string](OptionNew("hw"))
			o := oi()
			if o.Empty() {
				t.Errorf("Must have a value")
			}
			v := o.Value()
			if v != "hw" {
				t.Errorf("Unexpected value: %s", v)
			}
		})
	})

}
