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
			if 0 != o.Value() {
				t.Errorf("Unexpected value: %v", o.Value())
			}
		})

		t.Run("Multi", func(t *testing.T) {
			t.Parallel()
			i := IterFromArray[int]([]int{6, 3, 4})

			o1 := i()
			if !o1.HasValue() {
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

		oi := IterFromOpt[string](OptionNew("hw"))
		t.Run("1st", func(t *testing.T) {
			o := oi()
			if o.Empty() {
				t.Errorf("Must have a value")
			}
			v := o.Value()
			if v != "hw" {
				t.Errorf("Unexpected value: %s", v)
			}
		})

		t.Run("2nd", func(t *testing.T) {
			o := oi()
			if o.HasValue() {
				t.Errorf("Must be empty")
			}
		})
	})

	t.Run("IterFromChan", func(t *testing.T) {
		t.Parallel()

		c := make(chan int, 2)
		var i Iter[int] = IterFromChan(c)

		go func() {
			c <- 634
			c <- 333
			c <- 42
			close(c)
		}()

		checker := func(got, expected int) {
			if got != expected {
				t.Errorf("Unexpected value got: %v", got)
			}
		}

		checker(i().Value(), 634)
		checker(i().Value(), 333)
		checker(i().Value(), 42)

		o := i()
		if o.HasValue() {
			t.Errorf("Must be empty")
		}
	})

	t.Run("IterFromChanNB", func(t *testing.T) {
		t.Parallel()

		c := make(chan int, 3)
		var i Iter[int] = IterFromChanNB(c)

		c <- 634
		c <- 333
		c <- 42

		checker := func(got, expected int) {
			if got != expected {
				t.Errorf("Unexpected value got: %v", got)
			}
		}

		checker(i().Value(), 634)
		checker(i().Value(), 333)
		checker(i().Value(), 42)

		o := i()
		if o.HasValue() {
			t.Errorf("Must be empty")
		}
	})

}
