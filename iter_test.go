package sql2keyval

import (
	"testing"
)

func checker[T comparable](t *testing.T, got T, expected T) {
	if got != expected {
		t.Errorf("Unexpected value got.\n")
		t.Errorf("expected: %v\n", expected)
		t.Errorf("got: %v\n", got)
	}
}

func TestIterAll(t *testing.T) {

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

	t.Run("iter", func(t *testing.T) {
		t.Parallel()

		t.Run("IterMap", func(t *testing.T) {
			integers := IterInts(0, 3)
			mapd := IterMap(integers, func(_ int) string {
				return "7"
			})
			var sarr []string = mapd.ToArray()
			if 3 != len(sarr) {
				t.Errorf("Unexpected len: %v", len(sarr))
			}

			check := func(expected, got string) {
				if expected != got {
					t.Errorf("Unexpected value got.")
				}
			}

			check(sarr[0], "7")
			check(sarr[1], "7")
			check(sarr[2], "7")
		})
	})

	t.Run("IterFlatChan", func(t *testing.T) {
		t.Parallel()

		t.Run("empty", func(t *testing.T) {
			t.Parallel()
			ei := IterFromArray[Iter[int]](nil)
			c := make(chan int, 128)
			IterFlat2Chan(ei, c, 16)
			ci := IterFromChanNB(c)
			if ci().HasValue() {
				t.Errorf("Must be empty")
			}
		})

		t.Run("single empty iter", func(t *testing.T) {
			t.Parallel()
			ei := IterFromArray([]Iter[int]{})
			c := make(chan int, 128)
			IterFlat2Chan(ei, c, 16)
			ci := IterFromChanNB(c)
			if ci().HasValue() {
				t.Errorf("Must be empty")
			}
		})

		t.Run("single non empty iter", func(t *testing.T) {
			t.Parallel()
			ei := IterFromArray([]Iter[int]{
				IterFromArray([]int{6, 3, 4}),
			})
			c := make(chan int, 128)
			IterFlat2Chan(ei, c, 16)
			ci := IterFromChanNB(c)

			checker(t, 6, ci().Value())
			checker(t, 3, ci().Value())
			checker(t, 4, ci().Value())

			if ci().HasValue() {
				t.Errorf("Must be empty")
			}
		})

		t.Run("multi non empty iter", func(t *testing.T) {
			t.Parallel()
			ei := IterFromArray([]Iter[int]{
				IterFromArray([]int{6, 3, 4}),
				IterFromArray([]int{3, 3, 3}),
			})
			c := make(chan int, 128)
			IterFlat2Chan(ei, c, 16)
			ci := IterFromChanNB(c)

			checker(t, 6, ci().Value())
			checker(t, 3, ci().Value())
			checker(t, 4, ci().Value())
			checker(t, 3, ci().Value())
			checker(t, 3, ci().Value())
			checker(t, 3, ci().Value())

			if ci().HasValue() {
				t.Errorf("Must be empty")
			}
		})

		t.Run("too many items", func(t *testing.T) {
			t.Parallel()
			ei := IterFromArray([]Iter[int]{
				IterFromArray([]int{6, 3, 4}),
				IterFromArray([]int{3, 3, 3}),
			})
			c := make(chan int, 128)
			IterFlat2Chan(ei, c, 3)
			ci := IterFromChanNB(c)

			checker(t, 6, ci().Value())
			checker(t, 3, ci().Value())
			checker(t, 4, ci().Value())

			if !ci().Empty() {
				t.Errorf("Must be empty")
			}
		})
	})

	t.Run("Inspect", func(t *testing.T) {
		t.Parallel()

		t.Run("empty", func(t *testing.T) {
			ei := IterFromArray[int](nil)
			ins := ei.IntoInspect(func(_ int) {
				t.Errorf("Do not run")
			})

			if 0 != ins.Count() {
				t.Errorf("Must be empty")
			}
		})

		t.Run("non empty", func(t *testing.T) {
			ei := IterFromArray[int]([]int{3, 7, 7, 6})
			var sum int = 0
			ins := ei.IntoInspect(func(i int) {
				sum += i
			})

			if 4 != ins.Count() {
				t.Errorf("Must be empty")
			}

			checker(t, 23, sum)
		})
	})

	t.Run("Take", func(t *testing.T) {
		t.Parallel()

		t.Run("empty", func(t *testing.T) {
			var i Iter[int] = IterEmptyNew[int]()
			var k Iter[int] = i.Take(128)
			if k().HasValue() {
				t.Errorf("Must be empty")
			}
		})

		t.Run("single", func(t *testing.T) {
			var i Iter[int] = IterFromArray([]int{
				6, 3, 4,
			})
			var k Iter[int] = i.Take(1)
			var o Option[int] = k()
			checker(t, o.Value(), 6)

			if k().HasValue() {
				t.Errorf("Must be empty")
			}
		})
	})

	t.Run("IterReduce", func(t *testing.T) {
		t.Parallel()

		t.Run("empty", func(t *testing.T) {
			var i Iter[int] = IterEmptyNew[int]()
			var sum int = IterReduce(i, 42, func(state, item int) int { return state + item })
			checker(t, sum, 42)
		})

		t.Run("count odd", func(t *testing.T) {
			var i Iter[uint8] = IterFromArray([]uint8{
				0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
			})
			var cnt uint64 = IterReduce(i, 0, func(tot uint64, item uint8) uint64 {
				m := item & 0x01
				if 0 < m {
					return 1 + tot
				}
				return tot
			})
			checker(t, cnt, 5)
		})
	})

}
