package sql2keyval

import (
	"context"
	"testing"
)

func TestRegisterQueryGenerator(t *testing.T) {
	t.Parallel()

	t.Run("invalid generator", func(t *testing.T) {
		t.Parallel()
		RegisterQueryGenerator("invalid", nil)
	})

	t.Run("dummy generator", func(t *testing.T) {
		t.Parallel()
		RegisterQueryGenerator("dummy", &emptyQueryGenerator{})
	})

	t.Run("dup generator", func(t *testing.T) {
		t.Parallel()
		RegisterQueryGenerator("dummy", &emptyQueryGenerator{})
	})

	t.Run("use invalid generator", func(t *testing.T) {
		t.Parallel()
		egen := getQueryGeneratorOrEmpty("does-not-exist")

		_, e := egen.Get("")
		if nil == e {
			t.Errorf("Must fail")
		}

		pat := []struct {
			f func(s string) (string, error)
			n string
		}{
			{f: egen.Get, n: "Get"},
			{f: egen.Del, n: "Del"},
			{f: egen.Add, n: "Add"},
			{f: egen.Set, n: "Set"},
			{f: egen.DelBucket, n: "DelBucket"},
			{f: egen.AddBucket, n: "AddBucket"},
		}

		for _, p := range pat {
			t.Run(p.n, func(t *testing.T) {
				t.Parallel()
				_, e := p.f("")
				if nil == e {
					t.Errorf("Must fail")
				}
			})
		}
	})
}

func TestGetFactory(t *testing.T) {
	t.Parallel()

	var dummyFactory func(Query) Get = GetFactory("does-not-exist")
	var getter Get = dummyFactory(nil)
	_, e := getter(context.Background(), "", nil)
	if nil == e {
		t.Errorf("Must fail")
	}
}

func TestAddFactory(t *testing.T) {
	t.Parallel()

	var dummyFactory func(Exec) Add = AddFactory("does-not-exist")
	var adder Add = dummyFactory(nil)
	e := adder(context.Background(), "", nil, nil)
	if nil == e {
		t.Errorf("Must fail")
	}
}

func TestSetFactory(t *testing.T) {
	t.Parallel()

	var dummyFactory func(Exec) Set = SetFactory("does-not-exist")
	var setter Set = dummyFactory(nil)
	e := setter(context.Background(), "", nil, nil)
	if nil == e {
		t.Errorf("Must fail")
	}
}

func TestDelFactory(t *testing.T) {
	t.Parallel()

	var dummyFactory func(Exec) Del = DelFactory("does-not-exist")
	var remover Del = dummyFactory(nil)
	e := remover(context.Background(), "", nil)
	if nil == e {
		t.Errorf("Must fail")
	}
}

func TestDelBucketFactory(t *testing.T) {
	t.Parallel()

	var dummyFactory func(Exec) DelBucket = DelBucketFactory("does-not-exist")
	var remover DelBucket = dummyFactory(nil)
	e := remover(context.Background(), "")
	if nil == e {
		t.Errorf("Must fail")
	}
}

func TestAddBucketFactory(t *testing.T) {
	t.Parallel()

	var dummyFactory func(Exec) AddBucket = AddBucketFactory("does-not-exist")
	var adder AddBucket = dummyFactory(nil)
	e := adder(context.Background(), "")
	if nil == e {
		t.Errorf("Must fail")
	}
}
