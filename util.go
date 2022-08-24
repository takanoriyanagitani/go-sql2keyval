package sql2keyval

func Bool2error(ok bool, egen func() error) error {
	if ok {
		return nil
	}
	return egen()
}
