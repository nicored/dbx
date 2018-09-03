package dbx

type MissingParamErr struct {
	error string
}

func (e *MissingParamErr) Error() string {
	return e.error
}

type WrongTypeErr struct {
	error string
}

func (e *WrongTypeErr) Error() string {
	return e.error
}

type NilPointerErr struct {
	error string
}

func (e *NilPointerErr) Error() string {
	return e.error
}

type EmptySliceErr struct {
	error string
}

func (e *EmptySliceErr) Error() string {
	return e.error
}
