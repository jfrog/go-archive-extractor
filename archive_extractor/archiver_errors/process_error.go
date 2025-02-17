package archiver_errors

import "fmt"

type ProcessError struct {
	msg string
	err error
}

func NewProcessError(msg string, err error) *ProcessError {
	return &ProcessError{err: err, msg: msg}
}

func (pe ProcessError) Error() string {
	return fmt.Sprintf("Failed to process file file:%s err:%s", pe.msg, pe.err.Error())
}

func (pe ProcessError) Unwrap() error {
	return pe.err
}
