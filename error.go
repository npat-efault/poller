package poller

// Error is the type for the errors returned by poller functions and
// methods. See also the ErrXXX constants.
type Error int

// Errors returned by poller functions and methods. In addition to
// these, poller functions and methods may return the errors reported
// by the underlying system calls (open(2), read(2), write(2), etc.),
// as well as io.EOF and io.ErrUnexpectedEOF.
const (
	ErrClosed  Error = 1 // Use of closed poller file-descriptor
	ErrTimeout Error = 2 // Operation timed-out
)

// Error returns a string describing the error.
func (e Error) Error() string {
	switch e {
	case ErrClosed:
		return "use of closed descriptor"
	case ErrTimeout:
		return "I/O timeout error"
	}
	return "unknown error"
}

// Timeout returns true if the error indicates a timeout condition.
func (e Error) Timeout() bool {
	return e == ErrTimeout
}

// Temporary returns true if the error indicates a temporary condition
// (re-atempting the operation may succeed).
func (e Error) Temporary() bool {
	return e.Timeout()
}
