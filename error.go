package poller

type Error int

const (
	ErrClosed  Error = 1
	ErrTimeout Error = 2
)

func (e Error) Error() string {
	switch e {
	case ErrClosed:
		return "use of closed descriptor"
	case ErrTimeout:
		return "I/O timeout error"
	}
	return "unknown error"
}

func (e Error) Timeout() bool {
	return e == ErrTimeout
}

func (e Error) Temporary() bool {
	return e.Timeout()
}
