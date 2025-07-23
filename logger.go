package dbinitiator

import "fmt"

type logger struct {
	verbose bool
}

func (l *logger) Printf(format string, v ...any) {
	fmt.Printf(format, v...)
}

func (l *logger) Verbose() bool {
	return l.verbose
}
