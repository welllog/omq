package omq

import "log"

type Logger interface {
	Debugf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Warnf(format string, args ...interface{})
}

type DefLogger struct{}

func (d DefLogger) Debugf(format string, args ...interface{}) {
	log.Printf(format, args...)
}

func (d DefLogger) Infof(format string, args ...interface{}) {
	log.Printf(format, args...)
}

func (d DefLogger) Warnf(format string, args ...interface{}) {
	log.Printf(format, args...)
}
