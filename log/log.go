//  Copyright (C) 2023 The db_relocate authors.
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License version 2 as
// published by the Free Software Foundation;
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT OF THIRD PARTY RIGHTS.

package log

import (
	"github.com/sirupsen/logrus"
)

var (
	l *logrus.Logger
)

func init() {
	l = logrus.New()

	l.SetFormatter(&logrus.JSONFormatter{})
	l.SetReportCaller(false)
}

func SetLogLevel(level *string) {
	logLevel, err := logrus.ParseLevel(*level)
	if err != nil {
		Fatalln("Unsupported log level!", *level)
	}
	l.SetLevel(logLevel)
}

func Infof(format string, v ...interface{}) {
	l.Infof(format, v...)
}

func Warnf(format string, v ...interface{}) {
	l.Warnf(format, v...)
}

func Errorf(format string, v ...interface{}) {
	l.Errorf(format, v...)
}

func Debugf(format string, v ...interface{}) {
	l.Debugf(format, v...)
}

func Fatalf(format string, v ...interface{}) {
	l.Fatalf(format, v...)
}

func Panicf(format string, v ...interface{}) {
	l.Panicf(format, v...)
}

func Tracef(format string, v ...interface{}) {
	l.Tracef(format, v...)
}

func Infoln(args ...interface{}) {
	l.Infoln(args...)
}

func Warnln(args ...interface{}) {
	l.Warnln(args...)
}

func Errorln(args ...interface{}) {
	l.Errorln(args...)
}

func Debugln(args ...interface{}) {
	l.Debugln(args...)
}

func Fatalln(args ...interface{}) {
	l.Fatalln(args...)
}

func Panicln(args ...interface{}) {
	l.Panicln(args...)
}

func Traceln(args ...interface{}) {
	l.Traceln(args...)
}
