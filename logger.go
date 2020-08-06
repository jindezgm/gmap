/*
 * @Author: jinde.zgm
 * @Date: 2020-07-23 20:59:19
 * @Descripttion:
 */
package gmap

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/natefinch/lumberjack"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type Logger interface {
	Debug(v ...interface{})
	Debugf(format string, v ...interface{})

	Error(v ...interface{})
	Errorf(format string, v ...interface{})

	Info(v ...interface{})
	Infof(format string, v ...interface{})

	Warning(v ...interface{})
	Warningf(format string, v ...interface{})

	Fatal(v ...interface{})
	Fatalf(format string, v ...interface{})

	Panic(v ...interface{})
	Panicf(format string, v ...interface{})
}

var logger Logger

func init() {
	dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if nil != err {
		panic(err)
	}

	logger = NewLogger(fmt.Sprintf("%s/logs/%s.log", dir, filepath.Base(os.Args[0])))
}

type zapLogger struct {
	*zap.SugaredLogger
}

func (l *zapLogger) Warning(v ...interface{}) {
	l.Warn(v)
}

func (l *zapLogger) Warningf(format string, v ...interface{}) {
	l.Warnf(format, v)
}

func NewLogger(path string) Logger {
	config := zap.NewProductionEncoderConfig()
	config.EncodeTime = zapcore.ISO8601TimeEncoder
	encoder := zapcore.NewConsoleEncoder(config)

	file := zapcore.AddSync(&lumberjack.Logger{
		Filename:   path,
		MaxSize:    1024,
		MaxBackups: 7,
		MaxAge:     7,
	})

	stdout := zapcore.AddSync(os.Stdout)

	core := zapcore.NewTee(
		zapcore.NewCore(encoder, file, zap.InfoLevel),
		zapcore.NewCore(encoder, stdout, zap.DebugLevel),
	)

	logger := zap.New(core, zap.AddCaller(), zap.AddStacktrace(zap.PanicLevel))

	return &zapLogger{SugaredLogger: logger.Sugar()}
}

//
func GetLogger() Logger {
	return logger
}

//
func SetLogger(l Logger) {
	logger = l
}
