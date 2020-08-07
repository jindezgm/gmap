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

// Logger define log interface.
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

// init create default logger.
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

// NewLogger create logger.
func NewLogger(path string) Logger {
	config := zap.NewProductionEncoderConfig()
	config.EncodeTime = zapcore.ISO8601TimeEncoder
	encoder := zapcore.NewConsoleEncoder(config)
	// Create file writer.
	file := zapcore.AddSync(&lumberjack.Logger{
		Filename:   path, // Log file path.
		MaxSize:    1024, // 1G
		MaxBackups: 7,    // 7 files.
		MaxAge:     7,    // 7 days
	})
	// Create stdio writer.
	stdout := zapcore.AddSync(os.Stdout)
	// Create logger core.
	core := zapcore.NewTee(
		zapcore.NewCore(encoder, file, zap.InfoLevel),
		zapcore.NewCore(encoder, stdout, zap.DebugLevel),
	)
	// Create logger.
	logger := zap.New(core, zap.AddCaller(), zap.AddStacktrace(zap.PanicLevel))
	return &zapLogger{SugaredLogger: logger.Sugar()}
}

// GetLogger get gmap logger. If don't implement logger can reuse gmap's logger,
// for example, examples program or test program, gmap uses zap to implement logger.
func GetLogger() Logger {
	return logger
}

// SetLogger set user logger.
func SetLogger(l Logger) {
	logger = l
}
