package log

import (
    "os"
    "github.com/go-kit/kit/log"
    "github.com/go-kit/kit/log/level"
)

var (
    logger log.Logger
)

func Init(logLevel string) {
    logger = log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
    switch logLevel {
    case "info":
        logger = level.NewFilter(logger, level.AllowInfo())
    case "warn":
        logger = level.NewFilter(logger, level.AllowWarn())
    case "error":
        logger = level.NewFilter(logger, level.AllowError())
    case "debug":
    case "all":
    default:
        logger = level.NewFilter(logger, level.AllowAll())
    }
    logger = log.With(logger, "ts", log.DefaultTimestampUTC)
}

func Debug(keyvals ...interface{}) {
    level.Debug(logger).Log(keyvals...)
}
func Info(keyvals ...interface{}) {
    level.Info(logger).Log(keyvals...)
}
func Warn(keyvals ...interface{}) {
    level.Warn(logger).Log(keyvals...)
}
func Error(keyvals ...interface{}) {
    level.Error(logger).Log(keyvals...)
}
