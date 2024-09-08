package logger

import (
	"go.uber.org/zap"
)

var (
	log *zap.SugaredLogger
)

func InitLogger() {
	logger, _ := zap.NewProduction()
	log = logger.Sugar()
}

func GetLogger() *zap.SugaredLogger {
	if log == nil {
		InitLogger()
	}
	return log
}
