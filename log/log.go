package log

import (
	"io"
	"log"
	"os"
)

var (
	info  *log.Logger
	debug *log.Logger
	erro  *log.Logger
)

func init() {
	errFile, err := os.OpenFile("errors.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal(err)
	}
	info = log.New(os.Stdout, "Info:", log.Ldate|log.Ltime|log.Lshortfile)
	debug = log.New(os.Stdout, "Debug:", log.Ldate|log.Ltime|log.Lshortfile)
	erro = log.New(io.MultiWriter(os.Stderr, errFile), "Error:", log.Ldate|log.Ltime|log.Lshortfile)
}

func Info(format string, args ...any) {
	info.Printf(format, args...)
}

func Debug(format string, args ...any) {
	debug.Printf(format, args...)
}

func Error(format string, args ...any) {
	erro.Printf(format, args...)
}

func Fatal(format string, args ...any) {
	erro.Fatalf(format, args...)
}
