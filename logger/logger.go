package logger

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"internaltimeseries-migration/config"
	"os"
	"sync"
	"time"
)

var (
	mutexLogging sync.Mutex
	lineCounter  = 0
)

type Impl struct {
	LogFile        *os.File
	MaxLogfileSize int64
}

type Logger interface {
	Fatal(err error)
	Error(logMessage error)
	ErrorWithText(logMessage string)
	Info(logMessage string)
	Debug(logMessage string)
	replaceLogFile() error
	logFileIsTooLarge() bool

	Close()
}

var NewLogger = func(maxLogfileSize int64) (Logger, error) {
	logFile, err := os.OpenFile(config.GetLogFileName(), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	log.SetFormatter(&log.TextFormatter{QuoteEmptyFields: true, ForceColors: true, FullTimestamp: true})
	log.SetReportCaller(true)
	log.SetOutput(logFile)
	log.SetLevel(log.InfoLevel)
	if err != nil {
		// Cannot open log file. Logging to stderr
		fmt.Println(err)
	} else {
		log.SetOutput(logFile)
	}

	return &Impl{
		LogFile:        logFile,
		MaxLogfileSize: maxLogfileSize,
	}, err
}

func (i *Impl) ErrorWithText(logMessage string) {
	mutexLogging.Lock()
	defer mutexLogging.Unlock()

	lineCounter++

	log.Error(logMessage)
	if i.logFileIsTooLarge() {
		err := i.replaceLogFile()
		if err != nil {
			log.Error(err)
			return
		}
	}
}

func (i *Impl) Error(err error) {
	mutexLogging.Lock()
	defer mutexLogging.Unlock()

	lineCounter++

	log.Error(err)
	if i.logFileIsTooLarge() {
		err = i.replaceLogFile()
		if err != nil {
			log.Error(err)
			return
		}
	}
}

func (i *Impl) Info(logMessage string) {
	mutexLogging.Lock()
	defer mutexLogging.Unlock()

	lineCounter++

	log.Info(logMessage)
	if i.logFileIsTooLarge() {
		err := i.replaceLogFile()
		if err != nil {
			log.Error(err)
			return
		}
	}
}

func (i *Impl) Debug(logMessage string) {
	mutexLogging.Lock()
	defer mutexLogging.Unlock()

	lineCounter++

	log.Debug(logMessage)
	if i.logFileIsTooLarge() {
		err := i.replaceLogFile()
		if err != nil {
			log.Error(err)
			return
		}
	}
}

func (i *Impl) Fatal(err error) {
	mutexLogging.Lock()
	defer mutexLogging.Unlock()

	lineCounter++

	log.Fatal(err)
	if i.logFileIsTooLarge() {
		err := i.replaceLogFile()
		if err != nil {
			log.Error(err)
			return
		}
	}
}

func (i *Impl) replaceLogFile() error {

	log.Info("Archiving existing log file")

	// Replace the log file
	err := i.LogFile.Close()
	if err != nil {
		return err
	}
	newFileName := config.GetLogFileNameWithoutExtension() + "_" + time.Now().Format(config.GetFileDateLayout()) + "." + config.GetLogFileExtension()
	err = os.Rename(i.LogFile.Name(), newFileName)
	if err != nil {
		i.LogFile, err = os.OpenFile(config.GetLogFileName(), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
		return err
	}
	// Create a new file
	i.LogFile, err = os.OpenFile(config.GetLogFileName(), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	log.SetFormatter(&log.TextFormatter{QuoteEmptyFields: true, ForceColors: true, FullTimestamp: true})
	log.SetReportCaller(true)
	log.SetOutput(i.LogFile)
	if err != nil {
		return err
	}
	return nil
}

func (i *Impl) logFileIsTooLarge() bool {
	if lineCounter < 100 {
		return false
	} else {
		lineCounter = 0

		fileInfo, err := os.Stat(i.LogFile.Name())
		if err != nil {
			log.Error("Error:", err)
			return false
		}
		fileSize := fileInfo.Size()
		return fileSize/(1024*1024) >= i.MaxLogfileSize
	}
}

func (i *Impl) Close() {
	//Don't forget to close the log file
	i.LogFile.Close()
}
