package utils

import (
	"database/sql"
	"internaltimeseries-migration/config"
	"internaltimeseries-migration/logger"
	"math"
	"runtime"
	"strconv"
	"strings"
	"time"
)

func FormatDate(PST *time.Location, nullTime NullTime, resolution string, logFileLogger *logger.Logger) (string, error) {
	//Format the dates to ISO 8601 (RFC-3339)
	var dateFormatted string
	if nullTime.Valid {
		if resolution == "60" {
			dateFormatted = nullTime.Time.Add(-1 * time.Hour).Format(config.GetJSONDateLayoutLong())
		} else if resolution == "15" {
			dateFormatted = nullTime.Time.Add(-15 * time.Minute).Format(config.GetJSONDateLayoutLong())
		} else {
			dateFormatted = nullTime.Time.Format(config.GetJSONDateLayoutLong())
		}
		t, err := time.ParseInLocation(config.GetJSONDateLayoutLong(), dateFormatted, PST)
		if err != nil {
			(*logFileLogger).Error(err)
			return "", err
		}
		dateFormatted = t.UTC().Format(config.GetJSONDateLayoutLong())
	} else {
		dateFormatted = ""
	}
	return dateFormatted, nil
}

func FormatDatePointer(PST *time.Location, nullTime NullTime, logFileLogger *logger.Logger) (*string, error) {
	//Format the dates to ISO 8601 (RFC-3339)
	var dateFormatted string
	if nullTime.Valid {
		dateFormatted = nullTime.Time.Format(config.GetJSONDateLayoutLong())
		t, err := time.ParseInLocation(config.GetJSONDateLayoutLong(), dateFormatted, PST)
		if err != nil {
			(*logFileLogger).Error(err)
			return nil, err
		}
		dateFormatted = t.UTC().Format(config.GetJSONDateLayoutLong())
		return &dateFormatted, nil
	} else {
		return nil, nil
	}
}

type NullTime struct {
	sql.NullTime
}

type NullBool struct {
	sql.NullBool
}

type NullString struct {
	sql.NullString
}

type NullFloat struct {
	sql.NullFloat64
}

func PrintMemUsage(logFileLogger *logger.Logger) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	(*logFileLogger).Info("Memory usage: " +
		"Alloc = " + strconv.Itoa(int(m.Alloc/1024/1024)) + " MiB, " +
		"TotalAlloc = " + strconv.Itoa(int(m.TotalAlloc/1024/1024)) + " MiB, " +
		"Sys = " + strconv.Itoa(int(m.Sys/1024/1024)) + " MiB, " +
		"NumGC = " + strconv.Itoa(int(m.NumGC)))
}

func ConvertToTwoDArray(oneDArray []string, columns int) [][]string {
	// Calculate the number of rows
	rows := int(math.Ceil(float64(len(oneDArray)) / float64(columns)))

	// Initialize the 2D slice
	twoDArray := make([][]string, rows)
	for i := 0; i < rows; i++ {
		twoDArray[i] = make([]string, columns)
	}

	// Fill the 2D slice
	for i, val := range oneDArray {
		row := i / columns
		col := i % columns
		twoDArray[row][col] = val
	}

	return twoDArray
}

func GetInternalValueQualifier(valueQualifier string) string {
	if valueQualifier == "" {
		return ""
	} else if strings.EqualFold(valueQualifier, "E01") {
		return "M"
	} else if strings.EqualFold(valueQualifier, "56") {
		return "E"
	} else if strings.EqualFold(valueQualifier, "36") {
		return "C"
	} else if strings.EqualFold(valueQualifier, "D01") {
		return "B"
	} else if strings.EqualFold(valueQualifier, "QM") {
		return "?"
	} else {
		return valueQualifier
	}
}
