package utils

import (
	"database/sql"
	log "github.com/sirupsen/logrus"
	"time"
	"timeseries-migration/config"
)

func FormatDate(PST *time.Location, nullTime NullTime, resolution string) (string, error) {
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
			log.Error(err)
			return "", err
		}
		dateFormatted = t.UTC().Format(config.GetJSONDateLayoutLong())
	} else {
		dateFormatted = ""
	}
	return dateFormatted, nil
}

func FormatDatePointer(PST *time.Location, nullTime NullTime) (*string, error) {
	//Format the dates to ISO 8601 (RFC-3339)
	var dateFormatted string
	if nullTime.Valid {
		dateFormatted = nullTime.Time.Format(config.GetJSONDateLayoutLong())
		t, err := time.ParseInLocation(config.GetJSONDateLayoutLong(), dateFormatted, PST)
		if err != nil {
			log.Error(err)
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

func bool2intstring(b bool) string {
	if b {
		return "1"
	}
	return "0"
}

