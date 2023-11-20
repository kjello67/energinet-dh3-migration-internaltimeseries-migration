package config

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/tkanos/gonfig"
)

type Configuration struct {
	DB_USERNAME    string
	DB_PASSWORD    string
	DB_PORT        string
	DB_ALIAS       string
	DB_SID         string
	DB_HOST        string
	LOG_USERNAME   string
	LOG_PASSWORD   string
	LOG_PORT       string
	LOG_ALIAS      string
	LOG_SID        string
	LOG_HOST       string
	DEBUG_LOGGING  bool
	SKIP_DB_UPDATE bool
	FILE_LOCATION  string
	SQL_ITEM_COUNT string
	SQL_ITEM_ID    string
	FROM_DATE      string
	TO_DATE        string
	RENAME_BULK    int
}

func GetConfig(params ...string) Configuration {
	configuration := Configuration{}
	env := ""
	if len(params) > 0 {
		env = params[0]
	}
	fileName := fmt.Sprintf("./%s_ts_config.json", env)

	gonfig.GetConf(fileName, &configuration)

	log.Info("Using configurations in config file with prefix: ", env)

	return configuration
}

func GetConfigDB(params ...string) Configuration {
	configuration := Configuration{}
	env := ""
	if len(params) > 0 {
		env = params[0]
	}
	fileName := fmt.Sprintf("./%s_config_db.json", env)
	gonfig.GetConf(fileName, &configuration)

	log.Info("Using DB configurations for environment:  ", env)

	return configuration
}

//GetJSONDateLayoutLong returns the date layout to be used in the file
func GetJSONDateLayoutLong() string {
	return "2006-01-02T15:04:05"
}

//GetJSONDateLayoutShort returns the date layout to be used in the file
func GetJSONDateLayoutShort() string {
	return "2006-01-02"
}

//GetExportDateLayout returns the date layout to be used in the file
func GetExportDateLayout() string {
	return "02.01.2006 15:04:05"
}

//GetFileDateLayout returns the date layout to be used in the file
func GetFileDateLayout() string {
	return "20060102150405"
}

//GetFilenameDelimiter returns the delimiter to be used in the filename
func GetFilenameDelimiter() string {
	return "_"
}

//GetTSValueDelimiter returns the delimiter to be used in the filename
func GetTSValueDelimiter() string {
	return ";"
}

//GetFilenamePrefix returns the prefix to be used for time series in the filename
func GetFilenamePrefix() string {
	return "TS"
}

//GetTimeLocation returns the location used for timestamps
func GetTimeLocation() string {
	return "Europe/Copenhagen"
}

//GetTmpExtension returns the temporary extension of the filename
func GetTmpExtension() string {
	return ".tmp"
}

//GetFinalExtension returns the final extension of the filename
func GetFinalExtension() string {
	return ".json"
}

//GetExportTableName returns the name of the table where the scheduled runs and the final status should be stored
func GetExportTableName() string {
	return "DMDH3_OWN.DATAMIGRATION_EXPORT"
}

//GetExportProgressTableName returns the name of the table where the detailed progress of the export should be stored
func GetExportProgressTableName() string {
	return "DMDH3_OWN.DATAMIGRATION_EXPORT_PROGRESS"
}

//GetStatusNew returns the string used in the DB for new
func GetStatusNew() string {
	return "NEW"
}

//GetStatusRunning returns the string used in the DB for running
func GetStatusRunning() string {
	return "RUN"
}

//GetStatusFinished returns the string used in the DB for finished
func GetStatusFinished() string {
	return "FIN"
}

//GetStatusError returns the string used in the DB for errors
func GetStatusError() string {
	return "ERR"
}

//GetDomain returns the string used in the DB for the domain
func GetDomain() string {
	return "TimeSeries"
}

//GetMigrationDetailsWhenRunning returns the string used in the DB for details when running
func GetMigrationDetailsWhenRunning() string {
	return "Migration run is started"
}

//GetMigrationDetailsWhenFinished returns the string used in the DB for details when finished
func GetMigrationDetailsWhenFinished() string {
	return "Finished"
}

//GetLogFileName return the name of the log file
func GetLogFileName() string {
	return "./out/timeseries-migration.log"
}

//GetDefaultEnvironment returns the default environment to check for scheduled migration runs
func GetDefaultEnvironment() string {
	return "PROD"
}

//GetMaxOpenConnections returns...
func GetMaxOpenConnections() int {
	return 4
}

//GetMaxIdleConnections returns...
func GetMaxIdleConnections() int {
	return 2
}

//GetScheduledRunFromMigrationTable returns...
func GetScheduledRunFromMigrationTable() bool {
	return false
}