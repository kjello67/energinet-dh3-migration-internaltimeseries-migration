package database

import (
	"database/sql"
	"internaltimeseries-migration/logger"

	_ "github.com/godror/godror"
)

// InitDB opens a connection to the database
func InitDB(dbConnectionString string, logFileLogger *logger.Logger) (*sql.DB, error) {
	db, err := sql.Open("godror", dbConnectionString)
	if err != nil {
		(*logFileLogger).Error(err)
		return nil, err
	}
	//err = db.Ping()
	//if err != nil {
	//	log.Error(err)
	//	return nil,err
	//}
	return db, nil
}
