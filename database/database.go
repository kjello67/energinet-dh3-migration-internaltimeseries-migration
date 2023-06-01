package database

import (
	"database/sql"
	log "github.com/sirupsen/logrus"

	_ "github.com/godror/godror"
)

// InitDB opens a connection to the database
func InitDB(dbConnectionString string) (*sql.DB, error) {
	db, err := sql.Open("godror", dbConnectionString)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	//err = db.Ping()
	//if err != nil {
	//	log.Error(err)
	//	return nil,err
	//}
	return db, nil
}
