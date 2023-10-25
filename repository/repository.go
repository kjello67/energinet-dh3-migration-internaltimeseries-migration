package repository

import (
	"database/sql"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
	"timeseries-migration/models"
	"timeseries-migration/sqls"
	"timeseries-migration/utils"
)

var sqlstmtNoTimeSeriesFound, sqlstmtTimeSeriesFound, sqlstmtSelectMasterData, sqlstmtSelectTimesSeries *sql.Stmt
var mutexProgressTableInserts sync.Mutex

type Repository interface {
	InitProgressTableSQLs() error
	ExecSqlstmtTimeSeriesFound(migrationRunId int, fromTimeFormatted, toTimeFormatted, fileName, fileDetails, strId string) error
	ExecSqlstmtNoTimeSeriesFound(id int, fromTimeFormatted string, toTimeFormatted string, filename string, fileDetails string, strId string) error
    ExecSqlstmtSelectTimesSeries(meteringPointId string, processedFromTime, processedUntilTime time.Time) (*sql.Rows, error)
	GetMasterData(meteringPointId string, PST *time.Location) ([]models.Masterdata, error)
	Close()
}



var NewRepository = func(db, logDb *sql.DB) Repository {
	return &Impl{
		Db: db,
		LogDB: logDb,

	}
}

func  (i *Impl)  Close() {
	sqlstmtNoTimeSeriesFound.Close()
	sqlstmtTimeSeriesFound.Close()
	sqlstmtSelectMasterData.Close()
	sqlstmtSelectTimesSeries.Close()
}

type Impl struct {
	Db, LogDB                 *sql.DB

}

func (i *Impl) InitProgressTableSQLs() error {
	var err error

	//Prepare the SQL query that inserts to the progress table
	sqlstmtNoTimeSeriesFound, err = i.LogDB.Prepare(sqls.GetSQLInsertNoDataFound())
	if err != nil {
		log.Error(err)
		return err
	}

	//Prepare the SQL query that inserts to the progress table
	sqlstmtTimeSeriesFound, err = i.LogDB.Prepare(sqls.GetSQLInsertFinishedTimeSeriesExportFile())
	if err != nil {
		log.Error(err)
		return err
	}

	//Prepare the SQL query that retrieves the time series
	sqlstmtSelectMasterData, err = i.Db.Prepare(sqls.GetSQLSelectMasterData())
	if err != nil {
		log.Error(err)
		return err
	}

	//Prepare the SQL query that retrieves the time series
	sqlstmtSelectTimesSeries, err = i.Db.Prepare(sqls.GetSQLSelectData())
	if err != nil {
		log.Error(err)
		return err
	}

	return nil
}

// ExecSqlstmtTimeSeriesFound
func (i *Impl) ExecSqlstmtTimeSeriesFound(migrationRunId int, fromTimeFormatted, toTimeFormatted, fileName, fileDetails, strId string) error {
	mutexProgressTableInserts.Lock()
	defer mutexProgressTableInserts.Unlock()

	_, err := sqlstmtTimeSeriesFound.Exec(migrationRunId, fromTimeFormatted, toTimeFormatted, "Y", fileName, fileDetails, strId)
	if err != nil {
		log.Error(err)
		return err
	}

	return nil
}

// ExecSqlstmtNoTimeSeriesFound
func (i *Impl) ExecSqlstmtNoTimeSeriesFound(migrationRunId int, fromTimeFormatted, toTimeFormatted, fileName, fileDetails, strId string) error {
	mutexProgressTableInserts.Lock()
	defer mutexProgressTableInserts.Unlock()

	_, err := sqlstmtNoTimeSeriesFound.Exec(migrationRunId, fromTimeFormatted, toTimeFormatted, "N", fileName, fileDetails, strId)
	if err != nil {
		log.Error(err)
		return err
	}

	return nil
}

// ExecSqlstmtSelectTimesSeries
func (i *Impl) ExecSqlstmtSelectTimesSeries(meteringPointId string, processedFromTime, processedUntilTime time.Time) (*sql.Rows, error) {

	rows, err := sqlstmtSelectTimesSeries.Query(meteringPointId, processedFromTime, processedUntilTime)

	if err != nil {
		log.Error(err)
		return nil, err
	}

	return rows, nil
}


func (i *Impl) GetMasterData(meteringPointId string, PST *time.Location) ([]models.Masterdata, error) {

	var masterDataRows []models.Masterdata
	var masterDataRow models.Masterdata
	var gridArea, typeOfMP, validFromDateFormatted string
	var validToDateFormatted *string
	var validFromDate, validToDate utils.NullTime

	//Run the prepared SQL query that retrieves the time series
	rows, err := sqlstmtSelectMasterData.Query(meteringPointId)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	defer rows.Close()

	//Loop through the result from the executed SQL query
	for rows.Next() {

		//Store the values from the current line in the result to local variables
		rows.Scan(
			&meteringPointId, &gridArea, &typeOfMP, &validFromDate, &validToDate)

		validFromDateFormatted, err = utils.FormatDate(PST, validFromDate, "")
		if err != nil {
			log.Error(err)
			return masterDataRows, err
		}
		validToDateFormatted, err = utils.FormatDatePointer(PST, validToDate)
		if err != nil {
			log.Error(err)
			return masterDataRows, err
		}
		masterDataRow.GridArea = gridArea
		masterDataRow.TypeOfMP = typeOfMP
		masterDataRow.MasterDataStartDate = validFromDateFormatted
		masterDataRow.MasterDataEndDate = validToDateFormatted
		masterDataRows = append(masterDataRows, masterDataRow)
	}
	return masterDataRows, nil
}


