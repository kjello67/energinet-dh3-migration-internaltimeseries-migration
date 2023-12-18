package repository

import (
	"database/sql"
	"errors"
	log "github.com/sirupsen/logrus"
	"strconv"
	"sync"
	"time"
	"timeseries-migration/config"
	"timeseries-migration/database"
	"timeseries-migration/logger"
	"timeseries-migration/models"
	"timeseries-migration/sqls"
	"timeseries-migration/utils"
)

var (
	sqlstmtNoTimeSeriesFound, sqlstmtTimeSeriesFound, sqlstmtSelectMasterData, sqlstmtSelectTimesSeries *sql.Stmt
	mutexProgressTableInserts                                                                           sync.Mutex
)

type Repository interface {
	InitProgressTableSQLs() error
	ExecSqlstmtTimeSeriesFound(migrationRunId int, fromTimeFormatted, toTimeFormatted, fileName, fileDetails, strId string) error
	ExecSqlstmtNoTimeSeriesFound(id int, fromTimeFormatted string, toTimeFormatted string, filename string, fileDetails string, strId string) error
	ExecSqlstmtSelectTimesSeries(meteringPointId string, processedFromTime, processedUntilTime time.Time) (*sql.Rows, error)
	GetMasterData(meteringPointId string, PST *time.Location) ([]models.Masterdata, error)
	SchedulerWorker() (*models.ScheduledRun, int, error)
	GetNumberOfMeteringPoints(sqlFlag bool, sqlCount string) (*int, error)
	GetMeteringPoints(meteringPoints chan<- []string, nWorkload int, mpFlag bool, sqlObjectIds string) error
	FindDataMigrationExportedPeriod(meteringPointId string, periodFromDate, periodToDate time.Time) (time.Time, bool, error)
	SetSQLUpdateStatusToRunning(migrationRunId int) error
	SetSQLUpdateStatusToFinished(migrationRunId int) error
	SetSQLUpdateStatusToError(migrationRunId int, errorMessage string) error
	GetLogger() *logger.Logger
	Close()
}

var NewRepository = func(dbConnectionString, logConnectionString string, logFileLogger *logger.Logger) (Repository, error) {
	//Open connection to the database from where the data should be retrieved
	db, err := database.InitDB(dbConnectionString, logFileLogger)
	if err != nil {
		//Only log in file as the DB is not available
		(*logFileLogger).Fatal(err)
	}

	//By default, data should be retrieved from same database as logging is done
	logDb := db
	if dbConnectionString != logConnectionString {
		//Open connection to the database where the logging should be done if different from data DB
		logDb, err = database.InitDB(logConnectionString, logFileLogger)
		if err != nil {
			(*logFileLogger).Fatal(err)
		}
	}

	return &Impl{
		Db:            db,
		LogDB:         logDb,
		LogFileLogger: logFileLogger,
	}, err
}

func (i *Impl) Close() {
	sqlstmtNoTimeSeriesFound.Close()
	sqlstmtTimeSeriesFound.Close()
	sqlstmtSelectMasterData.Close()
	sqlstmtSelectTimesSeries.Close()

	i.Db.Close()

	if i.Db != i.LogDB {
		i.LogDB.Close()
	}
}

type Impl struct {
	Db, LogDB     *sql.DB
	LogFileLogger *logger.Logger
}

func (i *Impl) GetLogger() *logger.Logger {
	return i.LogFileLogger
}

func (i *Impl) InitProgressTableSQLs() error {
	var err error

	//Prepare the SQL query that inserts to the progress table
	sqlstmtNoTimeSeriesFound, err = i.LogDB.Prepare(sqls.GetSQLInsertNoDataFound())
	if err != nil {
		(*i.LogFileLogger).Error(err)
		return err
	}

	//Prepare the SQL query that inserts to the progress table
	sqlstmtTimeSeriesFound, err = i.LogDB.Prepare(sqls.GetSQLInsertFinishedTimeSeriesExportFile())
	if err != nil {
		(*i.LogFileLogger).Error(err)
		return err
	}

	//Prepare the SQL query that retrieves the time series
	sqlstmtSelectMasterData, err = i.Db.Prepare(sqls.GetSQLSelectMasterData())
	if err != nil {
		(*i.LogFileLogger).Error(err)
		return err
	}

	//Prepare the SQL query that retrieves the time series
	sqlstmtSelectTimesSeries, err = i.Db.Prepare(sqls.GetSQLSelectData())
	if err != nil {
		(*i.LogFileLogger).Error(err)
		return err
	}

	return nil
}

// ExecSqlstmtTimeSeriesFound Run SQL statement for setting into database information about the migrated timeseries
func (i *Impl) ExecSqlstmtTimeSeriesFound(migrationRunId int, fromTimeFormatted, toTimeFormatted, fileName, fileDetails, strId string) error {
	mutexProgressTableInserts.Lock()
	defer mutexProgressTableInserts.Unlock()

	tx, err := i.LogDB.Begin()
	if err != nil {
		(*i.LogFileLogger).Error(err)
		return err
	}
	_, err = sqlstmtTimeSeriesFound.Exec(migrationRunId, fromTimeFormatted, toTimeFormatted, "Y", fileName, fileDetails, strId)
	if err != nil {
		(*i.LogFileLogger).Error(err)
		err = tx.Rollback()
		if err != nil {
			(*i.LogFileLogger).Error(err)
			return err
		}
		return err
	}
	err = tx.Commit()
	if err != nil {
		(*i.LogFileLogger).Error(err)
		return err
	}

	return nil
}

// ExecSqlstmtNoTimeSeriesFound un SQL statement for setting into database information about that no time series was found for the metering point
func (i *Impl) ExecSqlstmtNoTimeSeriesFound(migrationRunId int, fromTimeFormatted, toTimeFormatted, fileName, fileDetails, strId string) error {
	mutexProgressTableInserts.Lock()
	defer mutexProgressTableInserts.Unlock()

	tx, err := i.LogDB.Begin()
	if err != nil {
		(*i.LogFileLogger).Error(err)
		return err
	}

	_, err = sqlstmtNoTimeSeriesFound.Exec(migrationRunId, fromTimeFormatted, toTimeFormatted, "N", fileName, fileDetails, strId)
	if err != nil {
		(*i.LogFileLogger).Error(err)
		err = tx.Rollback()
		if err != nil {
			(*i.LogFileLogger).Error(err)
			return err
		}
		return err
	}
	err = tx.Commit()
	if err != nil {
		(*i.LogFileLogger).Error(err)
		return err
	}

	return nil
}

// ExecSqlstmtSelectTimesSeries Run SQL for getting time series for a metering point
func (i *Impl) ExecSqlstmtSelectTimesSeries(meteringPointId string, processedFromTime, processedUntilTime time.Time) (*sql.Rows, error) {

	rows, err := sqlstmtSelectTimesSeries.Query(meteringPointId, processedFromTime, processedUntilTime)

	if err != nil {
		(*i.LogFileLogger).Error(err)
		return nil, err
	}

	return rows, nil
}

// GetMasterData Run SQL for getting masterdata for a meteringpoint
func (i *Impl) GetMasterData(meteringPointId string, PST *time.Location) ([]models.Masterdata, error) {

	var masterDataRows []models.Masterdata
	var masterDataRow models.Masterdata
	var gridArea, typeOfMP, validFromDateFormatted string
	var validToDateFormatted *string
	var validFromDate, validToDate utils.NullTime

	//Run the prepared SQL query that retrieves the time series
	rows, err := sqlstmtSelectMasterData.Query(meteringPointId)
	if err != nil {
		(*i.LogFileLogger).Error(err)
		return nil, err
	}
	defer rows.Close()

	//Loop through the result from the executed SQL query
	for rows.Next() {

		//Store the values from the current line in the result to local variables
		rows.Scan(
			&meteringPointId, &gridArea, &typeOfMP, &validFromDate, &validToDate)

		validFromDateFormatted, err = utils.FormatDate(PST, validFromDate, "", i.LogFileLogger)
		if err != nil {
			(*i.LogFileLogger).Error(err)
			return masterDataRows, err
		}
		validToDateFormatted, err = utils.FormatDatePointer(PST, validToDate, i.LogFileLogger)
		if err != nil {
			(*i.LogFileLogger).Error(err)
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

func checkDataMigrationExportedPeriod(db *sql.DB, periodFromDate time.Time, migrationRunId int) error {
	rows, err := db.Query(sqls.GetDataMigrationExportedPeriod())
	if err != nil {
		log.Error(err)
		return err
	}
	defer rows.Close()

	var maxFromDate, maxToDate utils.NullTime

	//Loop through the result from the executed SQL query
	for rows.Next() {

		//Store the values from the current line in the result to local variables
		err = rows.Scan(&maxFromDate, &maxToDate)
		if err != nil {
			return err
		}

		if maxToDate.Valid {
			if periodFromDate.After(maxToDate.Time) {
				errorText := "Cannot start migration run " + strconv.Itoa(migrationRunId) + ",  Start date " + periodFromDate.UTC().Format(config.GetJSONDateLayoutLong()) +
					" is after the last end date " + maxToDate.Time.UTC().Format(config.GetJSONDateLayoutLong())
				err = errors.New(errorText)
				log.Error(errorText + " - migration run aborted")
			} else {
				err = nil
			}
		} else {
			// First Run, any date is OK
			err = nil
		}
	}

	return err
}

//SchedulerWorker reads the DB to see if there are any scheduled migration runs
func (i *Impl) SchedulerWorker() (*models.ScheduledRun, int, error) {

	//timer := time.Now()

	var scheduledRuns []models.ScheduledRun

	//Check if there exists any scheduled runs
	rows, err := i.LogDB.Query(sqls.GetSQLSelectNewRuns())
	if err != nil {
		(*i.LogFileLogger).Error(err)
		return nil, -1, err
	}
	defer rows.Close()

	//Variables to hold the scheduler data
	var scheduledRun models.ScheduledRun
	var threads, migrationRunId int
	var migrationDueDate utils.NullTime
	var periodFromDate, periodToDate time.Time
	var parameter, migrationDueDateFormatted string
	var useListOfMPs, useListOfOwners, useListOfGridAreas bool

	PST, err := time.LoadLocation(config.GetTimeLocation())
	if err != nil {
		(*i.LogFileLogger).Error(err)
		return nil, -1, err
	}

	//Loop through the result from the executed SQL query
	for rows.Next() {

		//Store the values from the current line in the result to local variables
		err = rows.Scan(&migrationRunId, &threads, &migrationDueDate, &parameter, &useListOfMPs, &useListOfOwners, &useListOfGridAreas, &periodFromDate, &periodToDate)
		if err != nil {
			return nil, -1, err
		}

		//Format the dates to ISO 8601 (RFC-3339)
		if migrationDueDate.Valid {
			migrationDueDateFormatted = migrationDueDate.Time.Format(config.GetJSONDateLayoutShort())
			t, err := time.ParseInLocation(config.GetJSONDateLayoutShort(), migrationDueDateFormatted, PST)
			if err != nil {
				(*i.LogFileLogger).Error(err)
				return nil, migrationRunId, err
			}

			migrationDueDateFormatted = t.UTC().Format(config.GetJSONDateLayoutLong())
		} else {
			migrationDueDateFormatted = ""
		}

		err := checkDataMigrationExportedPeriod(i.LogDB, periodFromDate, migrationRunId)

		if err != nil {
			return nil, migrationRunId, err
		}

		if parameter == "" {
			errorText := "DB column PARAMETER is mandatory"
			err = errors.New(errorText)
			(*i.LogFileLogger).ErrorWithText(errorText + " - migration run aborted")
			return nil, migrationRunId, err
		}

		scheduledRun.Threads = threads
		scheduledRun.MigrationDueDate = migrationDueDateFormatted
		scheduledRun.MigrationRunId = migrationRunId
		scheduledRun.UseListOfMPs = useListOfMPs
		scheduledRun.PeriodFromDate = periodFromDate
		scheduledRun.PeriodToDate = periodToDate
		scheduledRuns = append(scheduledRuns, scheduledRun)
	}

	if len(scheduledRuns) == 1 {
		//If all other statuses != "RUN" -> continue
		return &scheduledRuns[0], migrationRunId, nil //TODO Verify that it's correct to expect only one scheduled run
	} else if len(scheduledRuns) == 0 {
		if err != nil {
			//log in file why the export is not started
			(*i.LogFileLogger).ErrorWithText("Something went wrong - " + err.Error())
			return nil, -1, err
		} else {
			(*i.LogFileLogger).Debug("No (more) scheduled runs found")
			return nil, -1, nil
		}
	} else { //TODO What to do if there are more than one scheduled run?
		(*i.LogFileLogger).Debug("Too many scheduled runs found")
		return nil, -1, nil
	}
}

//GetNumberOfMeteringPoints finds the number of metering points to be migrated (all or a subset defined in the config file)
func (i *Impl) GetNumberOfMeteringPoints(sqlFlag bool, sqlCount string) (*int, error) {

	var meteringPointCount int
	query := ""
	if sqlFlag {
		query = sqlCount
	} else {
		query = "select count(distinct(objectid)) from reading.m_meterpoint"
	}

	err := i.Db.QueryRow(query).Scan(&meteringPointCount)
	if err != nil {
		(*i.LogFileLogger).Error(err)
		return nil, err
	}

	return &meteringPointCount, nil
}

//GetMeteringPoints populates the channel with the metering points that will be migrated (all or a subset defined in the config file)
func (i *Impl) GetMeteringPoints(meteringPoints chan<- []string, nWorkload int, mpFlag bool, sqlObjectIds string) error {

	//Variable to hold a list of meteringPoints equal to the number specified in nWorkload
	var meteringPointSlice []string

	//Prepare SQL statement to fetch meteringPoints to migrate
	query := ""
	if mpFlag {
		query = sqlObjectIds
	} else {
		query = "select distinct(objectid) from reading.m_meterpoint"
	}

	//Run the prepared SQL statement
	rows, err := i.Db.Query(query)
	if err != nil {
		(*i.LogFileLogger).Error(err)
		return err
	}
	defer rows.Close()

	//Loop through the metering points returned from the database
	var objectId string
	for rows.Next() {

		//Fetch the current metering points into the objectId variable
		err := rows.Scan(&objectId)
		if err != nil {
			(*i.LogFileLogger).Error(err)
			return err
		}

		//Add the objectId variable to the list of metering points in meteringPointSlice
		meteringPointSlice = append(meteringPointSlice, objectId)

		//If the number of metering points in the list matches the number specified in nWorkload, write the list to the channel and then empty it
		if len(meteringPointSlice) == nWorkload {
			meteringPoints <- meteringPointSlice
			meteringPointSlice = nil
		}
	}
	if err = rows.Err(); err != nil {
		(*i.LogFileLogger).Error(err)
		return err
	}

	//If the list still contains any meteringPoints, write the list to the channel
	if meteringPointSlice != nil {
		meteringPoints <- meteringPointSlice
	}
	return nil
}

// FindDataMigrationExportedPeriod Find the last period this for which this meteringpoint is already migrated
func (i *Impl) FindDataMigrationExportedPeriod(meteringPointId string, periodFromDate, periodToDate time.Time) (time.Time, bool, error) {
	//Prepare the SQL query that inserts to the progress table
	migrate := true
	rows, err := i.LogDB.Query(sqls.GetDataMigrationExportedPeriodForMp(meteringPointId))
	if err != nil {
		(*i.LogFileLogger).Error(err)
		return periodFromDate, false, err
	}
	defer rows.Close()

	var exportedToDate utils.NullTime

	//Loop through the result from the executed SQL query
	for rows.Next() {

		//Store the values from the current line in the result to local variables
		err = rows.Scan(&exportedToDate)
		if err != nil {
			return periodFromDate, false, err
		}

		if exportedToDate.Valid {
			if exportedToDate.Time.Before(periodToDate) {
				if exportedToDate.Time.Before(periodFromDate) {
					if exportedToDate.Time.UTC().Before(time.Date(2000, 12, 31, 23, 0, 0, 0, time.UTC)) {
						periodFromDate = time.Date(2000, 12, 31, 23, 0, 0, 0, time.UTC)
					} else {
						periodFromDate = exportedToDate.Time
					}
				}
			} else {
				migrate = false
			}
		}
	}

	return periodFromDate, migrate, err

}

func (i *Impl) SetSQLUpdateStatusToRunning(migrationRunId int) error {
	tx, err := i.LogDB.Begin()
	if err != nil {
		(*i.LogFileLogger).Error(err)
		return err
	}
	_, err = i.LogDB.Exec(sqls.GetSQLUpdateStatusToRunning(migrationRunId))
	if err != nil {
		(*i.LogFileLogger).Error(err)
		err = tx.Rollback()
		if err != nil {
			(*i.LogFileLogger).Error(err)
			return err
		}
		return err
	}
	err = tx.Commit()
	if err != nil {
		(*i.LogFileLogger).Error(err)
		return err
	}

	return nil
}

func (i *Impl) SetSQLUpdateStatusToFinished(migrationRunId int) error {
	tx, err := i.LogDB.Begin()
	if err != nil {
		(*i.LogFileLogger).Error(err)
		return err
	}
	_, err = i.LogDB.Exec(sqls.GetSQLUpdateStatusToFinished(migrationRunId))
	if err != nil {
		(*i.LogFileLogger).Error(err)
		err = tx.Rollback()
		if err != nil {
			(*i.LogFileLogger).Error(err)
			return err
		}
		return err
	}
	err = tx.Commit()
	if err != nil {
		(*i.LogFileLogger).Error(err)
		return err
	}

	return nil
}

func (i *Impl) SetSQLUpdateStatusToError(migrationRunId int, errorMessage string) error {
	tx, err := i.LogDB.Begin()
	if err != nil {
		(*i.LogFileLogger).Error(err)
		return err
	}
	_, err = i.LogDB.Exec(sqls.GetSQLUpdateStatusToError(migrationRunId, errorMessage))
	if err != nil {
		(*i.LogFileLogger).Error(err)
		err = tx.Rollback()
		if err != nil {
			(*i.LogFileLogger).Error(err)
			return err
		}
		return err
	}
	err = tx.Commit()
	if err != nil {
		(*i.LogFileLogger).Error(err)
		return err
	}

	return nil
}
