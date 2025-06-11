package repository

import (
	"database/sql"
	"errors"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"internaltimeseries-migration/config"
	"internaltimeseries-migration/database"
	"internaltimeseries-migration/logger"
	"internaltimeseries-migration/models"
	"internaltimeseries-migration/sqls"
	"internaltimeseries-migration/utils"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	sqlstmtNoTimeSeriesFound, sqlstmtTimeSeriesFound, sqlstmtSelectMasterData, sqlstmtSelectTimesSeries *sql.Stmt
	sqlStmtInsertSerieMessage, sqlStmtInsertSerieCounter                                                *sql.Stmt
	sqlStmtInsertSerieValue, sqlStmtInsertRecipient                                                     *sql.Stmt
	mutexProgressTableInserts, mutexRSM012Inserts                                                       sync.Mutex
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
	FindNextRSM012MessageSeqNo() (int, error)
	FindMeteringPointInfo(meteringpointId string) (int, string, string, error)
	FindSettlementMethodType(meteringpointSeqNo int, fromTimeStr string) (string, error)
	FindBreakRules() ([]string, error)
	CreateRSM012MessageCounter(messageSeqNo int, unit, resolution string, sumSeries int) error
	CreateRSM012MessageValues(messageSeqNo int, data models.TimeSeriesData) error
	CreateRSM012Message(data models.Data) error
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
	sqlStmtInsertSerieMessage.Close()
	sqlStmtInsertSerieCounter.Close()
	sqlStmtInsertSerieValue.Close()
	sqlStmtInsertRecipient.Close()

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

	sqlStmtInsertSerieMessage, err = i.LogDB.Prepare(sqls.GetSQLInsertSerieMessage())
	if err != nil {
		(*i.LogFileLogger).Error(err)
		return err
	}

	sqlStmtInsertSerieCounter, err = i.LogDB.Prepare(sqls.GetSQLInsertSerieCounter())
	if err != nil {
		(*i.LogFileLogger).Error(err)
		return err
	}

	sqlStmtInsertSerieValue, err = i.LogDB.Prepare(sqls.GetSQLInsertSerieValue())
	if err != nil {
		(*i.LogFileLogger).Error(err)
		return err
	}

	sqlStmtInsertRecipient, err = i.LogDB.Prepare(sqls.GetSQLInsertRecipient())
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
	var gridArea, typeOfMP string

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
			&meteringPointId, &gridArea, &typeOfMP)

		masterDataRow.GridArea = gridArea
		masterDataRow.TypeOfMP = typeOfMP
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

// SchedulerWorker reads the DB to see if there are any scheduled migration runs
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

// GetNumberOfMeteringPoints finds the number of metering points to be migrated (all or a subset defined in the config file)
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

// GetMeteringPoints populates the channel with the metering points that will be migrated (all or a subset defined in the config file)
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

// FindNextRSM012MessageSeqNo Find the id to use for the next RSM012 message
func (i *Impl) FindNextRSM012MessageSeqNo() (int, error) {
	//Prepare the SQL query that inserts to the progress table
	rows, err := i.LogDB.Query(sqls.GetSeriesMessSeqNo())
	if err != nil {
		(*i.LogFileLogger).Error(err)
		return -1, err
	}
	defer rows.Close()
	var id = -1

	//Loop through the result from the executed SQL query
	for rows.Next() {

		//Store the values from the current line in the result to local variables
		err = rows.Scan(&id)
		if err != nil {
			return -1, err
		}
	}

	return id, err
}

// FindNextRecipientSeqNo Find the next id for use in the recipient for a new RSM-012 message
func (i *Impl) FindNextRecipientSeqNo() (int, error) {
	//Prepare the SQL query that inserts to the progress table
	rows, err := i.LogDB.Query(sqls.GetRecipientSeqNo())
	if err != nil {
		(*i.LogFileLogger).Error(err)
		return -1, err
	}
	defer rows.Close()
	var id = -1

	//Loop through the result from the executed SQL query
	for rows.Next() {

		//Store the values from the current line in the result to local variables
		err = rows.Scan(&id)
		if err != nil {
			return -1, err
		}
	}

	return id, err
}

// FindMetroingPointInfo Find information about the meteringPoint
func (i *Impl) FindMeteringPointInfo(meteringpointId string) (int, string, string, error) {
	//Prepare the SQL query that inserts to the progress table
	rows, err := i.LogDB.Query(sqls.GetMeteringPointInfo(), meteringpointId)
	if err != nil {
		(*i.LogFileLogger).Error(err)
		return -1, "", "", err
	}
	defer rows.Close()
	var meteringPointSeqNo = -1
	var objectType = ""
	var externalTypeCode = ""

	//Loop through the result from the executed SQL query
	for rows.Next() {

		//Store the values from the current line in the result to local variables
		err = rows.Scan(&meteringPointSeqNo, &objectType, &externalTypeCode)
		if err != nil {
			return -1, "", "", err
		}
	}

	return meteringPointSeqNo, objectType, externalTypeCode, err

}

// FindSettlementMethodType Find settlement method type
func (i *Impl) FindSettlementMethodType(meteringpointSeqNo int, fromTimeStr string) (string, error) {
	//Prepare the SQL query that inserts to the progress table
	rows, err := i.LogDB.Query(sqls.GetSettlementMethodType(), meteringpointSeqNo, fromTimeStr, fromTimeStr)
	if err != nil {
		(*i.LogFileLogger).Error(err)
		return "", err
	}
	defer rows.Close()
	var settlementMethodType = ""

	//Loop through the result from the executed SQL query
	for rows.Next() {

		//Store the values from the current line in the result to local variables
		err = rows.Scan(&settlementMethodType)
		if err != nil {
			return "", err
		}
	}

	return settlementMethodType, err
}

// FindMarketActorByRole Get the marketaAtor by role
func (i *Impl) FindMarketActorByRole(role string) (int, error) {
	//Prepare the SQL query that inserts to the progress table
	rows, err := i.LogDB.Query(sqls.GetMarketActorByRole(), role)
	if err != nil {
		(*i.LogFileLogger).Error(err)
		return -1, err
	}
	defer rows.Close()
	var actorId = -1
	//Loop through the result from the executed SQL query, For the roles we are looking at it should only be 1
	for rows.Next() {

		//Store the values from the current line in the result to local variables
		err = rows.Scan(&actorId)
		if err != nil {
			return -1, err
		}
	}

	return actorId, err
}

// FindBreakRules Get the fields used in the break strings
func (i *Impl) FindBreakRules() ([]string, error) {
	//Prepare the SQL query that inserts to the progress table
	rows, err := i.LogDB.Query(sqls.GetBreakRules())
	if err != nil {
		(*i.LogFileLogger).Error(err)
		return nil, err
	}
	defer rows.Close()
	var breakRule string
	var breakRules []string

	//Loop through the result from the executed SQL query, For the roles we are looking at it should only be 1
	for rows.Next() {

		//Store the values from the current line in the result to local variables
		err = rows.Scan(&breakRule)
		if err != nil {
			return nil, err
		}

		breakRules = append(breakRules, breakRule)
	}

	return breakRules, err
}

func (i *Impl) CreateRSM012MessageCounter(messageSeqNo int, unit, resolution string, sumSeries int) error {

	var resolutionStr string
	resolutionInt, err := strconv.Atoi(resolution)

	if err != nil {
		return err
	}

	if resolutionInt == 15 {
		resolutionStr = "Q"
	} else {
		if resolutionInt == 60 {
			resolutionStr = "H"
		} else {
			if resolutionInt == 1440 {
				resolutionStr = "D"
			} else {
				if resolutionInt == 43200 {
					resolutionStr = "Y"
				}
			}
		}
	}

	_, err = sqlStmtInsertSerieCounter.Exec(messageSeqNo, unit, resolutionStr, sumSeries)
	if err != nil {
		(*i.LogFileLogger).Error(err)
		return err
	} else {
		return nil
	}
}

func (i *Impl) CreateRSM012MessageValues(messageSeqNo int, data models.TimeSeriesData) error {
	values := strings.Split(data.RawTimeSeriesValues, ";")

	listNo := 1
	splitData2Dim := utils.ConvertToTwoDArray(values, 150)

	for _, splitData1Dim := range splitData2Dim {

		var valueList, valueQualList, startTimeList, completenessList strings.Builder
		first := true

		for _, data := range splitData1Dim {

			if data != "" {
				details := strings.Split(data, "|")
				if len(details) < 3 {
					continue
				}

				dateTime := details[0]
				if len(dateTime) < 16 {
					continue
				}

				year := dateTime[6:10]
				month := dateTime[3:5]
				day := dateTime[0:2]
				hour := dateTime[11:13]
				minutes := dateTime[14:16]

				if !first {
					startTimeList.WriteString(";")
					valueList.WriteString(";")
					valueQualList.WriteString(";")
					completenessList.WriteString(";")
				}

				startTimeList.WriteString(year + month + day + hour + minutes)

				value := details[1]
				if strings.HasPrefix(value, ",") {
					value = "0" + value
				}
				value = strings.ReplaceAll(value, ",", ".")

				valueList.WriteString(value)
				valueQualList.WriteString(utils.GetInternalValueQualifier(strings.TrimSuffix(details[2], ";")))
				completenessList.WriteString("100")

				first = false
			}

		}
		valueStr := valueList.String()
		valueQualStr := valueQualList.String()
		startTimeStr := startTimeList.String()
		completenessStr := completenessList.String()

		_, err := sqlStmtInsertSerieValue.Exec(listNo, messageSeqNo, valueStr, valueQualStr, startTimeStr, completenessStr)
		if err != nil {
			(*i.LogFileLogger).Error(err)
			return err
		}

		listNo++
	}
	return nil
}

func (i *Impl) CreateRSM012Message(data models.Data) error {
	mutexRSM012Inserts.Lock()
	defer mutexRSM012Inserts.Unlock()

	objectId := data.MeteringPointData.MeteringPointId
	gridArea := data.MeteringPointData.MasterData[0].GridArea
	reasonForReading := "50"

	for _, seriesData := range data.TimeSeries {
		var meteringPointSeqNo = -1
		var objectType = ""
		var externalTypeCode = ""
		var msgSettlementMethod = ""
		messageSeqNo, err := i.FindNextRSM012MessageSeqNo()

		meteringPointSeqNo, objectType, externalTypeCode, err = i.FindMeteringPointInfo(objectId)
		dateGeneratedStr := time.Now().Format(config.GetExportDateLayout())
		fromTime, err := time.Parse(config.GetJSONDateLayoutLong(), seriesData.ValidFromDate)

		var fromTimeStr = fromTime.Format(config.GetExportDateLayout())
		settlementMethod, err := i.FindSettlementMethodType(meteringPointSeqNo, fromTime.In(time.Local).Format(config.GetExportDateLayout()))

		toTime, err := time.Parse(config.GetJSONDateLayoutLong(), seriesData.ValidToDate)
		var toTimeStr = toTime.Format(config.GetExportDateLayout())
		transactionInsertDate, err := time.Parse(config.GetJSONDateLayoutLong(), seriesData.TransactionInsertDate)
		var transactionInsertDateStr = transactionInsertDate.Format(config.GetExportDateLayout())

		if externalTypeCode == "E17" || externalTypeCode == "E18" || externalTypeCode == "E20" {
			externalTypeCode = ""
		}

		if objectType == "C" && externalTypeCode == "" {
			if settlementMethod != "" {
				msgSettlementMethod = settlementMethod
				if settlementMethod == "X" {
					reasonForReading = "75"
				} else if settlementMethod == "S" {
					reasonForReading = "56"
				}
			} else {
				reasonForReading = "50"
			}
		} else {
			reasonForReading = "50"
		}

		tx, err := i.LogDB.Begin()
		if err != nil {
			(*i.LogFileLogger).Error(err)
			return err
		}

		_, err = sqlStmtInsertSerieMessage.Exec(messageSeqNo, meteringPointSeqNo, objectId, objectId, gridArea, dateGeneratedStr, fromTimeStr, toTimeStr, objectType, gridArea, objectType, reasonForReading, msgSettlementMethod, externalTypeCode, transactionInsertDateStr, seriesData.Status)
		if err != nil {
			(*i.LogFileLogger).Error(err)
			err = tx.Rollback()
			if err != nil {
				(*i.LogFileLogger).Error(err)
				return err
			}
			return err
		}

		err = i.CreateRSM012MessageCounter(messageSeqNo, seriesData.Unit, seriesData.Resolution, len(seriesData.RawTimeSeriesValues))
		if err != nil {
			(*i.LogFileLogger).Error(err)
			err = tx.Rollback()
			if err != nil {
				(*i.LogFileLogger).Error(err)
				return err
			}
		}

		err = i.CreateRSM012MessageValues(messageSeqNo, seriesData)
		if err != nil {
			(*i.LogFileLogger).Error(err)
			err = tx.Rollback()
			if err != nil {
				(*i.LogFileLogger).Error(err)
				return err
			}
		}

		err = i.CreateRSM012MessageRecipient(messageSeqNo, reasonForReading)
		if err != nil {
			(*i.LogFileLogger).Error(err)
			err = tx.Rollback()
			if err != nil {
				(*i.LogFileLogger).Error(err)
				return err
			}
		}

		err = tx.Commit()
		if err != nil {
			(*i.LogFileLogger).Error(err)
			return err
		}

		messageSeqNo++
	}

	return nil
}

func getBreakData(recipientRole, senderRole string, senderId, recipientId int, reasonForReading string) map[string]string {
	result := make(map[string]string)

	result["RERO"] = recipientRole
	result["SNRO"] = senderRole
	result["SNID"] = strconv.Itoa(senderId)
	result["REID"] = strconv.Itoa(recipientId)
	result["XRRO"] = recipientRole
	result["XSRO"] = senderRole
	result["MTYP"] = "TSE"
	result["MEST"] = "TP"
	result["GMAV"] = reasonForReading
	result["UTYP"] = "EP"
	result["TEST"] = "P"
	result["XREF"] = ""

	return result
}

func (i *Impl) CreateRSM012MessageRecipient(serieMessageSeqNo int, reasonForReading string) error {

	seqNumber, err := i.FindNextRecipientSeqNo()
	if err != nil {
		(*i.LogFileLogger).Error(err)
		return err
	}

	senderId, err := i.FindMarketActorByRole("DHUB")
	if err != nil {
		(*i.LogFileLogger).Error(err)
		return err
	}

	recipientId, err := i.FindMarketActorByRole("D3M")
	if err != nil {
		(*i.LogFileLogger).Error(err)
		return err
	}

	transRef := strings.ReplaceAll(uuid.New().String(), "-", "")

	var breakStrList strings.Builder

	breakStrList.WriteString(strconv.Itoa(senderId))
	breakStrList.WriteString(";")
	breakStrList.WriteString(strconv.Itoa(recipientId))
	breakStrList.WriteString(";TD;")
	breakStrList.WriteString(reasonForReading)
	breakStrList.WriteString(";P;EP;;D3M")

	_, err = sqlStmtInsertRecipient.Exec(seqNumber, "D3M", senderId, recipientId, transRef, serieMessageSeqNo, breakStrList.String())
	if err != nil {
		(*i.LogFileLogger).Error(err)
		return err
	} else {
		return nil
	}
}
