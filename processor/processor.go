package processor

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
	"timeseries-migration/config"
	"timeseries-migration/models"
	"timeseries-migration/sqls"
)

func logError(err error) (bool, error) {
	return false, err
}

// MigrateTimeSeries extracts metering points from the database and writes the time series to json files for migration to DHUB3
func MigrateTimeSeries(nWorkers, nWorkload int, db, logDb *sql.DB, fileLocation string, sqlFlag bool, sqlItemCount, sqlItemIds string, run *models.ScheduledRun, skipDBUpdate bool) (bool, error) {
	var err error

	fromTimeFormatted := run.PeriodFromDate.Format(config.GetExportDateLayout())
	toTimeFormatted := run.PeriodToDate.Format(config.GetExportDateLayout())

	var meteringPointCount *int
	//Find the number of metering points to migrate
	meteringPointCount, err = GetNumberOfMeteringPoints(db, sqlFlag, sqlItemCount)
	if err != nil {
		return false, err
	}
	log.Info("Found " + strconv.Itoa(*meteringPointCount) + " metering points to migrate")

	//Create the channel that the workers will fetch meteringPoints from
	meteringPoints := make(chan []string, *meteringPointCount)

	//Populate the channel with the meteringPoints to migrate
	err = GetMeteringPoints(db, meteringPoints, nWorkload, sqlFlag, sqlItemIds)
	if err != nil {
		return false, err
	}

	//Close the channel for new entries
	close(meteringPoints)

	//Prepare the SQL query that retrieves the time series
	sqlstmtSelectMasterData, err := db.Prepare(sqls.GetSQLSelectMasterData())
	if err != nil {
		log.Error(err)
		return false, err
	}
	defer sqlstmtSelectMasterData.Close()


	//Prepare the SQL query that retrieves the time series
	sqlstmtSelectTimesSeries, err := db.Prepare(sqls.GetSQLSelectData())
	if err != nil {
		log.Error(err)
		return false, err
	}
	defer sqlstmtSelectTimesSeries.Close()

	//Prepare the SQL query that inserts to the progress table
	sqlstmtNoTimeSeriesFound, err := db.Prepare(sqls.GetSQLInsertNoDataFound())
	if err != nil {
		log.Error(err)
		return false, err
	}
	defer sqlstmtNoTimeSeriesFound.Close()

	//Prepare the SQL query that inserts to the progress table
	sqlstmtTimeSeriesFound, err := logDb.Prepare(sqls.GetSQLInsertFinishedTimeSeriesExportFile())
	if err != nil {
		log.Error(err)
		return false, err
	}
	defer sqlstmtTimeSeriesFound.Close()

	//WaitGroup used to ensure the function doesn't end before all the workers are finished
	wg := sync.WaitGroup{}

	// We just want to return one of the errors from the worker threads (if any)
	resultChannel := make(chan result, nWorkers)

	//Create a number of workers equal to the number in nWorkers
	for worker := 0; worker < nWorkers; worker++ {
		wg.Add(1)
		go func() {
			//Create a worker
			metaInfo, err := TimeSeriesWorker(sqlstmtSelectMasterData, sqlstmtSelectTimesSeries, sqlstmtNoTimeSeriesFound, meteringPoints, fromTimeFormatted, toTimeFormatted, fileLocation, run.MigrationRunId, skipDBUpdate)

			res := result{metaInfo: nil, mainErr: nil, returnValue: true}
			if err != nil {
				res.returnValue = false
				res.mainErr = err
			} else {
				// Just need a unique key. Not used later
				res.metaInfo = metaInfo
			}
			resultChannel <- res

			wg.Done()
		}()
	}

	//Wait for all the workers to be done before closing the database connection
	wg.Wait()

	// Close the channel for new occurrences
	close(resultChannel)

	returnValue := true
	var mainErr error

	// Make the array of metaInfo as 1 map instead of 1 map per worker thread to make the lookup easier
	allItemsInfo := map[string]metaInfo{}
	for m := range resultChannel {
		for k, v := range m.metaInfo {
			allItemsInfo[k] = v
		}
		if m.returnValue == false {
			returnValue = m.returnValue
			mainErr = m.mainErr
		}
	}

	//Rename .tmp to .json in the folder
	filenames, err := renameFiles(fileLocation, config.GetTmpExtension(), config.GetFinalExtension())
	if err == nil {
		if config.GetScheduledRunFromMigrationTable() {
			//Insert the filenames in the progress table
			for i := 0; i < len(filenames); i++ {
				//Run the prepared SQL query that inserts to the progress table
				filename := filenames[i]
				strId := getItemFromFileName(filenames[i])
				info := allItemsInfo[strId]
				fileDetails := fmt.Sprintf("transaction_count_actual_time_series:%d;transaction_count_historical_time_series=%d;sum_actual_reading_values=%d", info.transactionIdCountActual, info.transactionIdCountHist, int(info.sumActualReadingValues))

				if !skipDBUpdate && config.GetScheduledRunFromMigrationTable() {
					_, err = sqlstmtTimeSeriesFound.Exec(run.MigrationRunId, fromTimeFormatted, toTimeFormatted, "Y", filename, fileDetails, info.meteringPointId)
					if err != nil {
						log.Error(err)
						return false, err
					}
				}
			}
		}
	} else {
		return false, err
	}
	return returnValue, mainErr
}

//GetNumberOfMeteringPoints finds the number of metering points to be migrated (all or a subset defined in the config file)
func GetNumberOfMeteringPoints(db *sql.DB, sqlFlag bool, sqlCount string) (*int, error) {

	var meteringPointCount int
	query := ""
	if sqlFlag {
		query = sqlCount
	} else {
		query = "select count(distinct(objectid)) from reading.m_meterpoint"
	}

	err := db.QueryRow(query).Scan(&meteringPointCount)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	return &meteringPointCount, nil
}

//GetMeteringPoints populates the channel with the metering points that will be migrated (all or a subset defined in the config file)
func GetMeteringPoints(db *sql.DB, meteringPoints chan<- []string, nWorkload int, mpFlag bool, sqlObjectIds string) error {

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
	rows, err := db.Query(query)
	if err != nil {
		log.Error(err)
		return err
	}
	defer rows.Close()

	//Loop through the metering points returned from the database
	var objectId string
	for rows.Next() {

		//Fetch the current metering points into the objectId variable
		err := rows.Scan(&objectId)
		if err != nil {
			log.Error(err)
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
		log.Error(err)
		return err
	}

	//If the list still contains any meteringPoints, write the list to the channel
	if meteringPointSlice != nil {
		meteringPoints <- meteringPointSlice
	}
	return nil
}

//TimeSeriesWorker reads the metering points channel and writes the time series for each metering point in a separate json file
func TimeSeriesWorker(sqlstmtSelectMasterData, sqlstmtSelectTimeSeries, sqlstmtNoTimeSeriesFound *sql.Stmt, items <-chan []string, fromTimeFormatted, toTimeFormatted, fileLocation string, migrationRunId int, skipDBUpdate bool) (map[string]metaInfo, error) {

	timer := time.Now()
	timeSeriesInfo := map[string]metaInfo{}
	fromTime, _ := time.Parse(config.GetExportDateLayout(), fromTimeFormatted)
	toTime, _ := time.Parse(config.GetExportDateLayout(), toTimeFormatted)

	//For each metering point read from the metering points channel
	for itemSlice := range items {

		for _, itemId := range itemSlice {

			//The list of time series that will be written to the file
			data, metaInfo, err := getTimeSeriesList(itemId, fromTime, toTime,  sqlstmtSelectMasterData, sqlstmtSelectTimeSeries)

			if metaInfo != nil {
				timeSeriesInfo[itemId] = *metaInfo
			}

			if len(data.TimeSeries) > 0 {
				//Prepare the json for printing
				finishedJSON, err := json.MarshalIndent(data, "", "   ")
				//data = nil
				if err != nil {
					log.Error(err)
					return nil, err
				}

				//Sets the filename based on the current objectId. Will have extension tmp until all are done and then changed to json
				fileNameWithoutExtension :=
					config.GetFilenamePrefix() +
						config.GetFilenameDelimiter() +
						strconv.Itoa(migrationRunId) +
						config.GetFilenameDelimiter() +
						itemId +
						config.GetFilenameDelimiter() +
						fromTime.Format(config.GetFileDateLayout()) +
						config.GetFilenameDelimiter() +
						toTime.Format(config.GetFileDateLayout())

				//timer.Format(config.GetFilenameDateLayout())
				fileName := fileLocation + "/" + fileNameWithoutExtension + config.GetTmpExtension()

				//Write the json file
				err = ioutil.WriteFile(fileName, finishedJSON, 0644)
				if err != nil {
					log.Error(err)
					return nil, err
				}
			} else {
				//No time series found for MP
				log.Debug("No time series found for meterpoint ", itemId)

				if config.GetScheduledRunFromMigrationTable() {
					if !skipDBUpdate && config.GetScheduledRunFromMigrationTable() {
						filename := "N/A"
						fileDetails := fmt.Sprintf("transaction_count_actual_time_series:%d;transaction_count_historical_time_series=%d;sum_actual_reading_values=%d",0, 0,0)

						//Run the prepared SQL query that inserts to the progress table
						_, err := sqlstmtNoTimeSeriesFound.Exec(migrationRunId, fromTimeFormatted, toTimeFormatted, "N", filename, fileDetails, itemId)
						if err != nil {
							log.Error(err)
							return nil, err
						}
					}
				}
			}

			if err != nil {
				log.Error(err)
				return nil, err
			}
		}
	}
	log.Info("Time since TimeSeriesWorker started: ", time.Since(timer))
	return timeSeriesInfo, nil
}

func getMasterData(meteringPointId string, sqlstmtSelectMasterData *sql.Stmt, PST *time.Location)  ([]models.Masterdata, error) {

	var masterDataRows []models.Masterdata
	var masterDataRow models.Masterdata
	var gridArea, typeOfMP, validFromDateFormatted,validToDateFormatted  string
	var validFromDate, validToDate NullTime

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

		validFromDateFormatted, err = formatDate(PST, validFromDate)
		if err != nil {
			log.Error(err)
			return masterDataRows, err
		}
		validToDateFormatted, err = formatDate(PST, validToDate)
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


func getTimeSeriesList(meteringPointId string, processedFromTime, processedUntilTime time.Time,  sqlstmtSelectMasterData, sqlstmtSelectTimeSeries *sql.Stmt) (models.Data, *metaInfo, error) {
	//The list of time series that will be written to the file
	var meteringPointData models.MeteringPointData
	meteringPointData.MeteringPointId = meteringPointId
	var masterData []models.Masterdata
	var data models.Data

	PST, err := time.LoadLocation(config.GetTimeLocation())
	if err != nil {
		log.Error(err)
		return data, nil, err
	}

	masterData, err = getMasterData(meteringPointId, sqlstmtSelectMasterData, PST)
	if err != nil {
		log.Error(err)
		return data, nil, err
	}

	meteringPointData.MasterData = masterData
	var timeSeriesList []models.TimeSeriesData

	data.MeteringPointData = meteringPointData
	data.TimeSeries = timeSeriesList

	rows, err := sqlstmtSelectTimeSeries.Query(meteringPointId, processedFromTime, processedUntilTime)
	if err != nil {
		log.Error(err)
		return data, nil, err
	}
	defer rows.Close()

	//Variables to hold the time series
	var timeSeriesData models.TimeSeriesData
	var timeSerieValues []models.TimeSeriesValue
	var transactionId, readReason NullString
	var transactionIdStr, readReasonStr string
	var historicalFlag, resolution, unit string
	var validFromDate, validToDate, transactionInsertDate NullTime
	var validFromDateFormatted, prevValidFromDateFormatted, validToDateFormatted, transactionInsertDateFormatted string
	var timeSeriesValue models.TimeSeriesValue
	var position, serieStatus int
	var quantity float64
	var quality string
	var readingTime NullTime

	transactionIdCountActual := map[string]int{}
	transactionIdCountHist := map[string]int{}
	var sumActualReadingValues float64
	sumActualReadingValues = 0.0

	//var readingTimeFormatted string
	prevValidFromDateFormatted = ""

	//Loop through the result from the executed SQL query
	for rows.Next() {

		//Store the values from the current line in the result to local variables
		rows.Scan(
			&meteringPointId,
			&transactionId,
			&validFromDate,
			&validToDate,
			&transactionInsertDate,
			&historicalFlag,
			&resolution,
			&unit,
			&position,
			&readingTime,
			&quantity,
			&quality,
			&serieStatus,
			&readReason)

		validFromDateFormatted, err = formatDate(PST, validFromDate)
		if err != nil {
			log.Error(err)
			return data, nil, err
		}
		validToDateFormatted, err = formatDate(PST, validToDate)
		if err != nil {
			log.Error(err)
			return data, nil, err
		}
		transactionInsertDateFormatted, err = formatDate(PST, transactionInsertDate)
		if err != nil {
			log.Error(err)
			return data, nil, err
		}

		if prevValidFromDateFormatted != validFromDateFormatted {
			if prevValidFromDateFormatted != "" {
				timeSeriesData.TimeSeriesValues = timeSerieValues
				timeSerieValues = nil

				timeSeriesList = append(timeSeriesList, timeSeriesData)
			}
		}

		timeSeriesValue.Position = position
		timeSeriesValue.Quantity = quantity

		sumActualReadingValues   += quantity
		timeSeriesValue.Quality = quality

		transactionIdStr = formatNullString(transactionId)
		readReasonStr = formatNullString(readReason)

		timeSeriesData.TransactionId = transactionIdStr
		timeSeriesData.ValidFromDate = validFromDateFormatted
		timeSeriesData.ValidToDate = validToDateFormatted
		timeSeriesData.TransactionInsertDate = transactionInsertDateFormatted
		timeSeriesData.HistoricalFlag = historicalFlag

		if historicalFlag == "N" {
			_, ok := transactionIdCountActual[transactionIdStr]
			if !ok {
				transactionIdCountActual[transactionIdStr] = 1
			}
		} else {
			_, ok := transactionIdCountHist [transactionIdStr]
			if !ok {
				transactionIdCountHist [transactionIdStr] = 1
			}
		}

		timeSeriesData.Resolution = resolution
		timeSeriesData.Unit = unit
		timeSeriesData.Status = serieStatus
		timeSeriesData.ReadReason = readReasonStr
		timeSeriesValue.Position = position
		timeSeriesValue.Quantity = quantity
		timeSeriesValue.Quality = quality
		timeSerieValues = append(timeSerieValues, timeSeriesValue)
		prevValidFromDateFormatted = validFromDateFormatted
	}

	if timeSerieValues != nil {
		timeSeriesData.TimeSeriesValues = timeSerieValues
		timeSeriesList = append(timeSeriesList, timeSeriesData)
		data.TimeSeries = timeSeriesList
	}

	if len(timeSeriesList) > 0 {
		metaInfo := metaInfo{meteringPointId : meteringPointId, transactionIdCountActual  : len(transactionIdCountActual), transactionIdCountHist   : len(transactionIdCountHist ), sumActualReadingValues   :sumActualReadingValues   }
		return data, &metaInfo, err
	} else {
		return data, nil, err
	}
}

func formatNullString(nullString NullString) string {
	var formattedString string
	if nullString.Valid {
		formattedString = nullString.String
	} else {
		formattedString = ""
	}
	return formattedString
}

func formatNullStringPointer(nullString NullString) *string {
	var formattedString *string
	if nullString.Valid {
		if nullString.String == "" {
			formattedString = nil
		} else {
			formattedString = &nullString.String
		}
	} else {
		formattedString = nil
	}
	return formattedString
}

func formatNullFloat(nullFloat NullFloat) float64 {
	var formattedString float64
	if nullFloat.Valid {
		formattedString = nullFloat.Float64
	} else {
		formattedString = -1
	}
	return formattedString
}

func formatDate(PST *time.Location, nullTime NullTime) (string, error) {
	//Format the dates to ISO 8601 (RFC-3339)
	var dateFormatted string
	if nullTime.Valid {
		dateFormatted = nullTime.Time.Format(config.GetJSONDateLayoutLong())
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

func formatDate2(PST *time.Location, nullTime NullTime) (string, error) {
	//Format the dates to ISO 8601 (RFC-3339)
	var dateFormatted string
	if nullTime.Valid {
		dateFormatted = nullTime.Time.Format(config.GetFileDateLayout())
		t, err := time.ParseInLocation(config.GetFileDateLayout(), dateFormatted, PST)
		if err != nil {
			log.Error(err)
			return "", err
		}
		dateFormatted = t.UTC().Format(config.GetFileDateLayout())
	} else {
		dateFormatted = ""
	}
	return dateFormatted, nil
}

//SchedulerWorker reads the DB to see if there are any scheduled migration runs
func SchedulerWorker(db *sql.DB) (*models.ScheduledRun, int, error) {

	timer := time.Now()

	var scheduledRuns []models.ScheduledRun

	//Check if there exists any scheduled runs
	rows, err := db.Query(sqls.GetSQLSelectNewRuns())
	if err != nil {
		log.Error(err)
		return nil, -1, err
	}
	defer rows.Close()

	//Variables to hold the scheduler data
	var scheduledRun models.ScheduledRun
	var threads, migrationRunId int
	var migrationDueDate NullTime
	var periodFromDate, periodToDate time.Time
	var parameter, migrationDueDateFormatted string
	var useListOfMPs, useListOfOwners, useListOfGridAreas bool

	PST, err := time.LoadLocation(config.GetTimeLocation())
	if err != nil {
		log.Error(err)
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
				log.Error(err)
				return nil, migrationRunId, err
			}

			migrationDueDateFormatted = t.UTC().Format(config.GetJSONDateLayoutLong())
		} else {
			migrationDueDateFormatted = ""
		}

		if parameter == "" {
			errorText := "DB column PARAMETER is mandatory"
			err = errors.New(errorText)
			log.Error(errorText + " - migration run aborted")
			return nil, migrationRunId, err
		}

		scheduledRun.Threads = threads
		scheduledRun.MigrationDueDate = migrationDueDateFormatted
		scheduledRun.Parameter = parameter
		scheduledRun.MigrationRunId = migrationRunId
		scheduledRun.UseListOfMPs = useListOfMPs
		scheduledRun.PeriodFromDate = periodFromDate
		scheduledRun.PeriodToDate = periodToDate
		scheduledRuns = append(scheduledRuns, scheduledRun)
	}

	log.Info("Time since SchedulerWorker started: ", time.Since(timer))

	if len(scheduledRuns) == 1 {
		//If all other statuses != "RUN" -> continue
		return &scheduledRuns[0], migrationRunId, nil //TODO Verify that it's correct to expect only one scheduled run
	} else if len(scheduledRuns) == 0 {
		if err != nil {
			//log in file why the export is not started
			log.Error("Something went wrong - ", err)
			return nil, -1, err
		} else {
			log.Debug("No scheduled runs found")
			return nil, -1, nil
		}
	} else { //TODO What to do if there are more than one scheduled run?
		log.Debug("Too many scheduled runs found")
		return nil, -1, nil
	}
}

//renameFiles changes the extension of the files in the given folder
func renameFiles(folderName string, oldExtension string, newExtension string) ([]string, error) {
	var filenames []string

	files, err := ioutil.ReadDir(folderName + "/")
	if err != nil {
		log.Error(err)
		return nil, err
	}
	//Find all .tmp files
	for _, file := range files {
		if !file.IsDir() && filepath.Ext(file.Name()) == oldExtension {
			//Rename to .json
			filename := strings.TrimSuffix(file.Name(), filepath.Ext(file.Name()))
			err := os.Rename(folderName+"/"+file.Name(), folderName+"/"+filename+newExtension)
			if err != nil {
				log.Error(err)
			} else {
				filenames = append(filenames, filename+newExtension)
				log.Debug("JSON file created: ", filename+newExtension)
			}
		}
	}
	return filenames, nil
}

//RemoveFiles removes the files with the given extension in the given folder
func RemoveFiles(folderName string, extension string) error {
	files, err := ioutil.ReadDir(folderName + "/")
	if err != nil {
		log.Error(err)
		return err
	}
	//Find all .tmp files
	for _, file := range files {
		if !file.IsDir() && filepath.Ext(file.Name()) == extension {
			//Remove
			err := os.Remove(folderName + "/" + file.Name())
			if err != nil {
				log.Error(err)
				return err
			}
		}
	}
	return nil
}

func getItemFromFileName(fileName string) string {
	var name, tmpstr string

	i := strings.Index(fileName, "_")
	if i > -1 {
		tmpstr = fileName[i+1:]

		i := strings.Index(tmpstr, "_")
		if i > -1 {
			tmpstr = tmpstr[i+1:]

			i := strings.Index(tmpstr, "_")
			if i > -1 {
				name = tmpstr[:i]
			}
		}
	}
	return name
}

func bool2intstring(b bool) string {
	if b {
		return "1"
	}
	return "0"
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

type metaInfo struct {
	meteringPointId       string
	transactionIdCountActual int
	transactionIdCountHist int
	sumActualReadingValues  float64
}

type result struct {
	mainErr     error
	returnValue bool
	metaInfo    map[string]metaInfo
}
