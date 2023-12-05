package processor

import (
	"encoding/json"
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
	"timeseries-migration/repository"
	"timeseries-migration/utils"
)

// MigrateTimeSeries extracts metering points from the database and writes the time series to json files for migration to DHUB3
func MigrateTimeSeries(nWorkers, nWorkload int, repo repository.Repository, fileLocation string, sqlFlag bool, sqlItemCount, sqlItemIds string, run *models.ScheduledRun, skipDBUpdate bool, numberOfFilesToRename *int) (bool, error) {
	var err error

	fromTimeFormatted := run.PeriodFromDate.Format(config.GetExportDateLayout())
	toTimeFormatted := run.PeriodToDate.Format(config.GetExportDateLayout())

	var meteringPointCount *int
	//Find the number of metering points to migrate
	meteringPointCount, err = repo.GetNumberOfMeteringPoints(sqlFlag, sqlItemCount)
	if err != nil {
		return false, err
	}
	log.Info("Found " + strconv.Itoa(*meteringPointCount) + " metering points to migrate using " + strconv.Itoa(nWorkers) + " threads")

	//Create the channel that the workers will fetch meteringPoints from
	meteringPoints := make(chan []string, *meteringPointCount)

	//Populate the channel with the meteringPoints to migrate
	err = repo.GetMeteringPoints(meteringPoints, nWorkload, sqlFlag, sqlItemIds)
	if err != nil {
		return false, err
	}

	//Close the channel for new entries
	close(meteringPoints)

	//WaitGroup used to ensure the function doesn't end before all the workers are finished
	wg := sync.WaitGroup{}

	// We just want to return one of the errors from the worker threads (if any)
	resultChannel := make(chan result, nWorkers)

	//Create a number of workers equal to the number in nWorkers
	for worker := 0; worker < nWorkers; worker++ {
		wg.Add(1)
		go func() {
			//Create a worker
			metaInfo, err := TimeSeriesWorker(repo, meteringPoints, run.PeriodToDate, fromTimeFormatted, toTimeFormatted, fileLocation, run.MigrationRunId, skipDBUpdate, numberOfFilesToRename)

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
					err = repo.ExecSqlstmtTimeSeriesFound(run.MigrationRunId, fromTimeFormatted, toTimeFormatted, filename, fileDetails, info.meteringPointId)
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



//TimeSeriesWorker reads the metering points channel and writes the time series for each metering point in a separate json file
func TimeSeriesWorker(repo repository.Repository, items <-chan []string, runTo time.Time, fromTimeFormatted, toTimeFormatted, fileLocation string, migrationRunId int, skipDBUpdate bool, numberOfFilesToRename *int) (map[string]metaInfo, error) {
	timer := time.Now()
	var fileNames []string
	timeSeriesInfo := map[string]metaInfo{}
	fromTime, _ := time.Parse(config.GetExportDateLayout(), fromTimeFormatted)
	toTime, _ := time.Parse(config.GetExportDateLayout(), toTimeFormatted)

	//For each metering point read from the metering points channel
	for itemSlice := range items {
		for _, itemId := range itemSlice {
			//The list of time series that will be written to the file
			data, metaInfo, migrate, err := getTimeSeriesList(itemId, fromTime, toTime, runTo, repo)

			if migrate && len(data.TimeSeries) > 0{
				if metaInfo != nil {
					timeSeriesInfo[itemId] = *metaInfo
				}

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
						timeSeriesInfo[itemId].processedFromTime.Format(config.GetFileDateLayout()) +
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

				fileNames = append(fileNames, fileName)
				if len(fileNames) == *numberOfFilesToRename {
					for i := 0; i < len(fileNames); i++ {
						//Run the prepared SQL query that inserts to the progress table
						fileName = fileNames[i]
						strId := getItemFromFileName(fileName)

						//Rename .tmp to .json in the folder
						_ , err = renameFile(fileName, config.GetTmpExtension(), config.GetFinalExtension())
						if err == nil {
							if config.GetScheduledRunFromMigrationTable() {
								//Insert the filenames in the progress table
								//Run the prepared SQL query that inserts to the progress table
								fileDetails := fmt.Sprintf("transaction_count_actual_time_series:%d;transaction_count_historical_time_series=%d;sum_actual_reading_values=%d", timeSeriesInfo[strId].transactionIdCountActual, timeSeriesInfo[strId].transactionIdCountHist, int(timeSeriesInfo[strId].sumActualReadingValues))

								if !skipDBUpdate && config.GetScheduledRunFromMigrationTable() {
									err = repo.ExecSqlstmtTimeSeriesFound(migrationRunId, fromTimeFormatted, toTimeFormatted, fileNameWithoutExtension + config.GetFinalExtension(), fileDetails, strId)
									if err != nil {
										log.Error(err)
										return nil, err
									}
								}
							}
						} else {
							return nil, err
						}
					}
					fileNames = nil
				}
			} else {
				//No time series found for MP
				log.Debug("No time series found for meterpoint ", itemId)

				if !skipDBUpdate && config.GetScheduledRunFromMigrationTable() {
					filename := "N/A"
					fileDetails := fmt.Sprintf("transaction_count_actual_time_series:%d;transaction_count_historical_time_series=%d;sum_actual_reading_values=%d", 0, 0, 0)

					//Run the prepared SQL query that inserts to the progress table
					err := repo.ExecSqlstmtNoTimeSeriesFound(migrationRunId, fromTimeFormatted, toTimeFormatted, filename, fileDetails, itemId)
					if err != nil {
						log.Error(err)
						return nil, err
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



func getTimeSeriesList(meteringPointId string, processedFromTime, processedUntilTime, runTo time.Time, repo repository.Repository) (models.Data, *metaInfo, bool, error) {
	//The list of time series that will be written to the file
	var meteringPointData models.MeteringPointData
	meteringPointData.MeteringPointId = meteringPointId
	var masterData []models.Masterdata
	var data models.Data

	PST, err := time.LoadLocation(config.GetTimeLocation())
	if err != nil {
		log.Error(err)
		return data, nil, false, err
	}

	UTC, err := time.LoadLocation("UTC")
	if err != nil {
		log.Error(err)
		return data, nil, false, err
	}

	masterData, err = repo.GetMasterData(meteringPointId, PST)
	if err != nil {
		log.Error(err)
		return data, nil, false, err
	}

	meteringPointData.MasterData = masterData
	var timeSeriesList []models.TimeSeriesData
	data.MeteringPointData = meteringPointData
	data.TimeSeries = timeSeriesList

	if config.GetScheduledRunFromMigrationTable() {
		newProcessedFromTime, migrate, err := repo.FindDataMigrationExportedPeriod(meteringPointId, processedFromTime, runTo)
		if err != nil {
			log.Error(err)
			return data, nil, false, err
		}

		processedFromTime = newProcessedFromTime

		if !migrate {
			log.Info("Skipping meteringpoint " + meteringPointId + " as it has already been migrated for the the period")
			return data, nil, false, err
		}
	}

	rows, err := repo.ExecSqlstmtSelectTimesSeries(meteringPointId, processedFromTime, processedUntilTime)
	if err != nil {
		log.Error(err)
		return data, nil, true, err
	}
	defer rows.Close()

	//Variables to hold the time series
	var timeSeriesData models.TimeSeriesData
	var timeSerieValues []models.TimeSeriesValue
	var transactionId, messageId, readReason utils.NullString
	var transactionIdStr, messageIdStr, readReasonStr string
	var historicalFlag, resolution,  unit string
	var prevResolution  string

	var validFromDate, validToDate, transactionInsertDate utils.NullTime
	var prevValidFromDate, prevValidToDate utils.NullTime
	var validFromDateFormatted, validToDateFormatted, transactionInsertDateFormatted string
	var timeSeriesValue models.TimeSeriesValue
	var position, serieStatus int
	var quantity float64
	var quality string
	var valueData string
	//var readingTime NullTime

	transactionIdCountActual := map[string]int{}
	transactionIdCountHist := map[string]int{}
	var sumActualReadingValues float64
	sumActualReadingValues = 0.0

	//Loop through the result from the executed SQL query
	for rows.Next() {

		//Store the values from the current line in the result to local variables
		rows.Scan(
			&meteringPointId,
			&transactionId,
			&messageId,
			&validFromDate,
			&validToDate,
			&transactionInsertDate,
			&historicalFlag,
			&resolution,
			&unit,
			&valueData,
			&serieStatus,
			&readReason)

		skipThis := false

		if  prevResolution == "15" && resolution == "60" {
			if validFromDate.Time.Before(prevValidToDate.Time) && validToDate.Time.After(prevValidFromDate.Time){
				skipThis = true
			}
     	}
		if !skipThis {
			validFromDateFormatted, err = utils.FormatDate(UTC, validFromDate, "")
			if err != nil {
				log.Error(err)
				return data, nil, true, err
			}
			validToDateFormatted, err = utils.FormatDate(UTC, validToDate, resolution)
			if err != nil {
				log.Error(err)
				return data, nil, true, err
			}
			transactionInsertDateFormatted, err = utils.FormatDate(UTC, transactionInsertDate, "")
			if err != nil {
				log.Error(err)
				return data, nil, true, err
			}

			validFromDateNoTimeZone, err := time.Parse(config.GetExportDateLayout(), validFromDate.Time.Format(config.GetExportDateLayout()))
			if err != nil {
				log.Error(err)
				return data, nil, false, err
			}


//			validToDateNoTimeZone, _ := time.Parse(config.GetJSONDateLayoutLong(), validToDateFormatted)

/*			if timeSeriesValue.Position > position || (timeSeriesData.HistoricalFlag != "" && timeSeriesData.HistoricalFlag != historicalFlag) {
				timeSeriesData.TimeSeriesValues = timeSerieValues
				timeSerieValues = nil
				timeSeriesList = append(timeSeriesList, timeSeriesData)
			}
*/
			valueData = strings.ReplaceAll(valueData, "<E>", "" )
			valueData = strings.ReplaceAll(valueData, "</E>", "" )
			values := strings.Split(valueData, ";")

			var readingTime time.Time
			allQM := true
			for i, s := range values {
				position = i+1
				valueDetails := strings.Split(s, "|")
				intResolution, _ := strconv.Atoi(resolution)
				if len(valueDetails) >= 3 {
					if  intResolution == 15 || intResolution == 60 {
						readingTime, err = time.Parse(config.GetExportDateLayout(), valueDetails[0])
						if err != nil {
							log.Error(err)
							return data, nil, false, err
						}
						duration := readingTime.Sub(validFromDateNoTimeZone)
						position = int(duration.Minutes()) / intResolution + 1
					} else { // monthly timeseries
						position, err = strconv.Atoi(valueDetails[0])
						if err != nil {
							log.Error(err)
							return data, nil, false, err
						}
					}

					quantity, err = strconv.ParseFloat(strings.ReplaceAll(valueDetails[1], ",", "."), 64)

					if  err != nil {
						log.Error(meteringPointId + ": ", err)
					}

					quality = valueDetails[2]
					timeSeriesValue.Position = position

					timeSeriesValue.Quality = quality
					qm := quality == "QM"

					if !qm {
						timeSeriesValue.Quantity = quantity
						sumActualReadingValues += quantity
						allQM = false
					} else {
						timeSeriesValue.Quantity = 0
					}
					sumActualReadingValues += quantity
					timeSeriesValue.Quality = quality
					timeSerieValues = append(timeSerieValues, timeSeriesValue)
				}
			}
/*
			if resolution == "15" || resolution == "60" {
				deltaTime := readingTime.Sub(validToDate.Time)

				if deltaTime.Minutes() != 0 {
					validToDate.Time = validToDateNoTimeZone.Add(deltaTime)
					validToDateFormatted, err = formatDate(UTC, validToDate, resolution)
					if err != nil {
						log.Error(err)
						return data, nil, true, err
					}
				}
			}

			if resolution == "15" || resolution == "60" {
				readingTimeEnd := readingTime.Add(time.Hour )
				if  resolution == "15" {
					readingTimeEnd = readingTime.Add(time.Minute * 15 )
				}

				// Should be the same but in some rare occasions it is not
				if !readingTimeEnd.Equal(validToDate.Time) {
					deltaTime :=  readingTimeEnd.Sub(validToDate.Time)

					validToDate.Time = validToDate.Time.Add(deltaTime)
					validToDateFormatted, err = formatDate(UTC, validToDate, resolution)
					if err != nil {
						log.Error(err)
						return data, nil, true, err
					}
				}
			}
*/
			transactionIdStr = formatNullString(transactionId)
			messageIdStr = formatNullString(messageId)
			readReasonStr = formatNullString(readReason)

			timeSeriesData.TransactionId = transactionIdStr
			timeSeriesData.MessageId = messageIdStr
			timeSeriesData.ValidFromDate = validFromDateFormatted
			timeSeriesData.ValidToDate = validToDateFormatted
			timeSeriesData.TransactionInsertDate = transactionInsertDateFormatted
			timeSeriesData.HistoricalFlag = historicalFlag

			if historicalFlag == "N" {
				_, ok := transactionIdCountActual[transactionIdStr+transactionInsertDateFormatted]
				if !ok {
					transactionIdCountActual[transactionIdStr+transactionInsertDateFormatted] = 1
				}
			} else {
				_, ok := transactionIdCountHist[transactionIdStr+transactionInsertDateFormatted]
				if !ok {
					transactionIdCountHist[transactionIdStr+transactionInsertDateFormatted] = 1
				}
			}

			if readReasonStr == "" && allQM && transactionIdStr == "" {
				readReasonStr = "CAN"
			}

			timeSeriesData.Resolution = resolution
			timeSeriesData.Unit = unit
			timeSeriesData.Status = serieStatus
			timeSeriesData.ReadReason = readReasonStr

			prevValidFromDate = validFromDate
			prevValidToDate = validToDate
			prevResolution = resolution

			timeSeriesData.TimeSeriesValues = timeSerieValues
			timeSerieValues = nil
			timeSeriesList = append(timeSeriesList, timeSeriesData)
		}
	}

	data.TimeSeries = timeSeriesList

	if len(timeSeriesList) > 0 {
		metaInfo := metaInfo{meteringPointId: meteringPointId, processedFromTime: processedFromTime, transactionIdCountActual: len(transactionIdCountActual), transactionIdCountHist: len(transactionIdCountHist), sumActualReadingValues: sumActualReadingValues}
		return data, &metaInfo, true, err
	} else {
		return data, nil, true, err
	}
}

func formatNullString(nullString utils.NullString) string {
	var formattedString string
	if nullString.Valid {
		formattedString = nullString.String
	} else {
		formattedString = ""
	}
	return formattedString
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

//renameFile changes the extension of the file
func renameFile(originalFileName string, oldExtension string, newExtension string) (string, error) {
	filename := strings.TrimSuffix(originalFileName, filepath.Ext(originalFileName))
	err := os.Rename(filename+oldExtension, filename+newExtension)
	if err != nil {
		log.Error(err)
	} else {
		log.Debug("JSON file created: ", filename+newExtension)
	}
	return filename+newExtension, nil
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


type metaInfo struct {
	meteringPointId          string
	processedFromTime        time.Time
	transactionIdCountActual int
	transactionIdCountHist   int
	sumActualReadingValues   float64
}

type result struct {
	mainErr     error
	returnValue bool
	metaInfo    map[string]metaInfo
}
