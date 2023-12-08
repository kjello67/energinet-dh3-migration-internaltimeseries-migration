package main

import (
	"flag"
	"fmt"
	"os"
	"time"
	"timeseries-migration/config"
	"timeseries-migration/models"
	"timeseries-migration/processor"
	"timeseries-migration/repository"

	log "github.com/sirupsen/logrus"
)

var (
	sha1ver   string // sha1 revision used to build the program
	buildTime string // when the executable was built
	version   string // custom version number of the program
)

func main() {

	//Store the current time before running the program in order to track execution time
	timer := time.Now()

	// Create the log file if it doesn't exist. Append to it if it already exists.
	logFile := initLogger()

	log.Info("version = ", version)
	log.Info("buildTime = ", buildTime)
	log.Info("sha1Version = ", sha1ver)

	//The environment is given as a parameter (defaults to PROD)
	environment := getEnvironment()

	// Get the DB configurations for the given environment
	DBConfigurations := config.GetConfigDB(environment)
	log.Debug("Skip DB update? ", DBConfigurations.SKIP_DB_UPDATE)

	//Store the error message to log in the DB (if any)
	errorMessage := ""
	migrationRunId := -1

	// Find the DB connection strings
	connectionStringData, connectionStringLog, validatedOK := getConnectionStrings(DBConfigurations)
	if !validatedOK {
		return
	}
	var dbConnectionString = flag.String("log", connectionStringData, "Database connection string to the data server.")
	var logConnectionString = flag.String("logDb", connectionStringLog, "Database connection string to the log server.")

	repo, err := repository.NewRepository(*dbConnectionString, *logConnectionString)
	repo.InitProgressTableSQLs()
	defer repo.Close()
	if err != nil {
		//Only log in file as the DB is not available
		log.Fatal(err)
	} else {
		//Check if there is a scheduled run
		var scheduledRun *models.ScheduledRun
		if !DBConfigurations.SKIP_DB_UPDATE && config.GetScheduledRunFromMigrationTable() {
			scheduledRun, migrationRunId, err = repo.SchedulerWorker()
			if err != nil {
				log.Fatal(err)
			}
		} else {
			migrationRunId = 119
			scheduledRun = new(models.ScheduledRun)
			scheduledRun.UseListOfMPs = true
			scheduledRun.MigrationRunId = migrationRunId
			scheduledRun.Threads = 1
			scheduledRun.Parameter = "PRE07"
			//scheduledRun.PeriodFromDate = time.Date(2015, 04, 30, 22, 0, 0, 0, time.UTC)
			//scheduledRun.PeriodToDate = time.Date(2015, 05, 31, 22, 0, 0, 0, time.UTC)
			scheduledRun.PeriodFromDate = time.Date(2015, 12, 31, 23, 0, 0, 0, time.UTC)
			scheduledRun.PeriodToDate = time.Date(2023, 10, 20, 00, 0, 0, 0, time.UTC)
		}

		// Everything OK so far
		for scheduledRun != nil && err == nil {
			//Get the configurations from the file with prefix stored in the DB (field PARAMETER)
			configurations := config.GetConfig(scheduledRun.Parameter)

			//Setup input parameters from the configurations (some from the DB and some from the configuration file)
			nWorkers := scheduledRun.Threads                    // The number of workers. Default value is 6.
			sqlFlag := scheduledRun.UseListOfMPs                // A flag to decide whether or not the SQL in the configuration file is going to be used.
			sqlItemCount := configurations.SQL_ITEM_COUNT       // The SQL to fetch the number of items to be migrated.
			sqlItemIds := configurations.SQL_ITEM_ID            // The SQL to fetch the items to be migrated.
			nWorkload := 1                                      // The number of items each worker will process per database request.
			fileLocation := configurations.FILE_LOCATION        // Where to store the exported JSON logFile.
			numberOfFilesToRename := configurations.RENAME_BULK // How many tmp files to rename to json at once. Default value is 1.
			if numberOfFilesToRename == 0 {
				numberOfFilesToRename = 1
			}
			flag.Parse()

			//Update the status to running
			if !DBConfigurations.SKIP_DB_UPDATE && config.GetScheduledRunFromMigrationTable() {
				err = repo.SetSQLUpdateStatusToRunning(scheduledRun.MigrationRunId)
			}

			if err != nil {
				log.Error(err)
				errorMessage = err.Error()
			} else {
				//Cleanup - remove all .tmp files in the out folder before starting the migration
				err = processor.RemoveFiles(fileLocation, config.GetTmpExtension())
				if err != nil {
					log.Error(err)
					errorMessage = err.Error()
				} else {
					//Call the function that will extract the grid area and write the time series to files in the json format
					ok, err := processor.MigrateTimeSeries(nWorkers, nWorkload, repo, fileLocation, sqlFlag, sqlItemCount, sqlItemIds, scheduledRun, DBConfigurations.SKIP_DB_UPDATE, &numberOfFilesToRename)
					if err != nil {
						log.Error(err)
						errorMessage = err.Error()
					} else if ok {
						log.Debug("Time series are migrated successfully. Updating status in DB.")

						if !DBConfigurations.SKIP_DB_UPDATE && config.GetScheduledRunFromMigrationTable() {
							//Update status to finished
							err = repo.SetSQLUpdateStatusToFinished(scheduledRun.MigrationRunId)
							if err != nil {
								log.Error(err)
								errorMessage = err.Error()
							}
						}
					}
				}
			}
			//Update status to error
			if !DBConfigurations.SKIP_DB_UPDATE && config.GetScheduledRunFromMigrationTable() && errorMessage != "" && migrationRunId != -1 {
				log.Debug("Time series are NOT migrated successfully. Updating status in DB.")
				err := repo.SetSQLUpdateStatusToError(migrationRunId, errorMessage)
				if err != nil {
					log.Error(err)
				}
			}
			scheduledRun = nil

			// Check if there are more exports to be done
			if !DBConfigurations.SKIP_DB_UPDATE && config.GetScheduledRunFromMigrationTable() {
				scheduledRun, migrationRunId, err = repo.SchedulerWorker()
				if err != nil {
					log.Fatal(err)
				}
			} else {
				scheduledRun = nil
			}
		}
	}

	//Don't forget to close the log file
	defer logFile.Close()

	//Print the time it took to run the program
	log.Info(" Execution time: ", time.Since(timer))
}

func getConnectionStrings(DBConfigurations config.Configuration) (string, string, bool) {
	var connectionStringData, connectionStringLog string
	validatedOK := true

	if DBConfigurations.DB_USERNAME == "" {
		log.Error("DB_USERNAME must be specified in the configuration file")
		return "", "", false
	}
	if DBConfigurations.DB_PASSWORD == "" {
		log.Error("DB_PASSWORD must be specified in the configuration file")
		return "", "", false
	}
	if DBConfigurations.LOG_USERNAME == "" {
		log.Error("LOG_USERNAME must be specified in the configuration file")
		return "", "", false
	}
	if DBConfigurations.LOG_PASSWORD == "" {
		log.Error("LOG_PASSWORD must be specified in the configuration file")
		return "", "", false
	}

	if DBConfigurations.DB_ALIAS != "" && DBConfigurations.DB_HOST != "" {
		log.Error("DB_ALIAS and DB_HOST cannot both be specified in the configuration file")
		return "", "", false
	}

	if DBConfigurations.LOG_ALIAS != "" && DBConfigurations.LOG_HOST != "" {
		log.Error("LOG_ALIAS and LOG_HOST cannot both be specified in the configuration file")
		return "", "", false
	}

	if DBConfigurations.DB_ALIAS != "" {
		connectionStringData = DBConfigurations.DB_USERNAME + "/" + DBConfigurations.DB_PASSWORD + "@" + DBConfigurations.DB_ALIAS
	} else if DBConfigurations.DB_HOST != "" && DBConfigurations.DB_PORT != "" && DBConfigurations.DB_SID != "" {
		connectionStringData = DBConfigurations.DB_USERNAME + "/" + DBConfigurations.DB_PASSWORD + "@//" + DBConfigurations.DB_HOST + ":" + DBConfigurations.DB_PORT + "/" + DBConfigurations.DB_SID
	}
	if DBConfigurations.LOG_ALIAS != "" {
		connectionStringLog = DBConfigurations.LOG_USERNAME + "/" + DBConfigurations.LOG_PASSWORD + "@" + DBConfigurations.LOG_ALIAS
	} else if DBConfigurations.LOG_HOST != "" && DBConfigurations.LOG_PORT != "" && DBConfigurations.LOG_SID != "" {
		connectionStringLog = DBConfigurations.LOG_USERNAME + "/" + DBConfigurations.LOG_PASSWORD + "@//" + DBConfigurations.LOG_HOST + ":" + DBConfigurations.LOG_PORT + "/" + DBConfigurations.LOG_SID
	}
	if connectionStringData == "" {
		log.Error("DB_ALIAS or DB_HOST+DB_PORT+DB_SID must be specified in the configuration file")
		validatedOK = false
	}
	if connectionStringLog == "" {
		log.Error("LOG_ALIAS or LOG_HOST+LOG_PORT+LOG_SID must be specified in the configuration file")
		validatedOK = false
	}
	return connectionStringData, connectionStringLog, validatedOK
}

func getEnvironment() string {
	environment := config.GetDefaultEnvironment()
	if len(os.Args) > 1 {
		environment = os.Args[1]
	}
	return environment
}

func initLogger() *os.File {
	logFile, err := os.OpenFile(config.GetLogFileName(), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	log.SetFormatter(&log.TextFormatter{QuoteEmptyFields: true, ForceColors: true, FullTimestamp: true})
	log.SetReportCaller(true)
	log.SetOutput(logFile)
	log.SetLevel(log.DebugLevel)
	if err != nil {
		// Cannot open log file. Logging to stderr
		fmt.Println(err)
	} else {
		log.SetOutput(logFile)
	}

	return logFile
}
