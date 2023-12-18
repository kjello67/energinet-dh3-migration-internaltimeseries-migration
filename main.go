package main

import (
	"flag"
	"fmt"
	log "github.com/sirupsen/logrus"
	"os"
	"strconv"
	"time"
	"timeseries-migration/config"
	"timeseries-migration/logger"
	"timeseries-migration/models"
	"timeseries-migration/processor"
	"timeseries-migration/repository"
	"timeseries-migration/utils"
)

var (
	sha1ver   string // sha1 revision used to build the program
	buildTime string // when the executable was built
	version   string // custom version number of the program

	flgVersion bool
)

func main() {

	parseCmdLineFlags()

	//Store the current time before running the program in order to track execution time
	timer := time.Now()

	//The environment is given as a parameter (defaults to PROD)
	environment := getEnvironment()

	//Get the configurations for the given environment
	configurations := config.GetConfig(environment)

	// Create the log file if it doesn't exist. Append to it if it already exists.
	logFileLogger, err := logger.NewLogger(configurations.MAX_LOGFILE_SIZE)

	if configurations.DEBUG_LOGGING {
		log.SetLevel(log.DebugLevel)
	}

	// Get the DB configurations for the given environment
	DBConfigurations := config.GetConfigDB(environment)

	logFileLogger.Info("Using configurations from config files with prefix: " + environment)
	logFileLogger.Info("version = " + version)
	logFileLogger.Info("buildTime = " + buildTime)
	logFileLogger.Info("sha1Version = " + sha1ver)
	logFileLogger.Debug("Skip DB update? " + strconv.FormatBool(DBConfigurations.SKIP_DB_UPDATE))

	//Store the error message to log in the DB (if any)
	errorMessage := ""
	migrationRunId := -1

	// Find the DB connection strings
	connectionStringData, connectionStringLog, validatedOK := getConnectionStrings(DBConfigurations, &logFileLogger)
	if !validatedOK {
		return
	}
	var dbConnectionString = flag.String("log", connectionStringData, "Database connection string to the data server.")
	var logConnectionString = flag.String("logDb", connectionStringLog, "Database connection string to the log server.")

	repo, err := repository.NewRepository(*dbConnectionString, *logConnectionString, &logFileLogger)
	repo.InitProgressTableSQLs()
	defer repo.Close()
	if err != nil {
		//Only log in file as the DB is not available
		logFileLogger.Fatal(err)
	} else {
		//Check if there is a scheduled run
		var scheduledRun *models.ScheduledRun
		if !DBConfigurations.SKIP_DB_UPDATE && config.GetScheduledRunFromMigrationTable() {
			scheduledRun, migrationRunId, err = repo.SchedulerWorker()
			if err != nil {
				logFileLogger.Fatal(err)
			}
		} else {
			migrationRunId = 119
			scheduledRun = new(models.ScheduledRun)
			scheduledRun.UseListOfMPs = true
			scheduledRun.MigrationRunId = migrationRunId
			scheduledRun.Threads = 1
			scheduledRun.PeriodFromDate = time.Date(2015, 12, 31, 23, 0, 0, 0, time.UTC)
			scheduledRun.PeriodToDate = time.Date(2023, 10, 20, 00, 0, 0, 0, time.UTC)
		}

		statusOfPrevRun := config.GetStatusFinished()
		// Everything OK so far
		for scheduledRun != nil && err == nil {

			logFileLogger.Info("Starting run id " + strconv.Itoa(scheduledRun.MigrationRunId))

			//Setup input parameters from the configurations (some from the DB and some from the configuration file)
			nWorkers := scheduledRun.Threads                    // The number of workers. Default value is 6.
			sqlFlag := scheduledRun.UseListOfMPs                // A flag to decide whether or not the SQL in the configuration file is going to be used.
			sqlItemCount := configurations.SQL_ITEM_COUNT       // The SQL to fetch the number of items to be migrated.
			sqlItemIds := configurations.SQL_ITEM_ID            // The SQL to fetch the items to be migrated.
			nWorkload := 1                                      // The number of items each worker will process per database request.
			fileLocation := configurations.FILE_LOCATION        // Where to store the exported JSON file.
			numberOfFilesToRename := configurations.RENAME_BULK // How many tmp files to rename to json at once. Default value is 1.
			if numberOfFilesToRename == 0 {
				numberOfFilesToRename = 1
			}
			flag.Parse()

			//Update the status to running
			if !DBConfigurations.SKIP_DB_UPDATE && config.GetScheduledRunFromMigrationTable() {
				err = repo.SetSQLUpdateStatusToRunning(scheduledRun.MigrationRunId)
				statusOfPrevRun = config.GetStatusRunning()
			}

			if err != nil {
				logFileLogger.Error(err)
				errorMessage = err.Error()
			} else {
				//Cleanup - remove all .tmp files in the out folder before starting the migration
				err = processor.RemoveFiles(fileLocation, config.GetTmpExtension(), &logFileLogger)
				if err != nil {
					logFileLogger.Error(err)
					errorMessage = err.Error()
				} else {
					//Call the function that will extract the grid area and write the time series to files in the json format
					ok, err := processor.MigrateTimeSeries(nWorkers, nWorkload, repo, fileLocation, sqlFlag, sqlItemCount, sqlItemIds, scheduledRun, DBConfigurations.SKIP_DB_UPDATE, &numberOfFilesToRename)
					if err != nil {
						logFileLogger.Error(err)
						errorMessage = err.Error()
					} else if ok {
						logFileLogger.Debug("Time series are migrated successfully. Updating status in DB.")

						if !DBConfigurations.SKIP_DB_UPDATE && config.GetScheduledRunFromMigrationTable() {
							//Update status to finished
							err = repo.SetSQLUpdateStatusToFinished(scheduledRun.MigrationRunId)
							if err != nil {
								logFileLogger.Error(err)
								errorMessage = err.Error()
							}
							statusOfPrevRun = config.GetStatusFinished()
						}
					}
				}
			}
			//Update status to error
			if !DBConfigurations.SKIP_DB_UPDATE && config.GetScheduledRunFromMigrationTable() && errorMessage != "" && migrationRunId != -1 {
				logFileLogger.Debug("Time series are NOT migrated successfully. Updating status in DB.")
				err := repo.SetSQLUpdateStatusToError(migrationRunId, errorMessage)
				if err != nil {
					logFileLogger.Error(err)
				}
				statusOfPrevRun = config.GetStatusError()
			}
			logFileLogger.Info("Finished run id " + strconv.Itoa(scheduledRun.MigrationRunId))
			utils.PrintMemUsage(&logFileLogger)
			scheduledRun = nil

			if statusOfPrevRun == config.GetStatusFinished() {
				time.Sleep(2 * time.Second)

				// Check if there are more exports to be done
				if !DBConfigurations.SKIP_DB_UPDATE && config.GetScheduledRunFromMigrationTable() {
					scheduledRun, migrationRunId, err = repo.SchedulerWorker()
					if err != nil {
						logFileLogger.Fatal(err)
					}
				} else {
					scheduledRun = nil
				}
			}
		}
	}

	//Print the time it took to run the program
	logFileLogger.Info(" Execution time: " + time.Since(timer).String())
}

func getConnectionStrings(DBConfigurations config.Configuration, logFileLogger *logger.Logger) (string, string, bool) {
	var connectionStringData, connectionStringLog string
	validatedOK := true

	if DBConfigurations.DB_USERNAME == "" {
		(*logFileLogger).ErrorWithText("DB_USERNAME must be specified in the configuration file")
		return "", "", false
	}
	if DBConfigurations.DB_PASSWORD == "" {
		(*logFileLogger).ErrorWithText("DB_PASSWORD must be specified in the configuration file")
		return "", "", false
	}
	if DBConfigurations.LOG_USERNAME == "" {
		(*logFileLogger).ErrorWithText("LOG_USERNAME must be specified in the configuration file")
		return "", "", false
	}
	if DBConfigurations.LOG_PASSWORD == "" {
		(*logFileLogger).ErrorWithText("LOG_PASSWORD must be specified in the configuration file")
		return "", "", false
	}

	if DBConfigurations.DB_ALIAS != "" && DBConfigurations.DB_HOST != "" {
		(*logFileLogger).ErrorWithText("DB_ALIAS and DB_HOST cannot both be specified in the configuration file")
		return "", "", false
	}

	if DBConfigurations.LOG_ALIAS != "" && DBConfigurations.LOG_HOST != "" {
		(*logFileLogger).ErrorWithText("LOG_ALIAS and LOG_HOST cannot both be specified in the configuration file")
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
		(*logFileLogger).ErrorWithText("DB_ALIAS or DB_HOST+DB_PORT+DB_SID must be specified in the configuration file")
		validatedOK = false
	}
	if connectionStringLog == "" {
		(*logFileLogger).ErrorWithText("LOG_ALIAS or LOG_HOST+LOG_PORT+LOG_SID must be specified in the configuration file")
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

func parseCmdLineFlags() {
	flag.BoolVar(&flgVersion, "version", false, "if true, print version and exit")
	flag.Parse()
	if flgVersion {
		fmt.Printf("Version %s - build on %s from sha1 %s\n", version, buildTime, sha1ver)
		os.Exit(0)
	}
}
