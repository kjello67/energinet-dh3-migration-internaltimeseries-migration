package sqls

import (
	"strconv"
	"timeseries-migration/config"
)

//GetSQLSelectMasterData returns the SQL statement used to retrieve the master data
func GetSQLSelectMasterData() string {

	//Create the main body of the SQL statement
	sql :=
		`SELECT
    metering_point_id,
    grid_area,
    type_of_mp,
    valid_from_date,
    valid_to_date
FROM
    ccr_own.isc_metering_points    isc
WHERE
        isc.metering_point_id = :meteringPointId
    AND valid_from_date != nvl(valid_to_date, valid_from_date + 1)`

	return sql
}


//GetSQLSelectData returns the SQL statement used to retrieve the timeseries data
func GetSQLSelectData() string {

	//Create the main body of the SQL statement
	sql :=
		`with ts as (
select * from
		(
			SELECT   /*+ leading(m) use_nl(t) use_nl(v) use_nl(r) */
				m.OBJECTID                     as metering_point_id,
				r.transref                     as transaction_id,
				t.START_DATE                   as valid_from_date,
				t.END_DATE                     as valid_to_date,
				t.SERIES_TIMESTAMP             as inserted_timestamp,
				nvl2(t.hist_timestamp,'Y','N') as historical_flag, 
				t.RESOLUTION                   as resolution,        
				t.UNIT                         as unit,  
				round(months_between(v.READING_START_DATE, t.START_DATE) +1, 0)                  as position,
				v.READING_START_DATE           as READING_TIME,
				v.AMOUNT                       as quantity,
				decode (V.DATA_ORIGIN, 'M', 'E01', 'E', '56', 'C', '36', 'B','D01', '?', 'QM')   as quality
			from CHANGE.S_PERIODIC_SERIES_VALUE v,
				CHANGE.S_PERIODIC_TIMESERIES t,
				CHANGE.s_recipient r,
				READING.m_meterpoint m
			where t.PERIODIC_TIMESERIES_SEQNO = v.PERIODIC_TIMESERIES_SEQNO
			and m.MPOINT_SEQNO = t.UTVEKSLINGSOBJEKTNR
			and t.sender_ref = r.meldingsnr_data
			AND t.SERIES_TIMESTAMP >= TO_DATE('2016-04-01', 'YYYY-MM-DD') -- :fromDate
			AND t.SERIES_TIMESTAMP <  TO_DATE('2018-01-01', 'YYYY-MM-DD') -- :toDate
		union all		
			SELECT  /*+ leading(mis) use_nl(b) use_nl(sv) */
				MIS.SERIE_OBJECTID as metering_point_id,
				B.import_batch_id as transaction_id,
				MIS.MIN_READING_TIME as valid_from_date,
				MIS.MAX_READING_TIME as valid_to_date,
				MIS.SERIE_TIMESTAMP as inserted_timestamp,
				'N' as historical_flag,           
				to_char(MIS.RESOLUTION)   as resolution,         
				MIS.UNIT         as unit,     
				case  
				when mis.resolution = 60 then round(24*(SV.READING_TIME - MIS.MIN_READING_TIME)+1)
				when mis.resolution = 15 then round(96*(SV.READING_TIME - MIS.MIN_READING_TIME)+1)
				else 1 end as position,
				SV.READING_TIME,
				SV.READING_VALUE as quantity,
				decode (SV.DATA_ORIGIN, 'M', 'E01', 'E', '56', 'C', '36', 'B','D01', '?', 'QM')   as quality
			FROM READING.M_IMPORT_SERIE MIS,
				READING.M_SERIE_VALUE  SV,
				READING.M_BATCH        B
			WHERE MIS.BATCH_SEQNO = B.BATCH_SEQNO
			AND MIS.IMPORT_SERIE_SEQNO = SV.IMPORT_SERIE_SEQNO
			and MIS.SERIE_STATUS = '2'
			and not (mis.imp_resolution = 60 and mis.sender_ref like 'Calculated from 15 min values')
			AND MIS.SERIE_TIMESTAMP >= TO_DATE('2016-04-01', 'YYYY-MM-DD') -- :fromDate
			AND MIS.SERIE_TIMESTAMP <  TO_DATE('2018-01-01', 'YYYY-MM-DD') -- :toDate
			-- #13434 short discussion with Rune -  included all timeseries with reporting counter zero with exception of batch type ‘CA’
			AND B.batch_type != 'CA'
		union all    
			SELECT  /*+ leading(mis) use_nl(b) use_nl(sv) */
				MIS.SERIE_OBJECTID as metering_point_id,
				B.import_batch_id as transaction_id,
				MIS.MIN_READING_TIME as valid_from_date,
				MIS.MAX_READING_TIME as valid_to_date,
				MIS.SERIE_TIMESTAMP as inserted_timestamp,
				'Y' as historical_flag,           
				to_char(MIS.RESOLUTION)   as resolution,         
				MIS.UNIT         as unit,     
				case  
				when mis.resolution = 60 then round(24*(SV.READING_TIME - MIS.MIN_READING_TIME)+1)
				when mis.resolution = 15 then round(96*(SV.READING_TIME - MIS.MIN_READING_TIME)+1)
				else 1 end as position,
				SV.READING_TIME,
				SV.READING_VALUE as quantity,
				decode (SV.DATA_ORIGIN, 'M', 'E01', 'E', '56', 'C', '36', 'B','D01', '?', 'QM')   as quality
			FROM READING.M_IMPORT_SERIE MIS,
				READING.M_SERIE_VALUE_HIST  SV,
				READING.M_BATCH        B
			WHERE MIS.BATCH_SEQNO = B.BATCH_SEQNO
			AND MIS.IMPORT_SERIE_SEQNO = SV.OLD_SERIE_SEQNO 
			and MIS.SERIE_STATUS = '2'
			and not (mis.imp_resolution = 60 and mis.sender_ref like 'Calculated from 15 min values')
			AND MIS.SERIE_TIMESTAMP >= TO_DATE('2016-04-01', 'YYYY-MM-DD') -- :fromDate
			AND MIS.SERIE_TIMESTAMP <  TO_DATE('2018-01-01', 'YYYY-MM-DD') -- :toDate
			-- #13434 short discussion with Rune -  included all timeseries with reporting counter zero with exception of batch type ‘CA’
			AND B.batch_type != 'CA'
		)	
	)
    select * from ts
	where  metering_point_id = :meteringPointId
--	group by metering_point_id, transaction_id, valid_from_date, valid_to_date, inserted_timestamp, historical_flag, resolution, unit `
	//Set the order of the SQL statement
	sqlOrder := `   ORDER BY metering_point_id, valid_from_date asc, historical_flag,  position asc`

	return sql + sqlOrder
}

//GetSQLInsertFinishedTimeSeriesExportFile returns the SQL statement used to insert "data found""
func GetSQLInsertFinishedTimeSeriesExportFile() string {
	return " insert into " + config.GetExportProgressTableName() +
		" (MIGRATION_RUN_ID, MIGRATION_DOMAIN, EXPORTED_FROM_DATE, EXPORTED_TO_DATE, EXPORT_STATUS, DATA_FOUND, FILE_NAME, METERING_POINT_ID) " +
		" values (:migrationRunId, '" + config.GetDomain() + "', TO_DATE(:fromTimeDDMMYYYYHHMISS, 'DD.MM.YYYY HH24:MI:SS'), TO_DATE(:toTimeDDMMYYYYHHMISS, 'DD-MM-YYYY HH24:MI:SS'), '" + config.GetStatusFinished() + "', :dataFound, :fileName, :objectId)"
}

//GetSQLInsertNoDataFound returns the SQL statement used to
func GetSQLInsertNoDataFound() string {
	return " insert into " + config.GetExportProgressTableName() +
		" (MIGRATION_RUN_ID, METERING_POINT_ID, MIGRATION_DOMAIN, EXPORTED_FROM_DATE, EXPORTED_TO_DATE, EXPORT_STATUS, DATA_FOUND) " +
		" values (:migrationRunId, :objectId, '" + config.GetDomain() + "', TO_DATE(:fromTimeDDMMYYYYHHMISS, 'DD-MM-YYYY HH24:MI:SS'), TO_DATE(:toTimeDDMMYYYYHHMISS, 'DD-MM-YYYY HH24:MI:SS'), '" + config.GetStatusFinished() + "', :dataFound)"
}

//GetSQLUpdateStatusToRunning returns the SQL statement used to
func GetSQLUpdateStatusToRunning(migrationRunId int) string {
	return " update " + config.GetExportTableName() +
		" set MIGRATION_STATUS = '" + config.GetStatusRunning() + "' " +
		" , MIGRATION_START_DATE = sysdate " +
		" , MIGRATION_LOG_DETAILS = '" + config.GetMigrationDetailsWhenRunning() + "' " +
		" where MIGRATION_DOMAIN = '" + config.GetDomain() + "'" +
		" and MIGRATION_STATUS = '" + config.GetStatusNew() + "'" +
		" and MIGRATION_RUN_ID = " + strconv.Itoa(migrationRunId)
}

//GetSQLUpdateStatusToFinished returns the SQL statement used to update the status to "FIN"
func GetSQLUpdateStatusToFinished(migrationRunId int) string {
	return " update " + config.GetExportTableName() +
		" set MIGRATION_STATUS = '" + config.GetStatusFinished() + "' " +
		" , MIGRATION_FINISH_DATE = sysdate " +
		" , MIGRATION_LOG_DETAILS = '" + config.GetMigrationDetailsWhenFinished() + "' " +
		" where MIGRATION_DOMAIN = '" + config.GetDomain() + "'" +
		" and MIGRATION_STATUS = '" + config.GetStatusRunning() + "'" +
		" and MIGRATION_RUN_ID = " + strconv.Itoa(migrationRunId)
}

//GetSQLUpdateStatusToError returns the SQL statement used to update the status to "ERR"
func GetSQLUpdateStatusToError(migrationRunId int, errorMessage string) string {
	return " update " + config.GetExportTableName() +
		" set MIGRATION_STATUS = '" + config.GetStatusError() + "' " +
		" , MIGRATION_LOG_DETAILS = '" + errorMessage + "' " +
		" where MIGRATION_DOMAIN = '" + config.GetDomain() + "'" +
		" and MIGRATION_RUN_ID = " + strconv.Itoa(migrationRunId)
}

//GetSQLSelectNewRuns returns the SQL statement used to find scheduled runs
func GetSQLSelectNewRuns() string {
	return `SELECT
			MIGRATION_RUN_ID
			,THREADS
            ,MIGRATION_DUE_DATE
            ,PARAMETER
			,USE_LIST_OF_MP
			,USE_LIST_OF_OWNERS
			,USE_LIST_OF_GRID_AREAS
			,PERIOD_FROM_DATE
			,PERIOD_TO_DATE
			FROM
			(
			SELECT
			MIGRATION_RUN_ID
			,THREADS
            ,MIGRATION_DUE_DATE
            ,PARAMETER
			,USE_LIST_OF_MP
			,USE_LIST_OF_OWNERS
			,USE_LIST_OF_GRID_AREAS
			,PERIOD_FROM_DATE
			,PERIOD_TO_DATE
			FROM DMDH3_OWN.DATAMIGRATION_EXPORT e
			where MIGRATION_STATUS = 'NEW'
            and e.MIGRATION_DOMAIN = 'TimeSeries'
			and MIGRATION_DUE_DATE < sys_extract_utc(systimestamp)
			and not exists (select 1 from DMDH3_OWN.DATAMIGRATION_EXPORT de where de.MIGRATION_DOMAIN = e.MIGRATION_DOMAIN and de.MIGRATION_STATUS = 'RUN')
			AND EXISTS (SELECT 1 FROM DMDH3_OWN.DATAMIGRATION_DOMAIN d WHERE MANAGED_BY_TIBCO = '0' and d.MIGRATION_DOMAIN = e.MIGRATION_DOMAIN)
			order by THREADS, PERIOD_FROM_DATE
			) where rownum <= 1`
}