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
		`WITH
param AS
(SELECT
   rownum AS s_id,  -- IF there IS a selection OF MORE than one metering point OR periods, this id will identify the DATA SET.
   :meteringPointId AS metering_point_id, -- p_metering_point_id
   :processedFromTime AS processed_from_time, -- P_CUR_FROM_DATE_PERIOD
   :processedUntilTime AS processed_until_time -- P_CUR_TO_DATE_PERIOD
 FROM dual),
metering_point AS -- metering points to be included (for now only 1)
(SELECT
   p.s_id,
   m.MPOINT_SEQNO,
   p.metering_point_id
 FROM param p
   JOIN reading.m_meterpoint m ON m.objectid = p.metering_point_id),
counter AS -- counters for the relevant metering points
(SELECT 
   m.s_id,
   m.MPOINT_SEQNO,
   c.counter_seqno /* ,
   c.IS_COUNTER_CODE,
   c.COUNTER_CLASS_ID */
 FROM metering_point m
   JOIN reading.m_counter c ON c.mpoint_seqno = m.mpoint_seqno),
periodic_value AS -- the periodical volumes (monthly) stored as "time series" in IS Change
(SELECT
   p.s_id,
   pt.PERIODIC_TIMESERIES_SEQNO,
   m.mpoint_seqno,
   pt.START_DATE,
   pt.END_DATE,
   pt.RESOLUTION,
   pt.UNIT,
   pt.SERIES_TIMESTAMP,
   pt.sender_ref,
   nvl2(pt.HIST_TIMESTAMP, 'Y', 'N') AS historical_flag,
   round(MONTHS_BETWEEN(pv.READING_START_DATE, pt.START_DATE) + 1, 0) AS position,
   TO_CLOB (round(MONTHS_BETWEEN(pv.READING_START_DATE, pt.START_DATE) + 1, 0) || '|' || TO_CHAR(pv.READING_START_DATE, 'DD.MM.YYYY HH24:MI:SS') || '|' ||pv.AMOUNT || '|' || pv.DATA_ORIGIN || '|' ||pt.CANCELLED) as valuedata,
  pv.READING_START_DATE,
   pv.AMOUNT,
   pv.DATA_ORIGIN,
   pt.CANCELLED

 FROM param p
   JOIN metering_point m ON m.s_id = p.s_id
   JOIN CHANGE.S_PERIODIC_TIMESERIES pt ON pt.UTVEKSLINGSOBJEKTNR = m.MPOINT_SEQNO /* short-cut, but will always be equal for Energinet */
   JOIN CHANGE.S_PERIODIC_SERIES_VALUE pv ON pv.PERIODIC_TIMESERIES_SEQNO = pt.PERIODIC_TIMESERIES_SEQNO
 WHERE
   pt.SERIES_TIMESTAMP > p.processed_from_time - 1/24/60/60 AND 
   pt.SERIES_TIMESTAMP < p.processed_until_time),
series AS -- time series for the selected metering points
(SELECT
   p.s_id,
   s.IMPORT_SERIE_SEQNO,
   c.MPOINT_SEQNO ,
   c.COUNTER_SEQNO ,
   s.MIN_READING_TIME,
   s.max_reading_time,
   s.MAX_READING_TIME + s.RESOLUTION / 60 / 24 AS end_series_time, -- verify - this is not equal to original query!
   s.RESOLUTION,
   s.UNIT,
   s.SERIE_TIMESTAMP,
   s.sender_ref,
   b.IMPORT_BATCH_ID,
   b.batch_type,
   s.SERIE_STATUS,   
   s.read_reason as read_reason
 FROM param p
   JOIN counter c ON c.s_id = p.s_id
   JOIN reading.m_import_serie s ON s.counter_seqno = c.COUNTER_SEQNO
   JOIN reading.M_BATCH b ON b.batch_seqno = s.batch_seqno
 WHERE
   s.SERIE_STATUS IN (2,9) AND -- it has been mentioned to also include status 9 - to be verified
   s.SERIE_TIMESTAMP > p.processed_from_time - 1/24/60/60 AND 
   s.SERIE_TIMESTAMP < p.processed_until_time AND
   -- is there a better way to exclude these?
   --not (s.RESOLUTION = 60 and s.SENDER_REF like 'Calculated from 15 min values') AND
   -- Exclude roll up on resolution (replacing check on the sender_ref text above)
   b.BATCH_TYPE <> 'RR' AND
   -- Exclude time series that is both a roll up and a remainder from replacements 
   NOT EXISTS (
     SELECT 1 
     FROM counter c2 
       JOIN reading.m_import_serie s2 ON s2.counter_seqno = c2.counter_seqno 
     WHERE 
--       b.batch_type IN ('RT', 'AA') AND 
       b.batch_type = 'RT' AND 
       c2.s_id = c.s_id AND 
       c2.counter_seqno <> s.counter_seqno and
       s2.min_reading_time < s.max_reading_time AND 
       s2.max_reading_time > s.min_reading_time AND
       s2.serie_status = 2 and
       s2.resolution < s.resolution) /* and
   -- what is the purpose of this one?
    b.BATCH_TYPE <> 'CA' */),
message_data AS
(SELECT
   to_char(r.MELDINGSNR_DATA) AS message_id,
   r.TRANSREF,
   h.meldingsref
 FROM reading.s_recipient r 
   JOIN reading.s_melding h on h.meldingsnr = r.meldingsnr
 WHERE
   r.MELDINGSNR_DATA IN (SELECT to_number(sender_ref DEFAULT 0 ON conversion error) FROM (SELECT sender_ref FROM series UNION ALL SELECT sender_ref FROM periodic_value)) AND
   r.meldingsnr_data > 0 AND 
   r.DATA_KILDE = 'S'),
series_value AS -- values for each of the time series (active)
(SELECT
   s.IMPORT_SERIE_SEQNO,
  RTRIM(XMLAGG(XMLELEMENT(E, TO_CHAR(v.reading_time, 'DD.MM.YYYY HH24:MI:SS')  || '|' || v.READING_VALUE || '|' || decode(v.DATA_ORIGIN, 'M', 'E01', 'E', '56', 'C', '36', 'B', 'D01', '?', 'QM'), ';') order by v.reading_time).GetClobVal(),',') as valuedata, 
   'N' as historical_flag   
 FROM series s
   JOIN reading.M_SERIE_VALUE v ON v.IMPORT_SERIE_SEQNO = s.IMPORT_SERIE_SEQNO group by s.IMPORT_SERIE_SEQNO, 'N'),
historical_value AS -- historical values for each of the time series 
(SELECT
   s.IMPORT_SERIE_SEQNO,
   RTRIM(XMLAGG(XMLELEMENT(E, TO_CHAR(h.reading_time, 'DD.MM.YYYY HH24:MI:SS')  || '|' || h.READING_VALUE  || '|' || decode(h.DATA_ORIGIN, 'M', 'E01', 'E', '56', 'C', '36', 'B', 'D01', '?', 'QM'), ';') order by h.reading_time).GetClobVal(),',')  as valuedata,
   'Y' AS historical_flag
 FROM series s
   JOIN reading.M_SERIE_VALUE_HIST h ON h.OLD_SERIE_SEQNO = s.IMPORT_SERIE_SEQNO group by s.IMPORT_SERIE_SEQNO, 'Y')
SELECT -- Main SELECT
   m.metering_point_id,
   v.transref AS transaction_id, 
   v.meldingsref, 
   v.valid_from_date,
   v.valid_to_date,
   v.inserted_timestamp,
   v.historical_flag,
   v.resolution,
   v.unit,
   v.valuedata,
   v.status,
   v.read_reason
from
(SELECT -- values of series in IS Metering, both active and historical
   s.s_id,
   s.MPOINT_SEQNO,
   s.min_reading_time AS valid_from_date,
   s.end_series_time AS valid_to_date,
   s.UNIT,
   to_char(s.RESOLUTION) AS resolution,
   s.SERIE_TIMESTAMP AS inserted_timestamp,
   s.sender_ref,
   s.import_batch_id,
   s.transref,
   s.meldingsref,
   sv.historical_flag,
   sv.valuedata,
   --CASE s.serie_status WHEN 2 THEN 'OK' WHEN 9 THEN 'Deleted' ELSE 'Unknown' END AS status,
   s.serie_status AS status,
   s.read_reason
FROM (SELECT * FROM series_value UNION ALL SELECT * FROM historical_value) sv
   JOIN (SELECT ser.*, md.* FROM series ser LEFT JOIN message_data md ON md.message_id = ser.sender_ref) s ON s.IMPORT_SERIE_SEQNO = sv.IMPORT_SERIE_SEQNO
UNION ALL 
SELECT -- values of periodic (monthly) time series in IS CHANGE
   pv.s_id,
   pv.mpoint_seqno,
   pv.start_date AS valid_from_date,
   pv.END_date AS valid_to_date,
   pv.unit,
   pv.resolution,
   pv.series_timestamp AS inserted_timestamp,
   pv.sender_ref,
   NULL AS import_batch_id,
   pv.transref,
   pv.meldingsref,
   pv.historical_flag,
   pv.valuedata,
--   'OK' AS status,
   2 AS status,
   decode(pv.CANCELLED, 1, 'CAN', '') AS read_reason
FROM ((SELECT per.*, md.* FROM periodic_value per LEFT JOIN message_data md ON md.message_id = per.sender_ref)) pv) v
 JOIN METERING_POINT m ON m.mpoint_seqno = v.mpoint_seqno
 `
	//Set the order of the SQL statement
	sqlOrder := `   ORDER BY metering_point_id, valid_from_date, INSERTED_TIMESTAMP, historical_flag, meldingsref asc`

	return sql + sqlOrder
}

//GetSQLInsertFinishedTimeSeriesExportFile returns the SQL statement used to insert "data found""
func GetSQLInsertFinishedTimeSeriesExportFile() string {
	return " insert into " + config.GetExportProgressTableName() +
		" (MIGRATION_RUN_ID, MIGRATION_DOMAIN, EXPORTED_FROM_DATE, EXPORTED_TO_DATE, EXPORT_STATUS, DATA_FOUND, FILE_NAME, FILE_DETAILS, METERING_POINT_ID) " +
		" values (:migrationRunId, '" + config.GetDomain() + "', TO_DATE(:fromTimeDDMMYYYYHHMISS, 'DD.MM.YYYY HH24:MI:SS'), TO_DATE(:toTimeDDMMYYYYHHMISS, 'DD-MM-YYYY HH24:MI:SS'), '" + config.GetStatusFinished() + "', " +
		" :dataFound, :fileName, :fileDetails, :objectId)"
}

//GetSQLInsertNoDataFound returns the SQL statement used to
func GetSQLInsertNoDataFound() string {
	return " insert into " + config.GetExportProgressTableName() +
		" (MIGRATION_RUN_ID, MIGRATION_DOMAIN, EXPORTED_FROM_DATE, EXPORTED_TO_DATE, EXPORT_STATUS, DATA_FOUND, FILE_NAME, FILE_DETAILS, METERING_POINT_ID) " +
		" values (:migrationRunId, '" + config.GetDomain() + "', TO_DATE(:fromTimeDDMMYYYYHHMISS, 'DD-MM-YYYY HH24:MI:SS'), " +
		" TO_DATE(:toTimeDDMMYYYYHHMISS, 'DD-MM-YYYY HH24:MI:SS'), '" + config.GetStatusFinished() + "',  " +
		" :dataFound, :fileName, :fileDetails, :objectId)"
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
			order by THREADS, PERIOD_FROM_DATE
			) where rownum <= 1`
}

// GetDataMigrationExportedPeriod returns the SQL statement used to find scheduled runs
// Get the period that has been migrated successfully for the given data domain.
// Only full periods are selected. That means if one migration run partially succeeded, the PERIOD_TO_DATE of the migration domain is not updated by that run
// This means that for individual metering points the DATAMIGRATION_EXPORT_PROGRESS the period based on EXPORTED_FROM_DATE/EXPORTED_TO_DATE can differ. This will be handled in the export function below.
func GetDataMigrationExportedPeriod() string {

	return `
    SELECT MIN(PERIOD_FROM_DATE) as MaxFromDate, MAX(PERIOD_TO_DATE) as MaxToDate
	FROM DMDH3_OWN.DATAMIGRATION_EXPORT
    where MIGRATION_DOMAIN = 'TimeSeries'
	and MIGRATION_STATUS   = 'FIN'
`
}

// GetDataMigrationExportedPeriodForMp returns the period that has been migrated successfully for the given data domain and metering_point.
// Only successfully finished periods are selected. That means if a migration run failed, the PERIOD_TO_DATE of that migration will not be used (to allow retry for the export for this  period)
func GetDataMigrationExportedPeriodForMp(meterpointId string) string {

	return "SELECT MAX(EXPORTED_TO_DATE) as EXPORTED_TO_DATE " +
		" FROM  DMDH3_OWN.DATAMIGRATION_EXPORT_PROGRESS" +
		" where MIGRATION_DOMAIN = 'TimeSeries' " +
		" and   METERING_POINT_ID = '" + meterpointId + "'" +
		" and EXPORT_STATUS   = 'FIN'"
}
