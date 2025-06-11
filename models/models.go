package models

import "time"

// ScheduledRun defines the scheduled run stored in the DB table
type ScheduledRun struct {
	MigrationRunId   int
	Threads          int
	MigrationDueDate string
	UseListOfMPs     bool
	PeriodFromDate   time.Time
	PeriodToDate     time.Time
}

// Data defines the top level node used in our json files
type Data struct {
	MeteringPointData MeteringPointData `json:"metering_point"`
	TimeSeries        []TimeSeriesData  `json:"time_series"`
}

// MeteringPointData defines one of the top level node used in our json files
type MeteringPointData struct {
	MeteringPointId string       `  json:"metering_point_id"`
	MasterData      []Masterdata `json:"masterdata,omitempty"`
}

// Masterdata defines the top level node used in our json files
type Masterdata struct {
	GridArea string `json:"grid_area,omitempty"`
	TypeOfMP string `json:"type_of_mp,omitempty"`
}

// TimeSeriesData defines the top level node used in our json files
type TimeSeriesData struct {
	TransactionId         string            `json:"transaction_id"`
	MessageId             string            `json:"message_id"`
	ValidFromDate         string            `json:"valid_from_date"`
	ValidToDate           string            `json:"valid_to_date"`
	TransactionInsertDate string            `json:"transaction_insert_date"`
	HistoricalFlag        string            `json:"historical_flag"`
	Resolution            string            `json:"resolution"`
	Unit                  string            `json:"unit"`
	Status                int               `json:"status,omitempty"`
	ReadReason            string            `json:"read_reason"`
	TimeSeriesValues      []TimeSeriesValue `json:"values,omitempty"`
	RawTimeSeriesValues   string
}

// TimeSeriesValue defines the low level nodes used in our json files
type TimeSeriesValue struct {
	Position int     `json:"position"`
	Quantity float64 `json:"quantity"`
	Quality  string  `json:"quality"`
}
