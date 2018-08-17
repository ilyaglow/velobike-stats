/*Package vbpstats (velobike parkings statistics)
 */
package vbpstats

import (
	"database/sql"
	"time"

	"github.com/rumyantseva/go-velobike/velobike"
)

const (
	// CreateStmt creates a table in Clickhouse OLAP
	CreateStmt = `
		CREATE TABLE IF NOT EXISTS velobike_parkings (
			address String,
			free_electric_places UInt8,
			free_ordinary_places UInt8,
			free_places UInt8,
			has_terminal UInt8,
			id String,
			is_favorite UInt8,
			is_locked UInt8,
			name String,
			station_types Array(String),
			total_electric_places UInt8,
			total_ordinary_places UInt8,
			total_places UInt8,
			latitude Float64,
			longitude Float64,
			date Date Default today(),
			timestamp Datetime,
			state_seconds Float64
		) engine=MergeTree(date, (id, timestamp), 8192)
	`

	// InsertStmt is a prepare statement to insert data to the Clickhouse
	InsertStmt = `
		INSERT INTO velobike_parkings (
			address,
			free_electric_places,
			free_ordinary_places,
			free_places,
			has_terminal,
			id,
			is_favorite,
			is_locked,
			name,
			station_types,
			total_electric_places,
			total_ordinary_places,
			total_places,
			latitude,
			longitude,
			date,
			timestamp,
			state_seconds
		) VALUES (
			?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
		)
	`
)

// Config holds import data process configuration
type Config struct {
	Client        *velobike.Client
	ParkingsState map[string]*PlacesState
	Conn          *sql.DB
}

// PlacesState is a helper struct aimed to track same state time duration
type PlacesState struct {
	FreePlaces int
	Timestamp  time.Time
	Seconds    float64
}

// Record represents a struct ready to be imported to the database
type Record struct {
	*velobike.Parking
	StateSeconds float64   `json:"StateSeconds,omitempty"`
	Timestamp    time.Time `json:"Timestamp,omitempty"`
}

// FindNearest returns nearest parkings depends on geohash
// func FindNearest(latitude float64, longitude float64) (ParkingID string)

// GetFreePlaces retrieves a number of free places at the moment
// func GetFreePlaces(id string) int

// AvgBikeWaitTime returns an average time to wait for a bike within a time
// slot +- duration provided
// func AvgBikeWaitTime(id string, t time.Time, d time.Duration) time.Duration

// AvgPlaceWaitTime calculates an average time duration to wait for a free
// place
// func AvgPlaceWaitTime(id string, ts time.Time, d time.Duration) time.Duration
