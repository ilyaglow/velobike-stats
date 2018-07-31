package main

import (
	"database/sql"
	"log"
	"os"
	"time"

	"github.com/ilyaglow/velobike-stats"
	"github.com/kshvakov/clickhouse"
	"gopkg.ilya.app/ilyaglow/go-velobike.v2/velobike"
)

const (
	createStmt = `
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

	insertStmt = `
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
			timestamp,
			state_seconds
		) VALUES (
			?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
		)
	`
)

var (
	defaultTimeout = 5 * time.Second
)

type pState struct {
	freePlaces int
	timestamp  time.Time
	seconds    float64
}

type config struct {
	client        *velobike.Client
	parkingsState map[string]*pState
	conn          *sql.DB
}

func main() {
	conn, err := sql.Open("clickhouse", os.Getenv("CLICKHOUSE_URL"))
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	if err := conn.Ping(); err != nil {
		log.Fatal("check your CLICKHOUSE_URL environment variable")
	}

	if _, err := conn.Exec(createStmt); err != nil {
		panic(err)
	}

	conf := &config{
		client:        velobike.NewClient(nil),
		parkingsState: make(map[string]*pState),
		conn:          conn,
	}

	if err := conf.do(); err != nil {
		log.Fatal(err)
	}

	ticket := time.NewTicker(defaultTimeout)
	for range ticket.C {
		go func() {
			if err := conf.do(); err != nil {
				log.Fatal(err)
			}
		}()
	}
}

func (c *config) newRecord(p *velobike.Parking, ts time.Time) *vbpstats.Record {
	if _, ok := c.parkingsState[*p.ID]; !ok {
		c.parkingsState[*p.ID] = &pState{
			freePlaces: *p.FreePlaces,
			seconds:    defaultTimeout.Seconds(),
			timestamp:  ts,
		}
	} else {
		if c.parkingsState[*p.ID].freePlaces == *p.FreePlaces {
			c.parkingsState[*p.ID].seconds = time.Since(c.parkingsState[*p.ID].timestamp).Seconds() + c.parkingsState[*p.ID].seconds
		} else {
			c.parkingsState[*p.ID].seconds = ts.Sub(c.parkingsState[*p.ID].timestamp).Seconds()
		}

		c.parkingsState[*p.ID].timestamp = ts
	}

	return &vbpstats.Record{
		Parking:      p,
		StateSeconds: c.parkingsState[*p.ID].seconds,
		Timestamp:    ts,
	}
}

func (c *config) pollParkings() ([]*vbpstats.Record, error) {
	parkings, _, err := c.client.Parkings.List()
	if err != nil {
		return nil, err
	}

	ts := time.Now().UTC()
	var recs []*vbpstats.Record
	for i := range parkings.Items {
		recs = append(recs, c.newRecord(&parkings.Items[i], ts))
	}

	return recs, nil
}

func (c *config) do() error {
	recs, err := c.pollParkings()
	if err != nil {
		return err
	}

	tx, err := c.conn.Begin()
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare(insertStmt)
	if err != nil {
		return err
	}

	for i := range recs {
		if _, err := stmt.Exec(
			*recs[i].Address,
			*recs[i].FreeElectricPlaces,
			*recs[i].FreeOrdinaryPlaces,
			*recs[i].FreePlaces,
			*recs[i].HasTerminal,
			*recs[i].ID,
			*recs[i].IsFavourite,
			*recs[i].IsLocked,
			*recs[i].Name,
			clickhouse.Array(recs[i].StationTypes),
			*recs[i].TotalElectricPlaces,
			*recs[i].TotalOrdinaryPlaces,
			*recs[i].TotalPlaces,
			*recs[i].Position.Lat,
			*recs[i].Position.Lon,
			clickhouse.DateTime(recs[i].Timestamp),
			recs[i].StateSeconds,
		); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}
