package main

import (
	"database/sql"
	"log"
	"os"
	"time"

	"github.com/ilyaglow/go-velobike/velobike"
	"github.com/kshvakov/clickhouse"
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
			state_count UInt32
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
			state_count
		) VALUES (
			?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
		)
	`
)

var (
	defaultTimeout = 1 * time.Minute
)

type pState struct {
	freePlaces int
	count      int
}

type config struct {
	client             *velobike.Client
	parkingsStateCount map[string]*pState
	conn               *sql.DB
}

type record struct {
	*velobike.Parking
	StateCount int       `json:"StateCount,omitempty"`
	Timestamp  time.Time `json:"Timestamp,omitempty"`
}

func main() {
	conn, err := sql.Open("clickhouse", os.Getenv("CLICKHOUSE_URL"))
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	if err := conn.Ping(); err != nil {
		panic(err)
	}

	if _, err := conn.Exec(createStmt); err != nil {
		panic(err)
	}

	conf := &config{
		client:             velobike.NewClient(nil),
		parkingsStateCount: make(map[string]*pState),
		conn:               conn,
	}

	if err := conf.do(); err != nil {
		log.Fatal(err)
	}

	ticket := time.NewTicker(defaultTimeout)
	for range ticket.C {
		go func() {
			if err := conf.do(); err != nil {
				log.Println(err)
			}
		}()
	}
}

func (c *config) newRecord(p *velobike.Parking, ts time.Time) *record {
	if _, ok := c.parkingsStateCount[*p.ID]; !ok {
		c.parkingsStateCount[*p.ID] = &pState{
			freePlaces: *p.FreePlaces,
			count:      1,
		}
	} else {
		if c.parkingsStateCount[*p.ID].freePlaces == *p.FreePlaces {
			c.parkingsStateCount[*p.ID].count++
		} else {
			c.parkingsStateCount[*p.ID].count = 1
		}
	}

	return &record{
		Parking:    p,
		StateCount: c.parkingsStateCount[*p.ID].count,
		Timestamp:  ts,
	}
}

func (c *config) pollParkings() ([]*record, error) {
	parkings, _, err := c.client.Parkings.List()
	if err != nil {
		return nil, err
	}

	ts := time.Now().UTC()
	var recs []*record
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
			recs[i].StateCount,
		); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}
