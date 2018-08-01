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

var (
	defaultTimeout = 60 * time.Second
)

type config vbpstats.Config

func main() {
	conn, err := sql.Open("clickhouse", os.Getenv("CLICKHOUSE_URL"))
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	if err := conn.Ping(); err != nil {
		log.Fatal("check your CLICKHOUSE_URL environment variable")
	}

	if _, err := conn.Exec(vbpstats.CreateStmt); err != nil {
		panic(err)
	}

	conf := &config{
		Client:        velobike.NewClient(nil),
		ParkingsState: make(map[string]*vbpstats.PlacesState),
		Conn:          conn,
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
	if _, ok := c.ParkingsState[*p.ID]; !ok {
		c.ParkingsState[*p.ID] = &vbpstats.PlacesState{
			FreePlaces: *p.FreePlaces,
			Seconds:    defaultTimeout.Seconds(),
			Timestamp:  ts,
		}
	} else {
		if c.ParkingsState[*p.ID].FreePlaces == *p.FreePlaces {
			c.ParkingsState[*p.ID].Seconds = ts.Sub(c.ParkingsState[*p.ID].Timestamp).Seconds() + c.ParkingsState[*p.ID].Seconds
		} else {
			c.ParkingsState[*p.ID].Seconds = ts.Sub(c.ParkingsState[*p.ID].Timestamp).Seconds()
		}

		c.ParkingsState[*p.ID].Timestamp = ts
		c.ParkingsState[*p.ID].FreePlaces = *p.FreePlaces
	}

	return &vbpstats.Record{
		Parking:      p,
		StateSeconds: c.ParkingsState[*p.ID].Seconds,
		Timestamp:    ts,
	}
}

func (c *config) pollParkings() ([]*vbpstats.Record, error) {
	parkings, _, err := c.Client.Parkings.List()
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

	tx, err := c.Conn.Begin()
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare(vbpstats.InsertStmt)
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
			clickhouse.Date(recs[i].Timestamp),
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
