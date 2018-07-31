package main

import (
	"archive/tar"
	"compress/bzip2"
	"database/sql"
	"encoding/json"
	"flag"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ilyaglow/velobike-stats"
	"github.com/kshvakov/clickhouse"
	"gopkg.ilya.app/ilyaglow/go-velobike.v2/velobike"
)

const batchSize = 50000

var defaultTimeout = 60 * time.Second

func main() {
	fileName := flag.String("f", "velobike-parkings.tar.bz2", "Velobike parkings dataset: *.tar.bz2")
	click := flag.String("c", os.Getenv("CLICKHOUSE_URL"), "Clickhouse URL")
	flag.Parse()
	var errMessages []string
	if *fileName == "" {
		errMessages = append(errMessages, "no filename provided")
	}

	if *click == "" {
		errMessages = append(errMessages, "no clickhouse URL provided")
	}

	if len(errMessages) > 0 {
		log.Fatal(errMessages)
	}

	cfg, err := newConfig(*click)
	if err != nil {
		log.Fatal(err)
	}

	if err := procFile(cfg, *fileName); err != nil {
		log.Fatal(err)
	}
}

func newConfig(click string) (*vbpstats.Config, error) {
	conn, err := sql.Open("clickhouse", click)
	if err != nil {
		return nil, err
	}

	if err := conn.Ping(); err != nil {
		return nil, err
	}

	if _, err := conn.Exec(vbpstats.CreateStmt); err != nil {
		return nil, err
	}

	return &vbpstats.Config{
		Client:        velobike.NewClient(nil),
		ParkingsState: make(map[string]*vbpstats.PlacesState),
		Conn:          conn,
	}, nil
}

func newRecord(c *vbpstats.Config, p *velobike.Parking, ut int64) *vbpstats.Record {
	ts := time.Unix(ut, 0)

	if _, ok := c.ParkingsState[*p.ID]; !ok {
		c.ParkingsState[*p.ID] = &vbpstats.PlacesState{
			FreePlaces: *p.FreePlaces,
			Seconds:    defaultTimeout.Seconds(),
			Timestamp:  ts,
		}
	} else {
		if c.ParkingsState[*p.ID].FreePlaces == *p.FreePlaces {
			c.ParkingsState[*p.ID].Seconds = time.Since(c.ParkingsState[*p.ID].Timestamp).Seconds() + c.ParkingsState[*p.ID].Seconds
		} else {
			c.ParkingsState[*p.ID].Seconds = ts.Sub(c.ParkingsState[*p.ID].Timestamp).Seconds()
		}

		c.ParkingsState[*p.ID].Timestamp = ts
	}

	return &vbpstats.Record{
		Parking:      p,
		StateSeconds: c.ParkingsState[*p.ID].Seconds,
		Timestamp:    ts,
	}
}

func procFile(c *vbpstats.Config, src string) error {
	f, err := os.Open(src)
	if err != nil {
		return err
	}
	defer f.Close()

	recsChan := make(chan *vbpstats.Record, 500)

	var wg sync.WaitGroup
	wg.Add(1)
	defer wg.Wait()

	go func() {
		defer wg.Done()
		if err := send(c, recsChan); err != nil {
			log.Fatal(err)
		}
	}()

	bzf := bzip2.NewReader(f)
	tr := tar.NewReader(bzf)
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}

		if header.Size == 0 {
			continue
		}

		if err != nil {
			return err
		}

		var parkings velobike.Parkings
		if err = json.NewDecoder(tr).Decode(&parkings); err != nil {
			return err
		}

		ts := strings.TrimRight(strings.TrimLeft(header.Name, "parkings-"), ".json")
		if err = procParkings(c, ts, parkings.Items, recsChan); err != nil {
			return err
		}
	}
	close(recsChan)

	return nil
}

func procParkings(c *vbpstats.Config, uts string, parkings []velobike.Parking, recsChan chan *vbpstats.Record) error {
	ut, err := strconv.ParseInt(uts, 10, 64)
	if err != nil {
		return err
	}

	for i := range parkings {
		recsChan <- newRecord(c, &parkings[i], ut)
	}

	return nil
}

func send(c *vbpstats.Config, recsChan chan *vbpstats.Record) error {
	tx, err := c.Conn.Begin()
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare(vbpstats.InsertStmt)
	if err != nil {
		return err
	}

	j := 0
	k := 0
	for rec := range recsChan {
		if _, err := stmt.Exec(
			*rec.Address,
			*rec.FreeElectricPlaces,
			*rec.FreeOrdinaryPlaces,
			*rec.FreePlaces,
			*rec.HasTerminal,
			*rec.ID,
			*rec.IsFavourite,
			*rec.IsLocked,
			*rec.Name,
			clickhouse.Array(rec.StationTypes),
			*rec.TotalElectricPlaces,
			*rec.TotalOrdinaryPlaces,
			*rec.TotalPlaces,
			*rec.Position.Lat,
			*rec.Position.Lon,
			clickhouse.DateTime(rec.Timestamp),
			rec.StateSeconds,
		); err != nil {
			return err
		}

		k++
		j++
		if j == batchSize {
			if err := tx.Commit(); err != nil {
				return err
			}
			tx, err = c.Conn.Begin()
			if err != nil {
				return err
			}

			stmt, err = tx.Prepare(vbpstats.InsertStmt)
			if err != nil {
				return err
			}
			j = 0
			log.Printf("%d records has been imported", k)
		}
	}

	if j != 0 {
		if err := tx.Commit(); err != nil {
			return err
		}
		log.Printf("%d records has been imported", k)
	}

	return nil
}
