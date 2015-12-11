package bq

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"golang.org/x/oauth2"
	// "golang.org/x/oauth2/google"
	"golang.org/x/oauth2/jwt"
	bigquery "google.golang.org/api/bigquery/v2"

	"google.golang.org/api/googleapi"
)

type Streamer struct {
	project string
	dataset string
	service *bigquery.Service
	Errors  chan error

	tables map[string]*tableStreamer
	sync.RWMutex
}

func NewStreamer(service *bigquery.Service, project, dataset string) *Streamer {
	return &Streamer{
		service: service,
		project: project,
		dataset: dataset,

		tables: make(map[string]*tableStreamer),
	}
}

func (s *Streamer) Insert(table string, data interface{}) {
	s.RLock()
	ts := s.tables[table]
	s.RUnlock()

	if ts == nil {
		s.Lock()
		ts = newTableStreamer(s, table)
		s.tables[table] = ts
		go ts.run()
		s.Unlock()
	}

	ts.insert(data)
}

func (s *Streamer) Stop() {
	s.Lock()
	for table, ts := range s.tables {
		close(ts.stop)
		delete(s.tables, table)
	}
	s.Unlock()
}

type tableStreamer struct {
	streamer *Streamer
	service  *bigquery.TabledataService
	table    string

	incoming chan interface{}
	stop     chan struct{}

	queue []interface{}

	flushDelay time.Duration
	flushMax   int
}

func newTableStreamer(streamer *Streamer, table string) *tableStreamer {
	return &tableStreamer{
		streamer: streamer,
		service:  bigquery.NewTabledataService(streamer.service),
		table:    table,

		incoming: make(chan interface{}, 500),
		stop:     make(chan struct{}),

		flushDelay: 10 * time.Second,
		flushMax:   500,
	}
}

func (ts *tableStreamer) insert(data interface{}) {
	ts.incoming <- data
}

func (ts *tableStreamer) run() {
	timer := time.Tick(ts.flushDelay)
	for {
		select {
		case data := <-ts.incoming:
			ts.queue = append(ts.queue, data)
			if len(ts.queue) > ts.flushMax {
				ts.flush()
			}
		case <-timer:
			ts.flush()
		case <-ts.stop:
			ts.flush()
			return
		}
	}
}

func (ts *tableStreamer) flush() {
	if len(ts.queue) == 0 {
		return
	}

	// TODO: insertID
	rows := make([]*bigquery.TableDataInsertAllRequestRows, len(ts.queue))
	for i, row := range ts.queue {
		encoded, err := Encode(row)
		if err != nil {
			ts.streamer.Errors <- err
			continue
		}

		rows[i] = &bigquery.TableDataInsertAllRequestRows{
			Json: encoded,
		}
	}

	//  send request
	request := &bigquery.TableDataInsertAllRequest{
		Kind: "bigquery#tableDataInsertAllRequest",
		Rows: rows,
	}
	resp, err := ts.service.InsertAll(ts.streamer.project, ts.streamer.dataset, ts.table, request).Do()

	// success
	if err == nil {
		if len(resp.InsertErrors) == 0 {
			// fmt.Println("No errors found.")
		} else {
			for _, errs := range resp.InsertErrors {
				for _, err := range errs.Errors {
					ts.streamer.Errors <- fmt.Errorf("BQ error: %+v", err)
				}
			}
		}
		ts.queue = nil
		return
	}

	// internal errors
	if gerr, ok := err.(*googleapi.Error); ok {
		switch gerr.Code {
		case 500, 503:
			// sleep & retry
			time.Sleep(10 * time.Second)
			return
		}
	}

	// missing table
	if isTableNotFoundErr(err) {
		schema, _ := Schema(ts.queue[0])
		tablesService := bigquery.NewTablesService(ts.streamer.service)
		table := &bigquery.Table{
			Schema: schema,
			TableReference: &bigquery.TableReference{
				ProjectId: ts.streamer.project,
				DatasetId: ts.streamer.dataset,
				TableId:   ts.table,
			},
		}
		_, makeTableErr := tablesService.Insert(ts.streamer.project, ts.streamer.dataset, table).Do()
		if makeTableErr == nil || isAlreadyExistsErr(makeTableErr) {
			// table exists now (or someone already made it)
			// so try again
			fmt.Println("Made table, retrying...")
			ts.flush()
			return
		} else {
			ts.streamer.Errors <- makeTableErr
			return
		}
	}

	if err != nil {
		ts.streamer.Errors <- err
	}
}

func NewBigQueryService(c *jwt.Config) (service *bigquery.Service, err error) {
	client := c.Client(oauth2.NoContext)
	service, err = bigquery.New(client)
	return
}

func isTableNotFoundErr(err error) bool {
	if gerr, ok := err.(*googleapi.Error); ok {
		if gerr.Code == 404 && strings.Contains(gerr.Message, "Not found: Table") {
			return true
		}
	}
	return false
}

func isAlreadyExistsErr(err error) bool {
	if gerr, ok := err.(*googleapi.Error); ok {
		if gerr.Code == 409 && strings.Contains(gerr.Message, "Already Exists") {
			return true
		}
	}
	return false
}
