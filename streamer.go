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

const (
	bufferSize = 500
)

type Streamer struct {
	project string
	dataset string
	service *bigquery.Service

	tables map[string]*tableStreamer
	mu     sync.RWMutex // tables mutex

	CreateTables bool
	Errors       chan error
}

func NewStreamer(service *bigquery.Service, project, dataset string) *Streamer {
	return &Streamer{
		service: service,
		project: project,
		dataset: dataset,

		tables: make(map[string]*tableStreamer),
		Errors: make(chan error, bufferSize),
	}
}

func (s *Streamer) Insert(table string, data interface{}) {
	s.mu.RLock()
	ts := s.tables[table]
	s.mu.RUnlock()

	if ts == nil {
		s.mu.Lock()
		ts = newTableStreamer(s, table)
		s.tables[table] = ts
		go ts.run()
		s.mu.Unlock()
	}

	ts.insert(data)
}

func (s *Streamer) Stop() {
	s.mu.Lock()
	for table, ts := range s.tables {
		close(ts.stop)
		delete(s.tables, table)
		ts.flush()
	}
	s.mu.Unlock()
}

type tableStreamer struct {
	streamer *Streamer
	service  *bigquery.TabledataService
	table    string

	incoming chan interface{}
	stop     chan struct{}

	queue []interface{}

	flushInterval time.Duration
	flushMax      int
}

func newTableStreamer(streamer *Streamer, table string) *tableStreamer {
	return &tableStreamer{
		streamer: streamer,
		service:  bigquery.NewTabledataService(streamer.service),
		table:    table,

		incoming: make(chan interface{}, bufferSize),
		stop:     make(chan struct{}),

		flushInterval: 1 * time.Minute,
		flushMax:      bufferSize,
	}
}

func (ts *tableStreamer) insert(data interface{}) {
	ts.incoming <- data
}

func (ts *tableStreamer) run() {
	tick := time.NewTicker(ts.flushInterval)
	defer tick.Stop()
	for {
		select {
		case data := <-ts.incoming:
			ts.queue = append(ts.queue, data)
			if len(ts.queue) >= ts.flushMax {
				ts.flush()
			}
		case <-tick.C:
			ts.flush()
		case <-ts.stop:
			// should be flushed by Stop
			return
		}
	}
}

func (ts *tableStreamer) flush() {
	const arbitrarySleepAmount = 10 * time.Second

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
		if len(resp.InsertErrors) > 0 {
			for _, errs := range resp.InsertErrors {
				for _, err := range errs.Errors {
					ts.streamer.Errors <- fmt.Errorf("BQ error: %+v", err)
				}
			}
		}
		// TODO: figure out how to deal w/ row errors
		// TODO: schema changes...
		ts.queue = nil
		return
	}

	// internal errors
	if gerr, ok := err.(*googleapi.Error); ok {
		switch gerr.Code {
		case 500, 503:
			// sleep & retry
			time.Sleep(arbitrarySleepAmount)
			return
		}
	}

	// missing table
	if ts.streamer.CreateTables && isTableNotFoundErr(err) {
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
			time.Sleep(arbitrarySleepAmount)
			return
		} else {
			ts.streamer.Errors <- makeTableErr
			// try again
			time.Sleep(arbitrarySleepAmount)
			return
		}
	}

	// some other kind of unexpected error
	// keep trying
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
