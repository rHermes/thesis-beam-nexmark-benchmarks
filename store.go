package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	bolt "go.etcd.io/bbolt"
)

const (
	StatusOK     = "OK"
	StatusErr    = "ERR"
	StatusNotRun = "NOT_RUN"
)

var (
	benchPrefix  = []byte("bench-")
	statusPrefix = []byte("status-")
	stdoutPrefix = []byte("stdout-")
	stderrPrefix = []byte("stderr-")
	resultPrefix = []byte("result-")

	ErrorSeriesNotFound = errors.New("Series not found")
)

type Run struct {
	Series string
	Status string

	Result *Result
	Stdout *string
	Stderr *string
}

// A store stores data. The idea is that you have a list of series, which consists of benchmarks.
type Store struct {
	logger zerolog.Logger
	db     *bolt.DB
}

func NewStore(logger zerolog.Logger, path string) (*Store, error) {
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		return nil, err
	}

	// Setup the database
	err = db.Update(func(tx *bolt.Tx) error {
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &Store{
		logger: logger,
		db:     db,
	}, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) StoreSeries(id string, benchmarks []Benchmark) error {
	logger := s.logger.With().Str("series_key", id).Logger()
	bid := []byte(id)
	err := s.db.Update(func(tx *bolt.Tx) error {
		if err := tx.DeleteBucket(bid); err == nil {
			logger.Warn().Msg("Overwriting series")
		} else if err != bolt.ErrBucketNotFound {
			return err
		}

		serieBucket, err := tx.CreateBucket(bid)
		if err != nil {
			logger.Error().Err(err).Msg("Failed to create serie bucket")
			return err
		}

		for i, b := range benchmarks {
			data, err := json.Marshal(b)
			if err != nil {
				logger.Error().Err(err).Msg("Failed to marshal benchmark")
				return err
			}

			v := itob(i)
			if err := serieBucket.Put(append(benchPrefix, v...), data); err != nil {
				logger.Error().Err(err).Msg("Failed to store benchmark")
				return err
			}
		}
		return nil
	})
	return err
}

// Return true false if series exists
func (s *Store) HasSeries(id string) (bool, error) {
	var found bool
	err := s.db.View(func(tx *bolt.Tx) error {
		found = tx.Bucket([]byte(id)) != nil
		return nil
	})
	return found, err
}

func (s *Store) GetSeriesBenchmarks(id string) ([]Benchmark, error) {
	var benches []Benchmark
	err := s.db.View(func(tx *bolt.Tx) error {
		series := tx.Bucket([]byte(id))
		if series == nil {
			return ErrorSeriesNotFound
		}

		c := series.Cursor()
		for k, v := c.Seek(benchPrefix); k != nil && bytes.HasPrefix(k, benchPrefix); k, v = c.Next() {
			var bench Benchmark
			if err := json.Unmarshal(v, &bench); err != nil {
				return err
			}

			benches = append(benches, bench)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return benches, nil
}

func (s *Store) GetBenchmarkStatus(sid string, bid int) (string, error) {
	var out string
	err := s.db.View(func(tx *bolt.Tx) error {
		series := tx.Bucket([]byte(sid))
		if series == nil {
			return ErrorSeriesNotFound
		}

		v := series.Get(append(statusPrefix, itob(bid)...))
		if v == nil {
			out = StatusNotRun
		} else {
			out = string(v)
		}

		return nil
	})
	if err != nil {
		return "", err
	}

	return out, nil
}

// RunSeries executes the series and stores the results in the datbase.
func (s *Store) RunSeries(sid string) error {
	// We first read in the benchmarks that we need to do.
	benches, err := s.GetSeriesBenchmarks(sid)
	if err != nil {
		return err
	}

	for bid, bench := range benches {
		status, err := s.GetBenchmarkStatus(sid, bid)
		if err != nil {
			return err
		}
		fmt.Printf("Bench %d has status: %s\n", bid, status)

		if status != StatusNotRun {
			continue
		}

		if err := s.RunBenchmark(sid, bid, bench); err != nil {
			log.Error().Err(err).Msg("Benchmark errored out")
		}
	}

	return nil
}

// Run a single benchmark. An error here indicate some process error, not an error in running the benchmark
func (s *Store) RunBenchmark(sid string, bid int, bench Benchmark) error {
	tmpDir := os.TempDir()
	bench.JavascriptFilename = filepath.Join(tmpDir, "flink-jsfile.js")
	stdout, stderr, merr := bench.Run(s.logger, GradlePath, BeamPath)
	// We write the
	if merr != nil {
		fmt.Println(stderr)
	}

	s.logger.Info().Msg("Running a benchmark")

	err := s.db.Update(func(tx *bolt.Tx) error {
		series := tx.Bucket([]byte(sid))
		if series == nil {
			return ErrorSeriesNotFound
		}
		bb := itob(bid)

		status := StatusOK
		if merr != nil {
			status = StatusErr
		}
		series.Put(append(statusPrefix, bb...), []byte(status))
		series.Put(append(stdoutPrefix, bb...), stdout)
		series.Put(append(stderrPrefix, bb...), stderr)

		if merr != nil {
			return nil
		}

		res, err := bench.AugmentResults(s.logger)
		if err != nil {
			return err
		}

		data, err := json.Marshal(res)
		if err != nil {
			return err
		}
		series.Put(append(resultPrefix, bb...), data)

		return nil
	})
	return err
}
