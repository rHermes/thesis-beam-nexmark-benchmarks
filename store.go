package main

import (
	"encoding/json"
	"errors"

	"github.com/rs/zerolog"
	bolt "go.etcd.io/bbolt"
)

var (
	seriesKey = []byte("series")
)

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
		if _, err := tx.CreateBucketIfNotExists(seriesKey); err != nil {
			return err
		}

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
		seriesBucket := tx.Bucket(seriesKey)

		if err := seriesBucket.DeleteBucket(bid); err == nil {
			logger.Warn().Msg("Overwriting series")
		} else if err != bolt.ErrBucketNotFound {
			return err
		}

		serieBucket, err := seriesBucket.CreateBucket(bid)
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

			if err := serieBucket.Put(Bprintf("b-%d", i), data); err != nil {
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
		seriesBucket := tx.Bucket(seriesKey)
		found = seriesBucket.Bucket([]byte(id)) != nil
		return nil
	})
	return found, err
}

// RunSeries executes the series and stores the results in the datbase.
func (s *Store) RunSeries(id string) error {
	return errors.New("Not implemented yet")
}
