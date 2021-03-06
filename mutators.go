package main

import (
	"encoding/json"
	"io"
	"time"

	"github.com/rs/zerolog"
)

type Mutator func(zerolog.Logger, Benchmark) error
type Middleware func(Mutator) Mutator

// Vary the number of generators.
func VaryNumberOfGenerators(start, end, step int) Middleware {
	return func(mut Mutator) Mutator {
		return func(logger zerolog.Logger, b Benchmark) error {
			for i := start; i < end; i += step {
				b.NumEventGenerators = IntPtr(i)
				logger := logger.With().Int("numGenerators", i).Logger()
				if err := mut(logger, b); err != nil {
					return err
				}
			}
			return nil
		}
	}
}

func UseParallelism(vals []int) Middleware {
	return func(mut Mutator) Mutator {
		return func(logger zerolog.Logger, b Benchmark) error {
			for _, val := range vals {
				b.Parallelism = val
				logger := logger.With().Int("parallelism", val).Logger()
				if err := mut(logger, b); err != nil {
					return err
				}
			}
			return nil
		}
	}
}

func VaryAvgPersonSize(start, end, step int) Middleware {
	return func(mut Mutator) Mutator {
		return func(logger zerolog.Logger, b Benchmark) error {
			for i := start; i < end; i += step {
				b.AveragePersonByteSize = IntPtr(i)
				logger := logger.With().Int("avgPersonByteSize", i).Logger()
				if err := mut(logger, b); err != nil {
					return err
				}
			}
			return nil
		}
	}
}

func VaryAvgAuctionSize(start, end, step int) Middleware {
	return func(mut Mutator) Mutator {
		return func(logger zerolog.Logger, b Benchmark) error {
			for i := start; i < end; i += step {
				b.AverageAuctionByteSize = IntPtr(i)
				logger := logger.With().Int("avgAuctionByteSize", i).Logger()
				if err := mut(logger, b); err != nil {
					return err
				}
			}
			return nil
		}
	}
}

func VaryAvgBidSize(start, end, step int) Middleware {
	return func(mut Mutator) Mutator {
		return func(logger zerolog.Logger, b Benchmark) error {
			for i := start; i < end; i += step {
				b.AverageBidByteSize = IntPtr(i)
				logger := logger.With().Int("avgBidByteSize", i).Logger()
				if err := mut(logger, b); err != nil {
					return err
				}
			}
			return nil
		}
	}
}

func VaryParallelism(start, end, step int) Middleware {
	return func(mut Mutator) Mutator {
		return func(logger zerolog.Logger, b Benchmark) error {
			for i := start; i < end; i += step {
				b.Parallelism = i
				logger := logger.With().Int("parallelism", i).Logger()
				if err := mut(logger, b); err != nil {
					return err
				}
			}
			return nil
		}
	}
}

func VaryCoderStrategy(strats []string) Middleware {
	return func(mut Mutator) Mutator {
		return func(logger zerolog.Logger, b Benchmark) error {
			for _, strat := range strats {
				logger := logger.With().Str("coder", strat).Logger()
				b.CoderStrategy = strat
				if err := mut(logger, b); err != nil {
					logger.Error().Err(err).Msg("Error in some sort")
					return err
				}
			}
			return nil
		}
	}
}

func VaryQuery(queries []string) Middleware {
	return func(mut Mutator) Mutator {
		return func(logger zerolog.Logger, b Benchmark) error {
			for _, query := range queries {
				logger := logger.With().Str("query", query).Logger()
				b.Query = query
				if err := mut(logger, b); err != nil {
					return err
				}
			}
			return nil
		}
	}
}

// RepeatRuns repeats the mutator to run x amount of times
func RepeatRuns(times int) Middleware {
	return func(mut Mutator) Mutator {
		return func(logger zerolog.Logger, b Benchmark) error {
			for i := 0; i < times; i++ {
				logger := logger.With().Int("run", i).Logger()
				logger.Debug().Msg("Starting run")

				if err := mut(logger, b); err != nil {
					logger.Error().Err(err).Msg("Error during run")
					return err
				}

				logger.Debug().Msg("Finished with run")
			}
			return nil
		}
	}
}

// TimerMutator times the mutation
func TimerMutator(mut Mutator) Mutator {
	return func(logger zerolog.Logger, b Benchmark) error {
		start := time.Now()
		if err := mut(logger, b); err != nil {
			return err
		}
		dur := time.Since(start)

		logger.Debug().Dur("dur", dur).Msg("Timer finished")
		return nil
	}
}

// SwapFasterCopy runs the mutator twice, swapping the copy
func SwapFasterCopy(mut Mutator) Mutator {
	return func(logger zerolog.Logger, b Benchmark) error {
		b.FasterCopy = false
		if err := mut(logger.With().Bool("faster_copy", b.FasterCopy).Logger(), b); err != nil {
			return err
		}
		b.FasterCopy = true
		if err := mut(logger.With().Bool("faster_copy", b.FasterCopy).Logger(), b); err != nil {
			return err
		}
		return nil
	}
}

func ArrayBench(arr *[]Benchmark) Mutator {
	return func(logger zerolog.Logger, bench Benchmark) error {
		*arr = append(*arr, bench)
		return nil
	}
}

// StoreBench creates a mutator that stores the results of invocations
func StoreBench(dst io.Writer) Mutator {
	jwer := json.NewEncoder(dst)
	return func(logger zerolog.Logger, bench Benchmark) error {
		_, _, err := bench.Run(logger, GradlePath, BeamPath)
		if err != nil {
			logger.Error().Err(err).Msg("Something went wrong in the writing")
			// fmt.Printf("%s\n", gg)
			return err
		}

		res, err := bench.AugmentResults(logger)
		if err != nil {
			logger.Error().Err(err).Msg("Error during augmentation")
			return err
		}

		if err := jwer.Encode(res); err != nil {
			logger.Error().Err(err).Msg("Error during file writing")
			return err
		}
		logger.Info().Msg("Done with one")
		return nil
	}
}
