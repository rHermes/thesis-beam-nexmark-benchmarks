package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/renameio"
	"github.com/robertkrimen/otto"
	"github.com/rs/zerolog"
)

const (
	GradlePath = "/home/rhermes/build/beam/gradlew"
	BeamPath   = "/home/rhermes/build/beam"
	OutputPath = "/home/rhermes/commons/uni/thesis/beam-nexmark-benchmarks/results"
)

const (
	PassthroughQuery                 = "PASSTHROUGH"
	CurrencyConversionQuery          = "CURRENCY_CONVERSION"
	SelectionQuery                   = "SELECTION"
	LocalItemSuggestionQuery         = "LOCAL_ITEM_SUGGESTION"
	AveragePriceForCategoryQuery     = "AVERAGE_PRICE_FOR_CATEGORY"
	HotItemsQuery                    = "HOT_ITEMS"
	AverageSellingPriceBySellerQuery = "AVERAGE_SELLING_PRICE_BY_SELLER"
	HighestBidQuery                  = "HIGHEST_BID"
	MonitorNewUsersQuery             = "MONITOR_NEW_USERS"
	WinningBidsQuery                 = "WINNING_BIDS"
	LogToShardedFilesQuery           = "LOG_TO_SHARDED_FILES"
	UserSessionsQuery                = "USER_SESSIONS"
	ProcessingTimeWindowsQuery       = "PROCESSING_TIME_WINDOWS"
	BoundedSideInputJoinQuery        = "BOUNDED_SIDE_INPUT_JOIN"
	SessionSideInputJoinQuery        = "SESSION_SIDE_INPUT_JOIN"

	// Max we can do with different coders, don't know why
	MAX_EVENTS = 991683
)

var (
	AllQueries = []string{
		PassthroughQuery,
		CurrencyConversionQuery,
		SelectionQuery,
		LocalItemSuggestionQuery,
		AveragePriceForCategoryQuery,
		HotItemsQuery,
		AverageSellingPriceBySellerQuery,
		HighestBidQuery,
		MonitorNewUsersQuery,
		WinningBidsQuery,
		// LogToShardedFilesQuery,
		UserSessionsQuery,
		ProcessingTimeWindowsQuery,
		BoundedSideInputJoinQuery,
		SessionSideInputJoinQuery,
	}
)

type Benchmark struct {
	FlinkMaster        string
	JavascriptFilename string `json:"-"`
	Query              string
	Parallelism        int

	NumEventGenerators *int
	NumEvents          *int

	CoderStrategy string

	FasterCopy bool
}

func (b *Benchmark) Run(logger zerolog.Logger, gradlePath, beamPath string) ([]byte, []byte, error) {
	nargs := []string{
		"--runner=FlinkRunner",
		"--streaming",
		"--manageResources=false",
		"--monitorJobs=true",
		"--debug=true",
		fmt.Sprintf("--flinkMaster=%s", b.FlinkMaster),
		fmt.Sprintf("--query=%s", b.Query),
		fmt.Sprintf("--javascriptFilename=%s", b.JavascriptFilename),
		fmt.Sprintf("--fasterCopy=%t", b.FasterCopy),
		fmt.Sprintf("--parallelism=%d", b.Parallelism),
	}
	if b.NumEvents != nil {
		nargs = append(nargs, fmt.Sprintf("--numEvents=%d", *b.NumEvents))
	}
	if b.NumEventGenerators != nil {
		nargs = append(nargs, fmt.Sprintf("--numEventGenerators=%d", *b.NumEventGenerators))
	}
	if b.CoderStrategy != "" {
		nargs = append(nargs, fmt.Sprintf("--coderStrategy=%s", b.CoderStrategy))
	}

	args := []string{
		"-p", beamPath,
		"-Pnexmark.runner=:runners:flink:1.10",
		// "-Pnexmark.runner=:runners:direct-java",
		"-Pnexmark.args=" + strings.Join(nargs, "\n"),
		":sdks:java:testing:nexmark:run",
	}

	c := exec.Command(GradlePath, args...)
	var stdout, stderr bytes.Buffer
	// c.Stderr = os.Stderr
	c.Stderr = &stderr
	c.Stdout = &stdout
	if err := c.Run(); err != nil {
		return stdout.Bytes(), stderr.Bytes(), err
	}

	return stdout.Bytes(), stderr.Bytes(), nil
}

// Reads in the javascript file and adds the extra info to the result.
func (b *Benchmark) AugmentResults(logger zerolog.Logger) (*Result, error) {
	fp, err := os.Open(b.JavascriptFilename)
	if err != nil {
		return nil, err
	}
	defer fp.Close()
	vm, _, err := otto.Run(fp)
	if err != nil {
		return nil, err
	}
	all, err := vm.Eval("JSON.stringify(all)")
	if err != nil {
		return nil, err
	}

	var jres []JSResult
	if err := json.Unmarshal([]byte(all.String()), &jres); err != nil {
		return nil, err
	}

	if len(jres) != 1 {
		return nil, errors.New("Didn't expect more than one JSResult")
	}

	// Return the augmented result
	return &Result{
		JSResult: jres[0],
		Extra: Extra{
			FasterCopy:  b.FasterCopy,
			Parallelism: b.Parallelism,
		},
	}, nil
}

func main() {
	opts := func(w *zerolog.ConsoleWriter) {
		w.NoColor = true
		w.TimeFormat = time.Stamp
	}
	logger := zerolog.New(zerolog.NewConsoleWriter(opts)).
		With().Timestamp().Logger().Level(zerolog.InfoLevel)
	// logger = logger.Level(zerolog.InfoLevel)

	basedir := "/home/rhermes/commons/uni/thesis/beam-nexmark-benchmarks/results"
	// if err := battery01(logger, basedir+"/battery01"); err != nil {
	// 	logger.Fatal().Err(err).Msg("Couldn't run benchmark")
	// }
	// if err := battery02(logger, basedir+"/battery02"); err != nil {
	// 	logger.Fatal().Err(err).Msg("Couldn't run benchmark")
	// }

	store, err := NewStore(logger, basedir+"/dbs/proto.db")
	if err != nil {
		logger.Fatal().Err(err).Msg("Couldn't open the store")
	}
	defer store.Close()

	if err := battery03(logger, store, basedir+"/battery03"); err != nil {
		logger.Fatal().Err(err).Msg("couldn't run the battery")
	}

}

func battery03GenerateBenchmarks(logger zerolog.Logger) ([]Benchmark, error) {
	baseBench := Benchmark{
		FlinkMaster: "[local]",
		NumEvents:   IntPtr(MAX_EVENTS),
		Parallelism: 2,
	}

	var benches []Benchmark

	queries := AllQueries

	for _, query := range queries {
		baseBench.Query = query

		coders := []string{"HAND", "AVRO", "JAVA"}
		if query == LocalItemSuggestionQuery {
			coders = []string{"HAND", "AVRO"}
		}

		mutator := VaryCoderStrategy(coders)(
			SwapFasterCopy(
				RepeatRuns(10)(
					TimerMutator(
						ArrayBench(&benches),
					),
				),
			),
		)

		if err := mutator(logger, baseBench); err != nil {
			return nil, err
		}
	}

	return benches, nil
}

// Battery three, use bolt to craft the various setups.
func battery03(logger zerolog.Logger, store *Store, outdir string) error {
	sid := "first"

	if ok, err := store.HasSeries(sid); err != nil {
		return err
	} else if !ok {
		logger.Info().Msg("Creating series in database")
		benches, err := battery03GenerateBenchmarks(logger)
		if err != nil {
			return err
		}
		// for _, bench := range benches {
		// 	fmt.Printf("%#v\n", bench)
		// }

		if err := store.StoreSeries(sid, benches); err != nil {
			return err
		}
	}

	if err := store.RunSeries(sid); err != nil {
		logger.Error().Err(err).Msg("Couldn't run series")
		return err
	}

	return nil
}

// Battery two, create seperate outputfiles per query, to facilitate easier reruns
func battery02(logger zerolog.Logger, outdir string) error {

	// dir := filepath.Join(outdir, time.Now().UTC().Format("20060102150405"))
	// if err := os.Mkdir(dir, 0755); err != nil {
	// 	logger.Error().Err(err).Msg("Couldn't create directory")
	// 	return err
	// }
	// dir := filepath.Join(outdir, "patchy")
	dir := filepath.Join(outdir, "lucy")

	queries := AllQueries

	for _, query := range queries {
		logger := logger.With().Str("query", query).Logger()
		fname := filepath.Join(dir, query+".json")

		if ok, err := FileExists(fname); ok && err == nil {
			logger.Info().Msg("Skipping because of completed file")
			continue
		}

		aout, err := renameio.TempFile("", fname)
		if err != nil {
			return err
		}
		defer aout.Cleanup()

		bb := Benchmark{
			FlinkMaster: "[local]",
			// FlinkMaster:        "localhost",
			JavascriptFilename: fmt.Sprintf("%s/battery02.js", outdir),
			NumEvents:          IntPtr(MAX_EVENTS),
			Query:              query,
			Parallelism:        2,
		}

		coders := []string{"HAND", "AVRO", "JAVA"}
		if query == LocalItemSuggestionQuery {
			coders = []string{"HAND", "AVRO"}
		}

		mutator := VaryCoderStrategy(coders)(
			SwapFasterCopy(
				RepeatRuns(10)(
					TimerMutator(
						StoreBench(aout),
					),
				),
			),
		)

		if err := mutator(logger, bb); err != nil {
			return err
		}

		if err := aout.CloseAtomicallyReplace(); err != nil {
			return err
		}
	}
	return nil
}

// Battery one
//
// Simple test, create an aggregated output file with
func battery01(logger zerolog.Logger, outdir string) error {
	// Open a file for writing
	aout := fmt.Sprintf("%s/battery01.%s.json", outdir, time.Now().UTC().Format("20060102150405"))
	fp, err := os.OpenFile(aout, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer fp.Close()

	bb := Benchmark{
		FlinkMaster:        "[local]",
		JavascriptFilename: fmt.Sprintf("%s/battery01.js", outdir),
		NumEvents:          IntPtr(MAX_EVENTS),
		Query:              PassthroughQuery,
	}

	mutator := VaryQuery(AllQueries)(
		VaryCoderStrategy([]string{"HAND", "AVRO", "JAVA"})(
			SwapFasterCopy(
				RepeatRuns(10)(
					TimerMutator(
						StoreBench(fp),
					),
				),
			),
		),
	)

	if err := mutator(logger, bb); err != nil {
		return err
	}

	return nil
}
