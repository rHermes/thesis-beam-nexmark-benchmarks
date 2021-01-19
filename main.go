package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

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

	DefaultAveragePersonByteSize  = 200
	DefaultAverageAuctionByteSize = 500
	DefaultAverageBidByteSize     = 100
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

	// Queries that finish in a reasonable time.
	NormalQueries = []string{
		PassthroughQuery,
		CurrencyConversionQuery,
		SelectionQuery,
		LocalItemSuggestionQuery,
		AveragePriceForCategoryQuery,
		HotItemsQuery,
		AverageSellingPriceBySellerQuery,
		MonitorNewUsersQuery,
		WinningBidsQuery,
		UserSessionsQuery,
		ProcessingTimeWindowsQuery,
		SessionSideInputJoinQuery,
	}

	// FastQueries
	FastQueries = []string{
		PassthroughQuery,
		CurrencyConversionQuery,
		SelectionQuery,
		LocalItemSuggestionQuery,
		MonitorNewUsersQuery,
	}
)

type Benchmark struct {
	FlinkMaster        string
	JavascriptFilename string `json:"-"`
	Query              string
	Parallelism        int

	NumEventGenerators *int
	NumEvents          *int

	AveragePersonByteSize  *int
	AverageAuctionByteSize *int
	AverageBidByteSize     *int

	CoderStrategy string

	FasterCopy bool
}

func (b *Benchmark) Run(logger zerolog.Logger, gradlePath, beamPath string) ([]byte, []byte, error) {
	nargs := []string{
		"--runner=FlinkRunner",
		"--streaming",
		"--streamTimeout=60",
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

	if b.AveragePersonByteSize != nil {
		nargs = append(nargs, fmt.Sprintf("--avgPersonByteSize=%d", *b.AveragePersonByteSize))
	}
	if b.AverageAuctionByteSize != nil {
		nargs = append(nargs, fmt.Sprintf("--avgAuctionByteSize=%d", *b.AverageAuctionByteSize))
	}
	if b.AverageBidByteSize != nil {
		nargs = append(nargs, fmt.Sprintf("--avgBidByteSize=%d", *b.AverageBidByteSize))
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
	c.Stderr = &stderr
	c.Stdout = &stdout

	if false {
		c.Stderr = io.MultiWriter(os.Stderr, &stderr)
		c.Stdout = io.MultiWriter(os.Stdout, &stdout)
	}
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
	basedir := "/home/rhermes/commons/uni/thesis/beam-nexmark-benchmarks/results"

	opts := func(w *zerolog.ConsoleWriter) {
		w.NoColor = true
		w.TimeFormat = time.Stamp
	}
	logger := zerolog.New(zerolog.NewConsoleWriter(opts)).
		With().Timestamp().Logger().Level(zerolog.InfoLevel)
	// logger = logger.Level(zerolog.InfoLevel)

	store, err := NewStore(logger, basedir+"/dbs/proto.db")
	if err != nil {
		logger.Fatal().Err(err).Msg("Couldn't open the store")
	}
	defer store.Close()

	// if err := storeBattery(logger, store, basedir, "first", battery03GenerateBenchmarks); err != nil {
	// 	logger.Fatal().Err(err).Msg("couldn't run the battery")
	// }
	// if err := storeBattery(logger, store, basedir, "bat04-06", battery04GenerateBenchmarks); err != nil {
	// 	logger.Fatal().Err(err).Msg("couldn't run the battery")
	// }
	if err := storeBattery(logger, store, basedir, "bat05-02", battery05GenerateBenchmarks); err != nil {
		logger.Fatal().Err(err).Msg("couldn't run the battery")
	}

}

func storeBattery(logger zerolog.Logger, store *Store, outdir, sid string, genBench func(logger zerolog.Logger) ([]Benchmark, error)) error {
	if ok, err := store.HasSeries(sid); err != nil {
		return err
	} else if !ok {
		logger.Info().Msg("Creating series in database")
		benches, err := genBench(logger)
		if err != nil {
			return err
		}
		logger.Info().Int("benches", len(benches)).Msg("Generated benches")

		if err := store.StoreSeries(sid, benches); err != nil {
			return err
		}
	}

	if err := store.RunSeries(sid); err != nil {
		logger.Error().Err(err).Msg("Couldn't run series")
		return err
	}

	runs, err := store.GetSeriesResults(sid)
	if err != nil {
		logger.Error().Err(err).Msg("Couldn't retrieve series results")
		return err
	}

	fr, err := os.Create(filepath.Join(outdir, sid+".json"))
	if err != nil {
		return err
	}
	defer fr.Close()

	jec := json.NewEncoder(fr)

	for _, run := range runs {
		if err := jec.Encode(run); err != nil {
			return err
		}
	}

	return nil
}

func battery05GenerateBenchmarks(logger zerolog.Logger) ([]Benchmark, error) {
	baseBench := Benchmark{
		FlinkMaster:   "[local]",
		NumEvents:     IntPtr(MAX_EVENTS * 5),
		CoderStrategy: "HAND",
		Parallelism:   8,
		Query:         CurrencyConversionQuery,
	}

	var benches []Benchmark
	mutator := VaryAvgBidSize(DefaultAverageBidByteSize, 10*DefaultAverageBidByteSize, DefaultAverageBidByteSize)(
		SwapFasterCopy(
			RepeatRuns(10)(
				ArrayBench(&benches),
			),
		),
	)

	if err := mutator(logger, baseBench); err != nil {
		return nil, err
	}

	return benches, nil
}

// Battery04: Check how the difference changes parallelism is changed
func battery04GenerateBenchmarks(logger zerolog.Logger) ([]Benchmark, error) {
	baseBench := Benchmark{
		FlinkMaster:   "[local]",
		NumEvents:     IntPtr(MAX_EVENTS * 5),
		CoderStrategy: "HAND",
	}

	var benches []Benchmark
	mutator := UseParallelism([]int{1, 2, 4, 8})(
		VaryQuery(NormalQueries)(
			SwapFasterCopy(
				RepeatRuns(10)(
					ArrayBench(&benches),
				),
			),
		),
	)

	if err := mutator(logger, baseBench); err != nil {
		return nil, err
	}

	return benches, nil
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
