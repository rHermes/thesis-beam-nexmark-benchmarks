package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
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
	PassthroughQuery = "PASSTHROUGH"
)

type Benchmark struct {
	FlinkMaster        string
	JavascriptFilename string
	Query              string

	NumEventGenerators *int
	NumEvents          *int

	FasterCopy bool
}

func (b *Benchmark) Run(logger zerolog.Logger, gradlePath, beamPath string) ([]byte, error) {
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
	}
	if b.NumEvents != nil {
		nargs = append(nargs, fmt.Sprintf("--numEvents=%d", *b.NumEvents))
	}
	if b.NumEventGenerators != nil {
		nargs = append(nargs, fmt.Sprintf("--numEventGenerators=%d", *b.NumEventGenerators))
	}
	args := []string{
		"-p", beamPath,
		"-Pnexmark.runner=:runners:flink:1.10",
		// "-Pnexmark.runner=:runners:direct-java",
		"-Pnexmark.args=" + strings.Join(nargs, "\n"),
		":sdks:java:testing:nexmark:run",
	}

	c := exec.Command(GradlePath, args...)
	o, err := c.CombinedOutput()
	if err != nil {
		return nil, err
	}
	return o, nil
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
			FasterCopy: b.FasterCopy,
		},
	}, nil
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
		// NumEvents:          100000,
		Query: PassthroughQuery,
	}

	mutator := SwapFasterCopy(
		RepeatRuns(10)(
			TimerMutator(
				StoreBench(fp),
			),
		),
	)

	if err := mutator(logger, bb); err != nil {
		return err
	}

	// for i := 0; i < 10; i++ {
	// 	logger := logger.With().Int("run", i).Logger()
	// 	logger.Debug().Msg("Starting run")

	// 	if err := mutator(logger, bb); err != nil {
	// 		logger.Error().Err(err).Msg("Error during run")
	// 		break
	// 	}
	// }

	return nil
}

func main() {
	opts := func(w *zerolog.ConsoleWriter) {
		w.NoColor = true
		w.TimeFormat = time.Stamp
	}
	logger := zerolog.New(zerolog.NewConsoleWriter(opts)).
		With().Timestamp().Logger().Level(zerolog.DebugLevel)
	// logger = logger.Level(zerolog.InfoLevel)

	basedir := "/home/rhermes/commons/uni/thesis/beam-nexmark-benchmarks/results"
	if err := battery01(logger, basedir+"/battery01/"); err != nil {
		logger.Fatal().Err(err).Msg("Couldn't run benchmark")
	}
}
