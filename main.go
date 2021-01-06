package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/robertkrimen/otto"
	"go.uber.org/zap"
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

	NumEventGenerators int
	NumEvents          int

	FasterCopy bool
}

func (b *Benchmark) Run(logger *zap.Logger, gradlePath, beamPath string) ([]byte, error) {
	nargs := []string{
		"--runner=FlinkRunner",
		"--streaming",
		"--manageResources=false",
		"--monitorJobs=true",
		"--debug=true",
		fmt.Sprintf("--flinkMaster=%s", b.FlinkMaster),
		fmt.Sprintf("--query=%s", b.Query),
		fmt.Sprintf("--numEventGenerators=%d", b.NumEventGenerators),
		fmt.Sprintf("--numEvents=%d", b.NumEvents),
		fmt.Sprintf("--javascriptFilename=%s", b.JavascriptFilename),
		fmt.Sprintf("--fasterCopy=%t", b.FasterCopy),
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
func (b *Benchmark) AugmentResults(logger *zap.Logger) (*Result, error) {
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
func battery01(logger *zap.Logger, outdir string) error {
	bb := &Benchmark{
		FlinkMaster:        "[local]",
		JavascriptFilename: fmt.Sprintf("%s/battery01.js", outdir),
		// JavascriptFilename: fmt.Sprintf("%s/battery01.%s.js", outdir, time.Now().UTC().Format("20060102150405")),
		FasterCopy:         true,
		NumEventGenerators: 10,
		NumEvents:          100000,
		Query:              PassthroughQuery,
	}
	bb = bb

	for i := 0; i < 10; i++ {
		logger := logger.With(zap.Int("run", i))
		logger.Debug("Starting run")

		start := time.Now()
		gg, err := bb.Run(logger, GradlePath, BeamPath)
		dur := time.Since(start)

		if err != nil {
			logger.Error("Error during run", zap.Error(err))
			continue
		} else {
			logger.Debug("Finished run", zap.Duration("dur", dur), zap.Int("outputSize", len(gg)))
		}

		res, err := bb.AugmentResults(logger)
		if err != nil {
			logger.Error("Error during result augmentation", zap.Error(err))
			return err
		}
		res = res
		logger.Info("We have finished the run")

	}

	// gg, err := bb.Run(logger, GradlePath, BeamPath)
	// if err != nil {
	// 	return err
	// }
	// fmt.Printf("%s\n", gg)

	return nil
}

func main() {
	logger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatalf("Couldn't create zap logger: %s\n", err.Error())
	}
	defer logger.Sync()

	basedir := "/home/rhermes/commons/uni/thesis/beam-nexmark-benchmarks/results"
	if err := battery01(logger, basedir+"/battery01/"); err != nil {
		logger.Fatal("Couldn't run benchmark", zap.Error(err))
	}
}
