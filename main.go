package main

import (
	"errors"
	"fmt"
	"log"
	"os/exec"
	"strings"
	"time"

	"github.com/alecthomas/repr"
	"github.com/flink-go/api"
)

const (
	GradlePath = "/home/rhermes/build/beam/gradlew"
	BeamPath   = "/home/rhermes/build/beam"
	OutputPath = "/home/rhermes/commons/uni/thesis/beam-nexmark-benchmarks/results"
)

type Benchmark struct {
	FlinkMaster        string
	JavascriptFilename string
	Query              string
}

func (b *Benchmark) Run(gradlePath, beamPath string) ([]byte, error) {
	return nil, errors.New("Not implemented yet")
}

func simpleMetrics(c *api.Client, jobID string) error {
	res, err := c.Job(jobID)
	if err != nil {
		return err
	}
	repr.Println(res)

	// met, err := c.JobMetrics(api.JobMetricsOpts{
	// 	Jobs: []string{jobID},
	// })
	// if err != nil {
	// 	return err
	// }

	// fmt.Println("==== WTF ====")
	// repr.Println(met)

	return nil
}

func cc() error {
	c, err := api.New("127.0.0.1:8081")
	if err != nil {
		return err
	}

	config, err := c.Config()
	if err != nil {
		return err
	}

	repr.Println(config)

	for {
		jobs, err := c.Jobs()
		if err != nil {
			return err
		}

		log.Printf("Jobs:\n")
		for _, job := range jobs.Jobs {
			if job.Status == "FINISHED" || job.Status == "FAILED" {
				continue
			}
			repr.Print(job)

			if err := simpleMetrics(c, job.ID); err != nil {
				return err
			}
		}
		time.Sleep(1 * time.Second)
	}

	return nil
}

func main() {
	go func() {
		if err := cc(); err != nil {
			log.Fatalf("THis happened: %s\n", err.Error())
		}
	}()
	nexmarkArgs := []string{
		"--runner=FlinkRunner",
		"--streaming=true",
		// "--streamTimeout=600",
		// "--flinkMaster=[local]",
		"--flinkMaster=localhost",
		"--manageResources=false",
		"--monitorJobs=true",
		"--javascriptFilename=" + OutputPath + "/wow.data.js",
		"--query=PASSTHROUGH",
		"--fasterCopy=true",
		// "--numEvents=0",
		// "--isRateLimited=true",
		// "--firstEventRate=200",
		// "--numEvents=10000000000",
		// "--numEvents=10000",
		"--numEvents=100000000",
		// "--numEvents=10000000000",
		// "--rateUnit=PER_MINUTE",
	}

	args := []string{
		"-p", BeamPath,
		"-Pnexmark.runner=:runners:flink:1.10",
		"-Pnexmark.args=" + strings.Join(nexmarkArgs, "\n"),
		":sdks:java:testing:nexmark:run",
	}
	args = args

	c := exec.Command(GradlePath, args...)
	o, err := c.CombinedOutput()
	if err != nil {
		fmt.Printf("%s\n", o)
		log.Fatal(err.Error())
	}

	fmt.Printf("%s\n", o)

}
