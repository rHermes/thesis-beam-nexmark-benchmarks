package main

import (
	"fmt"
	"log"
	"os/exec"
	"strings"
)

const (
	GradlePath = "/home/rhermes/build/beam/gradlew"
	BeamPath   = "/home/rhermes/build/beam"
	OutputPath = "/home/rhermes/commons/uni/thesis/beam-nexmark-benchmarks/results"
)

type Benchmark struct {
}

func main() {
	nexmarkArgs := []string{
		"--runner=FlinkRunner",
		"--streaming=true",
		"--streamTimeout=60",
		"--flinkMaster=[local]",
		"--manageResources=false",
		"--monitorJobs=true",
		"--javascriptFilename=" + OutputPath + "/wow.data.js",
		"--query=1",
		"--fasterCopy=true",
	}

	args := []string{
		"-p", BeamPath,
		"-Pnexmark.runner=:runners:flink:1.10",
		"-Pnexmark.args=" + strings.Join(nexmarkArgs, "\n"),
		":sdks:java:testing:nexmark:run"}
	c := exec.Command(GradlePath, args...)
	o, err := c.CombinedOutput()
	if err != nil {
		fmt.Printf("%s\n", o)
		log.Fatal(err.Error())
	}

	fmt.Printf("%s\n", o)

}
