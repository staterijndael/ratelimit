package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"
)

const StdinReplacer = "{}"

type Args struct {
	Command   string
	Opts      []string
	Rate      int
	InFlight  int
	StdinChan chan string
}

func main() {
	args, err := getArgs()
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		os.Exit(1)
	}

	args.startReadingStdin()
	run(args)
}

func run(args *Args) {
	rateLimitChan := make(chan bool, args.InFlight)
	for i := 0; i < args.InFlight; i++ {
		rateLimitChan <- true
	}

	go func() {
		tickChan := time.Tick(time.Second / time.Duration(args.Rate))

		for range tickChan {
			for i := 0; i < args.InFlight; i++ {
				if len(rateLimitChan) == args.InFlight {
					break
				}

				rateLimitChan <- true
			}
		}
	}()

	inflightLimitChan := make(chan bool, args.InFlight)
	for i := 0; i < args.InFlight; i++ {
		inflightLimitChan <- true
	}

	wg := sync.WaitGroup{}

	for stdinArg := range args.StdinChan {
		wg.Add(1)

		replacedOpts := make([]string, 0, len(args.Opts))
		for _, opt := range args.Opts {
			replacedOpts = append(replacedOpts, strings.ReplaceAll(opt, StdinReplacer, stdinArg))
		}

		<-inflightLimitChan
		<-rateLimitChan

		go func(command string, opts []string) {
			defer func() {
				inflightLimitChan <- true

				wg.Done()
			}()

			cmd := exec.Command(command, opts...)

			cmd.Stdout = os.Stdout
			err := cmd.Run()
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
		}(args.Command, replacedOpts)
	}
	wg.Wait()
}

func getArgs() (*Args, error) {
	args := &Args{
		StdinChan: make(chan string),
	}

	ok, err := isStdin()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.New("input must be only from stdin")
	}

	rate := flag.Int("rate", 1, "max rate per second for commands")
	inFlight := flag.Int("inflight", 1, "max number of parallel working commands")
	flag.Parse()

	if len(flag.Args()) == 0 {
		return nil, errors.New("command to launch is not specified")
	}

	args.Rate = *rate
	args.InFlight = *inFlight

	args.Command = flag.Arg(0)

	for i := 1; i < len(flag.Args()); i++ {
		args.Opts = append(args.Opts, flag.Arg(i))
	}

	return args, nil
}

func (a *Args) startReadingStdin() {
	go func() {
		reader := bufio.NewReader(os.Stdin)

		for {
			arg, err := reader.ReadString('\n')
			if err != nil {
				if err == io.EOF {
					break
				}

				fmt.Fprintf(os.Stderr, err.Error())
			}

			a.StdinChan <- arg
		}
		close(a.StdinChan)

	}()
}

func isStdin() (bool, error) {
	stat, err := os.Stdin.Stat()
	if err != nil {
		return false, err
	}
	return (stat.Mode() & os.ModeCharDevice) == 0, nil
}
