package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
	"fmt"
	"strconv"

	"github.com/siddontang/chaos/pkg/control"
	"github.com/siddontang/chaos/pkg/core"
	"github.com/siddontang/chaos/pkg/history"
	"github.com/siddontang/chaos/pkg/nemesis"
	"github.com/siddontang/chaos/tidb"
)

var (
	action = flag.String("action", "run", "action:run, setupdb, "+
		"startpd, startkv, starttidb, startclient, shutdownclient, killkv")
	n            = flag.String("nodes", "", "nodes: 1,2,3,4, 5")
	initData     = flag.Bool("initData", false, "if init data in database")
	nodePort     = flag.Int("node-port", 8080, "node port")
	requestCount = flag.Int("request-count", 500, "client test request count")
	runTime      = flag.Duration("run-time", 10*time.Minute, "client test run time")
	clientCase   = flag.String("case", "bank", "client test case, like bank")
	historyFile  = flag.String("history", "./history.log", "history file")
	nemesises    = flag.String("nemesis", "", "nemesis, seperated by name, like random_kill,all_kill")
	nn           = flag.String("nemesis-nodes", "", "nemesis-nodes: 1,2,3,4,5")
)

func main() {
	flag.Parse()

	cfg := &control.Config{
		DB:           "tidb",
		NodePort:     *nodePort,
		RequestCount: *requestCount,
		RunTime:      *runTime,
		History:      *historyFile,
	}

	var (
		creator     core.ClientCreator
		verifier    history.Verifier
		nemesisGens []core.NemesisGenerator
	)

	switch *clientCase {
	case "bank":
		creator = tidb.BankClientCreator{}
		verifier = tidb.BankVerifier{}
	default:
		log.Fatalf("invalid client test case %s", *clientCase)
	}

	for _, name := range strings.Split(*nemesises, ",") {
		var g core.NemesisGenerator
		name := strings.TrimSpace(name)
		if len(name) == 0 {
			continue
		}

		switch name {
		case "random_kill", "all_kill", "minor_kill", "major_kill":
			g = nemesis.NewKillGenerator("tidb", name)
		case "random_drop", "all_drop", "minor_drop", "major_drop":
			g = nemesis.NewDropGenerator(name)
		default:
			log.Fatalf("invalid nemesis generator")
		}

		nemesisGens = append(nemesisGens, g)
	}

	c := control.NewController(cfg, creator, nemesisGens)

	var ns []int
	if *n == "" {
		ns = []int{}
	} else {
		nss := strings.Split(*n, ",")
		ns = make([]int, len(nss))
		for i, v := range nss {
			if x, err := strconv.Atoi(v); err == nil {
				ns[i] = x
			} else {
				panic(fmt.Sprintf("node value error %s", v))
			}
		}
	}

	var nemesisNodes []int
	if *nn == "" {
		nemesisNodes = []int{}
	} else {
		ss := strings.Split(*nn, ",")
		nemesisNodes = make([]int, len(ss))
		for i, v := range ss {
			if x, err := strconv.Atoi(v); err == nil {
				nemesisNodes[i] = x
			} else {
				panic(fmt.Sprintf("nemesis-ndoes values error %s", ss))
			}
		}
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		<-sigs
		c.Close()
		cancel()
	}()

	switch *action {
	case "setupdb":
		c.SetupDBs()
		cancel()
	case "startpd":
		c.StartPD()
		cancel()
	case "startkv":
		c.StartKVs(ns)
		cancel()
	case "starttidb":
		c.StartTiDBs(ns)
		cancel()

	case "killkv":
		c.KillServices(ns, tidb.SERVICE_TIKV)
		cancel()

	case "run":
		fmt.Printf("run client with initdata %v on %v and nemesis %v\n", *initData, ns, nemesisNodes)

		c.Run(ns, *initData, nemesisNodes)

		// Verify may take a long time, we should quit ASAP if receive signal.
		go func() {
			ok, err := verifier.Verify(*historyFile)
			if err != nil {
				log.Fatalf("verify history failed %v", err)
			}

			if !ok {
				log.Fatalf("%s history %s is not linearizable", *clientCase, *historyFile)
			} else {
				log.Printf("%s history %s is linearizable", *clientCase, *historyFile)
			}

			cancel()
		}()

	default:
		panic("unrecognized control action")
	}

	fmt.Println("Done")
	<-ctx.Done()
}
