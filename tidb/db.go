package tidb

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path"
	"strings"
	"time"

	"github.com/siddontang/chaos/pkg/core"
	"github.com/siddontang/chaos/pkg/util"
)

const (
	//archiveURL = "http://hxm-server/tidb-v1.0.8-linux-amd64.tar.gz"
	archiveURL   = "file:///root/tidb-v1.0.8-linux-amd64.tar.gz"
	deployDir    = "/opt/tidb"
	SERVICE_ALL  = "all"
	SERVICE_PD   = "pd"
	SERVICE_TIKV = "tikv"
	SERVICE_TIDB = "tidb"
)

var (
	pdBinary   = path.Join(deployDir, "./bin/pd-server")
	tikvBinary = path.Join(deployDir, "./bin/tikv-server")
	tidbBinary = path.Join(deployDir, "./bin/tidb-server")

	pdConfig   = path.Join(deployDir, "./conf/pd.toml")
	tikvConfig = path.Join(deployDir, "./conf/tikv.toml")

	pdLog   = path.Join(deployDir, "./log/pd.log")
	tikvLog = path.Join(deployDir, "./log/tikv.log")
	tidbLog = path.Join(deployDir, "./log/tidb.log")
)

// db is the TiDB database.
type db struct {
	nodes       []string
	currentNode string
}

func init() {
	d := new(db)

	//Refactor:init
	d.nodes = []string{"pd", "n1", "n2", "n3", "n4", "n5"}

	name, err := os.Hostname()
	if err != nil {
		log.Fatalf("db init error, cannot get hostname %v", err)
		panic(err)
		os.Exit(1)
	}
	d.currentNode = name
	core.RegisterDB(d)
}

// SetUp initializes the database.
func (db *db) SetUp(ctx context.Context) error {
	// Try kill all old servers
	exec.CommandContext(ctx, "killall", "-9", "tidb-server").Run()
	exec.CommandContext(ctx, "killall", "-9", "tikv-server").Run()
	exec.CommandContext(ctx, "killall", "-9", "pd-server").Run()

	//db.nodes = nodes

	if err := util.InstallArchive(ctx, archiveURL, deployDir); err != nil {
		log.Println("Install db error.")
		return err
	}

	os.MkdirAll(path.Join(deployDir, "conf"), 0755)
	os.MkdirAll(path.Join(deployDir, "log"), 0755)

	if err := ioutil.WriteFile(pdConfig, []byte("[replication]\nmax-replicas=5\n[log]\nlevel = \"debug\""), 0644); err != nil {
		log.Println("Write PD config file error.")
		return err
	}

	tikvCfs := []string{
		"[raftstore]",
		"pd-heartbeat-tick-interval=\"500ms\"",
		"pd-store-heartbeat-tick-interval=\"1s\"",
		"raft_store_max_leader_lease=\"900ms\"",
		"raft_base_tick_interval=\"100ms\"",
		"raft_heartbeat_ticks=3",
		"raft_election_timeout_ticks=10",
	}

	if err := ioutil.WriteFile(tikvConfig, []byte(strings.Join(tikvCfs, "\n")), 0644); err != nil {
		log.Println("Write TiKV config file error.")
		return err
	}

	return nil
	//return db.start(ctx, node, true)
}

// TearDown tears down the database.
func (db *db) TearDown(ctx context.Context) error {
	return db.Kill(ctx, SERVICE_ALL)
}

// Start starts the services in database
func (db *db) Start(ctx context.Context, service string) error {
	switch service {
	case SERVICE_PD:
		return db.startPD(ctx)
	case SERVICE_TIKV:
		return db.startKV(ctx)
	case SERVICE_TIDB:
		return db.startTiDB(ctx)
	default:
		return fmt.Errorf("start service %s not recognized", service)
	}

}

func (db *db) startPD(ctx context.Context) error {
	node := db.currentNode
	pdArgs := []string{
		fmt.Sprintf("--name=%s", node),
		"--data-dir=pd",
		"--client-urls=http://0.0.0.0:2379",
		"--peer-urls=http://0.0.0.0:2380",
		fmt.Sprintf("--advertise-client-urls=http://%s:2379", node),
		fmt.Sprintf("--advertise-peer-urls=http://%s:2380", node),
		fmt.Sprintf("--log-file=%s", pdLog),
		fmt.Sprintf("--config=%s", pdConfig),
	}

	initialClusterArgs := fmt.Sprintf("%s=http://%s:2380", node, node)
	pdArgs = append(pdArgs, fmt.Sprintf("--initial-cluster=%s", initialClusterArgs))
	//
	/*} else {
		pdArgs = append(pdArgs, fmt.Sprint("--join=http://n1:2379"))
	}*/

	log.Printf("start pd-server on node %s with %s", node, pdArgs)
	pdPID := path.Join(deployDir, "pd.pid")
	opts := util.NewDaemonOptions(deployDir, pdPID)
	if err := util.StartDaemon(ctx, opts, pdBinary, pdArgs...); err != nil {
		return err
	}

	time.Sleep(5 * time.Second)

	if !util.IsDaemonRunning(ctx, pdBinary, pdPID) {
		return fmt.Errorf("fail to start pd on node %s", node)
	}
	return nil
}

func (db *db) startKV(ctx context.Context) error {
	/*pdEndpoints := make([]string, len(db.nodes))
	for i, n := range db.nodes {
		pdEndpoints[i] = fmt.Sprintf("%s:2379", n)
	}*/
	pdEndpoints := []string{"pd:2379"}
	node := db.currentNode
	tikvArgs := []string{
		fmt.Sprintf("--pd=%s", strings.Join(pdEndpoints, ",")),
		//fmt.Sprintf("--pd=%s", pdEndpoints),
		"--addr=0.0.0.0:20160",
		fmt.Sprintf("--advertise-addr=%s:20160", node),
		"--data-dir=tikv",
		fmt.Sprintf("--log-file=%s", tikvLog),
		fmt.Sprintf("--config=%s", tikvConfig),
	}

	log.Printf("start tikv-server on node %s with %s", node, tikvArgs)
	tikvPID := path.Join(deployDir, "tikv.pid")
	opts := util.NewDaemonOptions(deployDir, tikvPID)
	if err := util.StartDaemon(ctx, opts, tikvBinary, tikvArgs...); err != nil {
		return err
	}

	time.Sleep(5 * time.Second)

	if !util.IsDaemonRunning(ctx, tikvBinary, tikvPID) {
		return fmt.Errorf("fail to start tikv on node %s", node)
	}

	return nil
}

func (db *db) startTiDB(ctx context.Context) error {
	pdEndpoints := []string{"pd:2379"}
	node := db.currentNode

	tidbArgs := []string{
		"--store=tikv",
		fmt.Sprintf("--path=%s", strings.Join(pdEndpoints, ",")),
		fmt.Sprintf("--log-file=%s", tidbLog),
	}

	log.Printf("start tidb-erver on node %s", node)
	tidbPID := path.Join(deployDir, "tidb.pid")
	opts := util.NewDaemonOptions(deployDir, tidbPID)
	if err := util.StartDaemon(ctx, opts, tidbBinary, tidbArgs...); err != nil {
		return err
	}

	time.Sleep(5 * time.Second)

	if !util.IsDaemonRunning(ctx, tidbBinary, tidbPID) {
		return fmt.Errorf("fail to start tidb on node %s", node)
	}

	return nil
}

func (db *db) start(ctx context.Context, inSetUp bool) error {
	initialClusterArgs := make([]string, len(db.nodes))
	for i, n := range db.nodes {
		initialClusterArgs[i] = fmt.Sprintf("%s=http://%s:2380", n, n)
	}
	node := db.currentNode
	pdArgs := []string{
		fmt.Sprintf("--name=%s", node),
		"--data-dir=pd",
		"--client-urls=http://0.0.0.0:2379",
		"--peer-urls=http://0.0.0.0:2380",
		fmt.Sprintf("--advertise-client-urls=http://%s:2379", node),
		fmt.Sprintf("--advertise-peer-urls=http://%s:2380", node),
		fmt.Sprintf("--initial-cluster=%s", strings.Join(initialClusterArgs, ",")),
		fmt.Sprintf("--log-file=%s", pdLog),
		fmt.Sprintf("--config=%s", pdConfig),
	}

	log.Printf("start pd-server on node %s", node)
	pdPID := path.Join(deployDir, "pd.pid")
	opts := util.NewDaemonOptions(deployDir, pdPID)
	if err := util.StartDaemon(ctx, opts, pdBinary, pdArgs...); err != nil {
		return err
	}

	if inSetUp {
		time.Sleep(5 * time.Second)
	}

	if !util.IsDaemonRunning(ctx, pdBinary, pdPID) {
		return fmt.Errorf("fail to start pd on node %s", node)
	}

	pdEndpoints := make([]string, len(db.nodes))
	for i, n := range db.nodes {
		pdEndpoints[i] = fmt.Sprintf("%s:2379", n)
	}

	tikvArgs := []string{
		fmt.Sprintf("--pd=%s", strings.Join(pdEndpoints, ",")),
		"--addr=0.0.0.0:20160",
		fmt.Sprintf("--advertise-addr=%s:20160", node),
		"--data-dir=tikv",
		fmt.Sprintf("--log-file=%s", tikvLog),
		fmt.Sprintf("--config=%s", tikvConfig),
	}

	log.Printf("start tikv-server on node %s", node)
	tikvPID := path.Join(deployDir, "tikv.pid")
	opts = util.NewDaemonOptions(deployDir, tikvPID)
	if err := util.StartDaemon(ctx, opts, tikvBinary, tikvArgs...); err != nil {
		return err
	}

	if inSetUp {
		time.Sleep(30 * time.Second)
	}

	if !util.IsDaemonRunning(ctx, tikvBinary, tikvPID) {
		return fmt.Errorf("fail to start tikv on node %s", node)
	}

	tidbArgs := []string{
		"--store=tikv",
		fmt.Sprintf("--path=%s", strings.Join(pdEndpoints, ",")),
		fmt.Sprintf("--log-file=%s", tidbLog),
	}

	log.Printf("start tidb-erver on node %s", node)
	tidbPID := path.Join(deployDir, "tidb.pid")
	opts = util.NewDaemonOptions(deployDir, tidbPID)
	if err := util.StartDaemon(ctx, opts, tidbBinary, tidbArgs...); err != nil {
		return err
	}

	if inSetUp {
		time.Sleep(30 * time.Second)
	}

	if !util.IsDaemonRunning(ctx, tidbBinary, tidbPID) {
		return fmt.Errorf("fail to start tidb on node %s", node)
	}

	return nil
}

// Stop stops the database
func (db *db) Stop(ctx context.Context, service string) error {
	if err := util.StopDaemon(ctx, tidbBinary, path.Join(deployDir, "tidb.pid")); err != nil {
		return err
	}

	if err := util.StopDaemon(ctx, tikvBinary, path.Join(deployDir, "tikv.pid")); err != nil {
		return err
	}

	if err := util.StopDaemon(ctx, pdBinary, path.Join(deployDir, "pd.pid")); err != nil {
		return err
	}

	return nil
}

// Kill kills the database, default kill all
func (db *db) Kill(ctx context.Context, service string) error {
	log.Printf("kill service %s", service)

	switch service {
	case SERVICE_TIDB:
		if err := util.KillDaemon(ctx, tidbBinary, path.Join(deployDir, "tidb.pid")); err != nil {
			return err
		}
	case SERVICE_TIKV:
		if err := util.KillDaemon(ctx, tikvBinary, path.Join(deployDir, "tikv.pid")); err != nil {
			return err
		}
	case SERVICE_PD:
		if err := util.KillDaemon(ctx, pdBinary, path.Join(deployDir, "pd.pid")); err != nil {
			return err
		}

	default:
		if err := util.KillDaemon(ctx, tidbBinary, path.Join(deployDir, "tidb.pid")); err != nil {
			return err
		}
		if err := util.KillDaemon(ctx, tikvBinary, path.Join(deployDir, "tikv.pid")); err != nil {
			return err
		}
		if err := util.KillDaemon(ctx, pdBinary, path.Join(deployDir, "pd.pid")); err != nil {
			return err
		}
	}

	return nil
}

// IsRunning checks whether the database is running or not
func (db *db) IsRunning(ctx context.Context, service string) bool {
	var s string
	switch service {
	case SERVICE_TIDB:
		s = "tidb.pid"
	case SERVICE_PD:
		s = "pd.pid"
	case SERVICE_TIKV:
		s = "tikv.pid"
	default:
		log.Printf("[IsRunning]service %s not recognized", service)
		return false
	}
	return util.IsDaemonRunning(ctx, tidbBinary, path.Join(deployDir, s))
}

// Name returns the unique name for the database
func (db *db) Name() string {
	return "tidb"
}

func (db *db) Node() string {
	return db.currentNode
}
