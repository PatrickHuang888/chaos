package control

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	"github.com/siddontang/chaos/pkg/core"
	"github.com/siddontang/chaos/pkg/history"
	"github.com/siddontang/chaos/pkg/node"
	"github.com/siddontang/chaos/tidb"
	"time"
)

// Controller controls the whole cluster. It sends request to the database,
// and also uses nemesis to disturb the cluster.
// Here have only 5 nodes, and the hosts are n1 - n5.
type Controller struct {
	cfg *Config

	nodes       []string
	nodeClients []*node.Client

	clients []core.Client

	nemesisGenerators []core.NemesisGenerator

	ctx    context.Context
	cancel context.CancelFunc

	proc int64

	recorder *history.Recorder
}

// NewController creates a controller.
func NewController(cfg *Config, clientCreator core.ClientCreator, nemesisGenerators []core.NemesisGenerator) *Controller {
	cfg.adjust()

	if len(cfg.DB) == 0 {
		log.Fatalf("empty database")
	}

	r, err := history.NewRecorder(cfg.History)
	if err != nil {
		log.Fatalf("prepare history failed %v", err)
	}

	c := new(Controller)
	c.cfg = cfg
	c.ctx, c.cancel = context.WithCancel(context.Background())
	c.recorder = r
	c.nemesisGenerators = nemesisGenerators

	name := "pd"
	c.nodes = append(c.nodes, name)
	client := node.NewClient(name, fmt.Sprintf("%s:%d", name, cfg.NodePort))
	c.nodeClients = append(c.nodeClients, client)
	c.clients = append(c.clients, clientCreator.Create(name))
	//db client
	for i := 1; i <= 5; i++ {
		name := fmt.Sprintf("n%d", i)
		c.nodes = append(c.nodes, name)
		client := node.NewClient(name, fmt.Sprintf("%s:%d", name, cfg.NodePort))
		c.nodeClients = append(c.nodeClients, client)
		c.clients = append(c.clients, clientCreator.Create(name))
	}

	return c
}

// Close closes the controller.
func (c *Controller) Close() {
	c.cancel()
}

// Run runs the controller.
func (c *Controller) Run(ns []int, initData bool, nemesisNodes []int) {
	//c.SetupDB()
	c.SetupClients(ns, initData)

	n := len(ns)
	var clientWg sync.WaitGroup
	clientWg.Add(n)
	for _, i := range ns {
		go func(i int) {
			defer clientWg.Done()
			c.onClientLoop(i)
		}(i)
	}

	time.Sleep(5 * time.Second)

	ctx, cancel := context.WithCancel(c.ctx)

	var nemesisWg sync.WaitGroup
	nemesisWg.Add(1)
	go func() {
		defer nemesisWg.Done()
		c.dispatchNemesis(ctx, nemesisNodes)
	}()

	clientWg.Wait()

	cancel()

	nemesisWg.Wait()

	//c.ShutdownClient()
	//c.TearDownDB()
	c.CloseClients(ns)

	c.recorder.Close()
}

/*func (c *Controller) syncExec(f func(i int)) {
	var wg sync.WaitGroup
	n := len(c.nodes)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			f(i)
		}(i)
	}
	wg.Wait()
}*/

/*func (c *Controller) SetUpDB(i int) {
	if i == -1 {
		c.syncExec(c.setupdb)
	} else {
		c.setupdb(i)
	}
}*/

func (c *Controller) syncExec(ns []int, f func(i int)) {
	var wg sync.WaitGroup
	n := len(ns)
	wg.Add(n)
	for _, i := range ns {
		go func(i int) {
			defer wg.Done()
			f(i)
		}(i)
	}
	wg.Wait()
}

func (c *Controller) SetupDBs() {
	ns := make([]int, len(c.nodes))
	for i, _ := range c.nodes {
		ns[i] = i
	}
	c.syncExec(ns, c.SetupDB)
}

func (c *Controller) SetupDB(i int) {
	client := c.nodeClients[i]
	log.Printf("set up database on %s", c.nodes[i])
	if err := client.SetUpDB(c.cfg.DB, c.nodes); err != nil {
		log.Fatalf("setup db %s at node %s failed %v", c.cfg.DB, c.nodes[i], err)
	}
}

/*func (c *Controller) StartPD(i int) {
	if i == -1 {
		c.syncExec(c.startPD)
	} else {
		c.startPD(i)
	}
}*/

func (c *Controller) StartPD() {
	//pd
	client := c.nodeClients[0]
	log.Printf("start pd on node %s", c.nodes[0])
	if err := client.Start(c.cfg.DB, tidb.SERVICE_PD); err != nil {
		log.Fatalf("start pd at node %s failed %v", c.nodes[0], err)
	}
}

func (c *Controller) StartKVs(ns []int) {
	c.syncExec(ns, c.StartKV)
}

func (c *Controller) StartKV(i int) {
	client := c.nodeClients[i]
	log.Printf("start kv on node %s", c.nodes[i])
	if err := client.Start(c.cfg.DB, tidb.SERVICE_TIKV); err != nil {
		log.Fatalf("start kv at node %s failed %v", c.nodes[i], err)
	}
}

func (c *Controller) StartTiDBs(ns []int) {
	c.syncExec(ns, c.StartTiDB)
}
func (c *Controller) StartTiDB(i int) {
	client := c.nodeClients[i]
	log.Printf("start tidb on node %s", c.nodes[i])
	if err := client.Start(c.cfg.DB, tidb.SERVICE_TIDB); err != nil {
		log.Fatalf("start tidb at node %s failed %v", c.nodes[i], err)
	}
}

func (c *Controller) SetupClients(ns []int, initData bool) {
	var wg sync.WaitGroup
	nl := len(ns)
	wg.Add(nl)
	for i, n := range ns {
		go func(i int, n int) {
			defer wg.Done()
			if initData {
				c.SetupClient(n, i == 0)
			} else {
				c.SetupClient(n, false)
			}
		}(i, n)
	}
	wg.Wait()
}
func (c *Controller) SetupClient(i int, initData bool) {
	node := c.nodes[i]
	dbClient := c.clients[i]
	log.Printf("setup db client for node %s", node)
	if err := dbClient.Setup(c.ctx, node, initData); err != nil {
		log.Fatalf("set up db client for node %s failed %v", node, err)
	}

}

func (c *Controller) CloseClients(ns []int) {
	c.syncExec(ns, c.CloseClient)
}
func (c *Controller) CloseClient(i int) {
	client := c.clients[i]
	node := c.nodes[i]
	log.Printf("shut down db client for node %s", node)
	if err := client.Close(c.ctx, c.nodes, node); err != nil {
		log.Printf("shut down db client for node %s failed %v", node, err)
	}
}

func (c *Controller) KillServices(ns []int, service string) {
	var wg sync.WaitGroup
	n := len(ns)
	wg.Add(n)
	for _, i := range ns {
		go func(i int) {
			defer wg.Done()
			c.Kill(i, service)
		}(i)
	}
	wg.Wait()
}
func (c *Controller) Kill(i int, service string) {
	client := c.nodeClients[i]
	log.Printf("kill service %s on %s", service, c.nodes[i])
	if err := client.Kill(c.cfg.DB, service); err != nil {
		log.Printf("kill services %s at node %s failed %v", service, c.nodes[i], err)
	}
}

/*func (c *Controller) SetUpClient() {
	log.Printf("begin to set up client")
	c.syncExec(func(i int) {
		client := c.clients[i]
		node := c.nodes[i]
		log.Printf("begin to set up db client for node %s", node)
		if err := client.Start(c.ctx, c.nodes, node); err != nil {
			log.Fatalf("set up db client for node %s failed %v", node, err)
		}
	})
}*/

func (c *Controller) onClientLoop(i int) {
	client := c.clients[i]
	node := c.nodes[i]
	log.Printf("client %v running", i)

	ctx, cancel := context.WithTimeout(c.ctx, c.cfg.RunTime)
	defer cancel()

	for i := 0; i < c.cfg.RequestCount; i++ {
		procID := atomic.AddInt64(&c.proc, 1)

		request := client.NextRequest()

		if err := c.recorder.RecordRequest(procID, request); err != nil {
			log.Fatalf("record request %v failed %v", request, err)
		}

		response := client.Invoke(ctx, node, request)

		if err := c.recorder.RecordResponse(procID, response); err != nil {
			log.Fatalf("record response %v failed %v", response, err)
		}

		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}

func (c *Controller) dispatchNemesis(ctx context.Context, ns []int) {
	if len(c.nemesisGenerators) == 0 {
		return
	}

	log.Printf("begin to run nemesis")
	var wg sync.WaitGroup
	var nodes []string
	for _, v := range ns {
		nodes = append(nodes, c.nodes[v])
	}

LOOP:
	//for {
		for _, g := range c.nemesisGenerators {
			select {
			case <-ctx.Done():
				break LOOP
			default:
			}

			log.Printf("begin to run %s nemesis generator on nodes %v", g.Name(), nodes)

			ops := g.Generate(nodes)

			wg.Add(len(ops))
			for i, op:= range ops {
				go c.onNemesisLoop(ctx, ns[i], op, &wg)
			}
			wg.Wait()
		}
	//}
	log.Printf("stop to run nemesis")
}

func (c *Controller) onNemesisLoop(ctx context.Context, index int, op *core.NemesisOperation, wg *sync.WaitGroup) {
	defer wg.Done()

	if op == nil {
		return
	}

	nodeClient := c.nodeClients[index]
	node := c.nodes[index]

	log.Printf("run nemesis %s on %s", op.Name, node)
	if err := nodeClient.RunNemesis(op); err != nil {
		log.Printf("run nemesis %s on %s failed: %v", op.Name, node, err)
	}
}
