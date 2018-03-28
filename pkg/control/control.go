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

	//pd client
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
func (c *Controller) Run() {
	c.SetupDB()
	c.SetUpClient()

	n := len(c.nodes)
	var clientWg sync.WaitGroup
	clientWg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer clientWg.Done()
			c.onClientLoop(i)
		}(i)
	}

	ctx, cancel := context.WithCancel(c.ctx)
	var nemesisWg sync.WaitGroup
	nemesisWg.Add(1)
	go func() {
		defer nemesisWg.Done()
		c.dispatchNemesis(ctx)
	}()

	clientWg.Wait()

	cancel()
	nemesisWg.Wait()

	c.TearDownClient()
	c.TearDownDB()

	c.recorder.Close()
}

func (c *Controller) syncExec(f func(i int)) {
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
}

/*func (c *Controller) SetUpDB(i int) {
	if i == -1 {
		c.syncExec(c.setupdb)
	} else {
		c.setupdb(i)
	}
}*/

func (c *Controller) SetupDB() {
	log.Printf("begin to set up database")
	c.syncExec(func(i int) {
		client := c.nodeClients[i]
		log.Printf("begin to set up database on %s", c.nodes[i])
		if err := client.SetUpDB(c.cfg.DB, c.nodes); err != nil {
			log.Fatalf("setup db %s at node %s failed %v", c.cfg.DB, c.nodes[i], err)
		}
	})
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
	if err := client.StartPD(c.cfg.DB); err != nil {
		log.Fatalf("start pd at node %s failed %v", c.nodes[0], err)
	}
}

func (c *Controller) StartKV(i int) {
	client := c.nodeClients[i]
	log.Printf("start kv on node %s", c.nodes[i])
	if err := client.StartKV(c.cfg.DB); err != nil {
		log.Fatalf("start kv at node %s failed %v", c.nodes[i], err)
	}
}

func (c *Controller) StartTiDB(i int) {
	client := c.nodeClients[i]
	log.Printf("start tidb on node %s", c.nodes[i])
	if err := client.StartTiDB(c.cfg.DB); err != nil {
		log.Fatalf("start tidb at node %s failed %v", c.nodes[i], err)
	}
}

func (c *Controller) TearDownDB() {
	log.Printf("begin to tear down database")
	c.syncExec(func(i int) {
		client := c.nodeClients[i]
		log.Printf("being to tear down database on %s", c.nodes[i])
		if err := client.TearDownDB(c.cfg.DB, c.nodes); err != nil {
			log.Printf("tear down db %s at node %s failed %v", c.cfg.DB, c.nodes[i], err)
		}
	})
}

func (c *Controller) SetUpClient() {
	log.Printf("begin to set up client")
	c.syncExec(func(i int) {
		client := c.clients[i]
		node := c.nodes[i]
		log.Printf("begin to set up db client for node %s", node)
		if err := client.SetUp(c.ctx, c.nodes, node); err != nil {
			log.Fatalf("set up db client for node %s failed %v", node, err)
		}
	})
}



func (c *Controller) TearDownClient() {
	log.Printf("begin to set up client")
	c.syncExec(func(i int) {
		client := c.clients[i]
		node := c.nodes[i]
		log.Printf("begin to tear down db client for node %s", node)
		if err := client.TearDown(c.ctx, c.nodes, node); err != nil {
			log.Printf("tear down db client for node %s failed %v", node, err)
		}
	})
}

func (c *Controller) onClientLoop(i int) {
	client := c.clients[i]
	node := c.nodes[i]

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

func (c *Controller) dispatchNemesis(ctx context.Context) {
	if len(c.nemesisGenerators) == 0 {
		return
	}

	log.Printf("begin to run nemesis")
	var wg sync.WaitGroup
	n := len(c.nodes)
LOOP:
	for {
		for _, g := range c.nemesisGenerators {
			select {
			case <-ctx.Done():
				break LOOP
			default:
			}

			log.Printf("begin to run %s nemesis generator", g.Name())
			ops := g.Generate(c.nodes)

			wg.Add(n)
			for i := 0; i < n; i++ {
				go c.onNemesisLoop(ctx, i, ops[i], &wg)
			}
			wg.Wait()
		}
	}
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
