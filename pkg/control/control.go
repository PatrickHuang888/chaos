package control

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/siddontang/chaos/pkg/core"
	"github.com/siddontang/chaos/pkg/node"
)

// Controller controls the whole cluster. It sends request to the database,
// and also uses nemesis to disturb the cluster.
// Here have only 5 nodes, and the hosts are n1 - n5.
type Controller struct {
	cfg *Config

	nodes       []string
	nodeClients []*node.Client

	clients          []core.Client
	requestGenerator []core.RequestGenerator

	nemesisGenerators []core.NemesisGenerator

	ctx    context.Context
	cancel context.CancelFunc
}

// NewController creates a controller.
func NewController(cfg *Config, clientCreator core.ClientCreator, nemesisGenerators []core.NemesisGenerator) *Controller {
	if err := cfg.adjust(); err != nil {
		log.Fatalf("invalid config %v", err)
	}

	c := new(Controller)
	c.cfg = cfg
	c.ctx, c.cancel = context.WithCancel(context.Background())

	c.nemesisGenerators = nemesisGenerators

	for i := 1; i <= 5; i++ {
		name := fmt.Sprintf("n%d", i)
		c.nodes = append(c.nodes, name)
		client := node.NewClient(name, fmt.Sprintf("%s:%d", name, cfg.NodePort))
		c.nodeClients = append(c.nodeClients, client)
		c.clients = append(c.clients, clientCreator.Create(name))
		c.requestGenerator = append(c.requestGenerator, clientCreator.CreateRequestGenerator())
	}

	return c
}

// Close closes the controller.
func (c *Controller) Close() {
	c.cancel()
}

// Run runs the controller.
func (c *Controller) Run() {
	if len(c.requestGenerator) == 0 {
		log.Fatal("must add a request generator")
	}

	c.setUpDB()
	c.setUpClient()

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

	c.tearDownClient()
	c.tearDownDB()
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

func (c *Controller) setUpDB() {
	log.Printf("begin to set up database")
	c.syncExec(func(i int) {
		client := c.nodeClients[i]
		log.Printf("begin to set up database on %s", c.nodes[i])
		if err := client.SetUpDB(c.cfg.DB); err != nil {
			log.Fatalf("setup db %s at node %s failed %v", c.cfg.DB, c.nodes[i], err)
		}
	})
}

func (c *Controller) tearDownDB() {
	log.Printf("begin to tear down database")
	c.syncExec(func(i int) {
		client := c.nodeClients[i]
		log.Printf("being to tear down database on %s", c.nodes[i])
		if err := client.TearDownDB(c.cfg.DB); err != nil {
			log.Printf("tear down db %s at node %s failed %v", c.cfg.DB, c.nodes[i], err)
		}
	})
}

func (c *Controller) setUpClient() {
	log.Printf("begin to set up client")
	c.syncExec(func(i int) {
		client := c.clients[i]
		node := c.nodes[i]
		log.Printf("begin to set up db client for node %s", node)
		if err := client.SetUp(c.ctx, node); err != nil {
			log.Fatalf("set up cdb lient for node %s failed %v", node, err)
		}
	})
}

func (c *Controller) tearDownClient() {
	log.Printf("begin to set up client")
	c.syncExec(func(i int) {
		client := c.clients[i]
		node := c.nodes[i]
		log.Printf("begin to tear down db client for node %s", node)
		if err := client.TearDown(c.ctx, node); err != nil {
			log.Printf("tear down db client for node %s failed %v", node, err)
		}
	})
}

func (c *Controller) onClientLoop(i int) {
	client := c.clients[i]
	gen := c.requestGenerator[i]
	node := c.nodes[i]

	ctx, cancel := context.WithTimeout(c.ctx, c.cfg.RunTime)
	defer cancel()

	for i := 0; i < c.cfg.RequestCount; i++ {
		request := gen.Generate()

		// TODO: add to history
		_, err := client.Invoke(ctx, node, request)
		if err != nil {
			log.Printf("invoke db client for node %s failed %v", node, err)
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

func (c *Controller) onNemesisLoop(ctx context.Context, index int, op core.NemesisOperation, wg *sync.WaitGroup) {
	defer wg.Done()

	nodeClient := c.nodeClients[index]
	node := c.nodes[index]

	log.Printf("start nemesis %s with %v on %s", op.Name, op.StartArgs, node)
	if err := nodeClient.StartNemesis(op.Name, op.StartArgs...); err != nil {
		log.Printf("start nemesis %s with %v on %s failed: %v", op.Name, op.StartArgs, node, err)
	}

	select {
	case <-time.After(op.WaitTime):
	case <-ctx.Done():
	}

	log.Printf("stop nemesis %s with %v on %s", op.Name, op.StartArgs, node)
	if err := nodeClient.StopNemesis(op.Name, op.StopArgs...); err != nil {
		log.Printf("stop nemesis %s with %v on %s failed: %v", op.Name, op.StopArgs, node, err)
	}
}
