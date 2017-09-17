package node

import "testing"
import "net"
import "time"

func getNodeAddr(t *testing.T) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed %v", err)
	}
	addr := l.Addr().String()
	l.Close()

	return addr
}
func TestNodeHandler(t *testing.T) {
	addr := getNodeAddr(t)
	node := NewNode(addr)
	defer node.Close()

	client := NewClient("n0", addr)

	go func() {
		node.Run()
	}()

	time.Sleep(time.Second)

	nodes := []string{"n0"}
	if err := client.SetUpDB("noop", nodes); err != nil {
		t.Fatalf("setup db failed %v", err)
	}

	if err := client.StartDB("noop"); err != nil {
		t.Fatalf("start db failed %v", err)
	}

	if !client.IsDBRunning("noop") {
		t.Fatalf("db must be running")
	}

	if err := client.StopDB("noop"); err != nil {
		t.Fatalf("stop db failed %v", err)
	}

	if err := client.KillDB("noop"); err != nil {
		t.Fatalf("kill db failed %v", err)
	}

	if err := client.TearDownDB("noop", nodes); err != nil {
		t.Fatalf("tear down db failed %v", err)
	}

	if err := client.RunNemesis("noop", 0); err != nil {
		t.Fatalf("start nemesis failed %v", err)
	}
}
