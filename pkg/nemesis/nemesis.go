package nemesis

import (
	"context"

	"github.com/siddontang/chaos/pkg/core"
	"github.com/siddontang/chaos/pkg/util/net"
)

type kill struct{}

func (kill) Invoke(ctx context.Context, args ...string) error {
	db := core.GetDB(args[0])
	service := args[1]
	return db.Kill(ctx, service)
}

func (kill) Recover(ctx context.Context, args ...string) error {
	db := core.GetDB(args[0])
	service := args[1]
	return db.Start(ctx, service)
}

func (kill) Name() string {
	return "kill"
}

type drop struct {
	t net.IPTables
}

func (n drop) Invoke(ctx context.Context, args ...string) error {
	for _, dropNode := range args {
		//TODO:
		/*if node == dropNode {
			// Don't drop itself
			continue
		}*/

		if err := n.t.Drop(ctx, dropNode); err != nil {
			return err
		}
	}
	return nil
}

func (n drop) Recover(ctx context.Context, args ...string) error {
	return n.t.Heal(ctx)
}

func (drop) Name() string {
	return "drop"
}

func init() {
	core.RegisterNemesis(kill{})
	core.RegisterNemesis(drop{})
}
